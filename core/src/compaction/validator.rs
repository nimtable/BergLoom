// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Contains file writer API, and provides methods to write row groups and columns by
//! using row group writers and column writers respectively.

use std::collections::HashSet;
use std::sync::Arc;

use futures::StreamExt;
use iceberg::spec::{DataFile, Schema};
use iceberg::table::Table;

use crate::error::Result;
use crate::executor::InputFileScanTasks;
use crate::executor::datafusion::datafusion_processor::{
    DataFusionTaskContext, DatafusionProcessor,
};
use crate::{CompactionConfig, CompactionError};

pub struct CompactionValidator {
    datafusion_processor: DatafusionProcessor,
    input_datafusion_task_ctx: Option<DataFusionTaskContext>,
    output_datafusion_task_ctx: Option<DataFusionTaskContext>,
    table_ident: String,
    catalog_name: String,
}

impl CompactionValidator {
    pub async fn new(
        input_file_scan_tasks: InputFileScanTasks,
        output_files: Vec<DataFile>,
        config: Arc<CompactionConfig>,
        input_schema: Arc<Schema>,
        output_schema: Arc<Schema>,
        table: Table,
        catalog_name: String,
    ) -> Result<Self> {
        // TODO: Support different Schema for input and output
        if input_schema.schema_id() != output_schema.schema_id() {
            return Err(CompactionError::Config(
                "Input and output schemas must be the same for validation".to_owned(),
            ));
        }

        let snapshot_id = table
            .metadata()
            .current_snapshot_id()
            .ok_or_else(|| CompactionError::Execution("Snapshot id is not set".to_owned()))?;

        let scan = table.scan().from_snapshot_id(snapshot_id).build()?;
        let output_file_paths = output_files
            .iter()
            .map(|f| f.file_path())
            .collect::<HashSet<_>>();
        let mut output_file_scan_tasks_full_table = scan.plan_files().await?;
        let mut output_file_scan_tasks = vec![];
        while let Some(file) = output_file_scan_tasks_full_table.as_mut().next().await {
            let file = file?;
            if output_file_paths.contains(file.data_file_path()) {
                output_file_scan_tasks.push(file);
            }
        }

        let input_datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(input_schema)
            .with_input_data_files(input_file_scan_tasks)
            .build_merge_on_read()?;
        let output_datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(output_schema)
            .with_data_files(output_file_scan_tasks)
            .build_merge_on_read()?;

        let validator_config = Arc::new(
            CompactionConfig::builder()
                .batch_parallelism(config.batch_parallelism / 2)
                .target_partitions(1)
                .build(),
        );

        let datafusion_processor =
            DatafusionProcessor::new(validator_config, table.file_io().clone());

        Ok(Self {
            datafusion_processor,
            input_datafusion_task_ctx: Some(input_datafusion_task_ctx),
            output_datafusion_task_ctx: Some(output_datafusion_task_ctx),
            table_ident: table.identifier().to_string(),
            catalog_name,
        })
    }

    pub async fn validate(&mut self) -> Result<()> {
        let input_datafusion_task_ctx = self.input_datafusion_task_ctx.take().ok_or_else(|| {
            CompactionError::Unexpected("Input datafusion task context is not set".to_owned())
        })?;
        let output_datafusion_task_ctx =
            self.output_datafusion_task_ctx.take().ok_or_else(|| {
                CompactionError::Unexpected("Output datafusion task context is not set".to_owned())
            })?;
        let (mut input_batches, _) = self
            .datafusion_processor
            .execute(input_datafusion_task_ctx)
            .await?;
        let (mut output_batches, _) = self
            .datafusion_processor
            .execute(output_datafusion_task_ctx)
            .await?;

        // The target partitions is 1, so we expect only one batch for both input and output.
        if input_batches.len() != output_batches.len() || input_batches.len() != 1 {
            return Err(CompactionError::CompactionValidator(format!(
                "Input and output batches length mismatch: {} != {} != 1 catalog {} table_ident {}",
                input_batches.len(),
                output_batches.len(),
                self.catalog_name,
                self.table_ident
            )));
        }

        let mut input_batch = input_batches.pop().unwrap();
        let mut output_batch = output_batches.pop().unwrap();

        let mut input_stream = input_batch.as_mut();
        let mut output_stream = output_batch.as_mut();

        loop {
            match (input_stream.next().await, output_stream.next().await) {
                (Some(input_batch), Some(output_batch)) => {
                    let input_batch = input_batch?;
                    let output_batch = output_batch?;
                    if input_batch != output_batch {
                        return Err(CompactionError::CompactionValidator(format!(
                            "Input and output batches mismatch: {:?} != {:?} catalog {} table_ident {}",
                            input_batch, output_batch, self.catalog_name, self.table_ident,
                        )));
                    }
                }
                (None, None) => {
                    break;
                }
                _ => {
                    return Err(CompactionError::CompactionValidator(format!(
                        "Input and output batches length mismatch catalog {} table_ident {}",
                        self.catalog_name, self.table_ident
                    )));
                }
            }
        }

        Ok(())
    }
}
