/*
 * Copyright 2025 BergLoom
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::{error::Result, executor::iceberg_writer::rolling_iceberg_writer};
use ::datafusion::parquet::file::properties::WriterProperties;
use async_trait::async_trait;
use datafusion_processor::{DataFusionTaskContext, DatafusionProcessor};
use futures::{future::try_join_all, StreamExt};
use iceberg::{
    io::FileIO,
    spec::{DataFile, PartitionSpec, Schema},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            ParquetWriterBuilder,
        },
        function_writer::fanout_partition_writer::FanoutPartitionWriterBuilder,
        IcebergWriter, IcebergWriterBuilder,
    },
};
use sqlx::types::Uuid;
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::CompactionError;

use super::{CompactionExecutor, RewriteFilesStat};
pub mod datafusion_processor;
use super::{RewriteFilesRequest, RewriteFilesResponse};
pub mod file_scan_task_table_provider;
pub mod iceberg_file_task_scan;

#[derive(Default)]
pub struct DataFusionExecutor {}

#[async_trait]
impl CompactionExecutor for DataFusionExecutor {
    async fn rewrite_files(&self, request: RewriteFilesRequest) -> Result<RewriteFilesResponse> {
        let RewriteFilesRequest {
            file_io,
            schema,
            input_file_scan_tasks,
            config,
            dir_path,
            partition_spec,
        } = request;

        let mut stat = RewriteFilesStat::default();
        let rewritten_files_count = input_file_scan_tasks.input_files_count();

        let datafusion_task_ctx = DataFusionTaskContext::builder()?
            .with_schema(schema)
            .with_input_data_files(input_file_scan_tasks)
            .build()?;
        let (batches, input_schema) = DatafusionProcessor::new(config.clone(), file_io.clone())
            .execute(datafusion_task_ctx)
            .await?;
        let arc_input_schema = Arc::new(input_schema);
        let mut futures = Vec::with_capacity(config.batch_parallelism);
        // build iceberg writer for each partition
        for mut batch in batches {
            let dir_path = dir_path.clone();
            let schema = arc_input_schema.clone();
            let config = config.clone();
            let file_io = file_io.clone();
            let partition_spec = partition_spec.clone();
            let future: JoinHandle<
                std::result::Result<Vec<iceberg::spec::DataFile>, CompactionError>,
            > = tokio::spawn(async move {
                let mut data_file_writer = Self::build_iceberg_writer(
                    config.data_file_prefix.clone(),
                    dir_path,
                    schema,
                    file_io,
                    partition_spec,
                    config.target_file_size,
                    config.write_parquet_properties.clone(),
                )
                .await?;
                while let Some(b) = batch.as_mut().next().await {
                    data_file_writer.write(b?).await?;
                }
                let data_files = data_file_writer.close().await?;
                Ok(data_files)
            });
            futures.push(future);
        }
        // collect all data files from all partitions
        let output_data_files: Vec<DataFile> = try_join_all(futures)
            .await
            .map_err(|e| CompactionError::Execution(e.to_string()))?
            .into_iter()
            .map(|res| res.map(|v| v.into_iter()))
            .collect::<Result<Vec<_>>>()
            .map(|iters| iters.into_iter().flatten().collect())?;

        stat.added_files_count = output_data_files.len() as u32;
        stat.rewritten_bytes = output_data_files
            .iter()
            .map(|f| f.file_size_in_bytes())
            .sum();
        stat.rewritten_files_count = rewritten_files_count;

        Ok(RewriteFilesResponse {
            data_files: output_data_files,
            stat,
        })
    }
}

impl DataFusionExecutor {
    async fn build_iceberg_writer(
        data_file_prefix: String,
        dir_path: String,
        schema: Arc<Schema>,
        file_io: FileIO,
        partition_spec: Arc<PartitionSpec>,
        target_file_size: u64,
        write_parquet_properties: WriterProperties,
    ) -> Result<Box<dyn IcebergWriter>> {
        let location_generator = DefaultLocationGenerator { dir_path };
        let unique_uuid_suffix = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(
            data_file_prefix,
            Some(unique_uuid_suffix.to_string()),
            iceberg::spec::DataFileFormat::Parquet,
        );

        let parquet_writer_builder = ParquetWriterBuilder::new(
            write_parquet_properties,
            schema.clone(),
            file_io,
            location_generator,
            file_name_generator,
        );

        let data_file_builder =
            DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec.spec_id());
        let data_file_size_writer = rolling_iceberg_writer::RollingIcebergWriterBuilder::new(
            data_file_builder,
            target_file_size,
        );
        let iceberg_output_writer = if partition_spec.fields().is_empty() {
            Box::new(data_file_size_writer.build().await?) as Box<dyn IcebergWriter>
        } else {
            Box::new(
                FanoutPartitionWriterBuilder::new(
                    data_file_size_writer,
                    partition_spec.clone(),
                    schema,
                )?
                .build()
                .await?,
            ) as Box<dyn IcebergWriter>
        };
        Ok(iceberg_output_writer)
    }
}
