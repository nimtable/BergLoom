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

use bergloom_codegen::compactor::RewriteFilesStat;
use iceberg::spec::DataFile;
use iceberg::{Catalog, ErrorKind, TableIdent};
use mixtrics::metrics::BoxedRegistry;
use mixtrics::registry::noop::NoopMetricsRegistry;

use crate::CompactionError;
use crate::Result;
use crate::common::Metrics;
use crate::compaction::validator::CompactionValidator;
use crate::executor::{
    ExecutorType, InputFileScanTasks, RewriteFilesRequest, RewriteFilesResponse,
    create_compaction_executor,
};
use crate::{CompactionConfig, CompactionExecutor};
use futures_async_stream::for_await;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg::transaction::Transaction;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use backon::ExponentialBuilder;
use backon::Retryable;

mod validator;

pub enum CompactionType {
    Full,
}

/// Builder for creating Compaction instances with flexible configuration
pub struct CompactionBuilder {
    config: Option<Arc<CompactionConfig>>,
    executor_type: ExecutorType,
    catalog: Option<Arc<dyn Catalog>>,
    registry: BoxedRegistry,
    table_ident: Option<TableIdent>,
    compaction_type: Option<CompactionType>,
    catalog_name: Option<String>,
    commit_retry_config: RewriteDataFilesCommitManagerRetryConfig,
}

impl CompactionBuilder {
    /// Create a new CompactionBuilder with default settings
    pub fn new() -> Self {
        Self {
            config: None,
            executor_type: ExecutorType::DataFusion, // Default executor type
            catalog: None,
            registry: Box::new(NoopMetricsRegistry),
            table_ident: None,
            compaction_type: None,
            catalog_name: None,
            commit_retry_config: RewriteDataFilesCommitManagerRetryConfig::default(),
        }
    }

    /// Set the compaction configuration
    pub fn with_config(mut self, config: Arc<CompactionConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Set the executor type (defaults to DataFusion)
    pub fn with_executor_type(mut self, executor_type: ExecutorType) -> Self {
        self.executor_type = executor_type;
        self
    }

    /// Set the catalog
    pub fn with_catalog(mut self, catalog: Arc<dyn Catalog>) -> Self {
        self.catalog = Some(catalog);
        self
    }

    /// Set the metrics registry (optional, defaults to NoopMetricsRegistry)
    pub fn with_registry(mut self, registry: BoxedRegistry) -> Self {
        self.registry = registry;
        self
    }

    pub fn with_table_ident(mut self, table_ident: TableIdent) -> Self {
        self.table_ident = Some(table_ident);
        self
    }

    pub fn with_compaction_type(mut self, compaction_type: CompactionType) -> Self {
        self.compaction_type = Some(compaction_type);
        self
    }

    pub fn with_catalog_name(mut self, catalog_name: String) -> Self {
        self.catalog_name = Some(catalog_name);
        self
    }

    pub fn with_retry_config(
        mut self,
        retry_config: RewriteDataFilesCommitManagerRetryConfig,
    ) -> Self {
        self.commit_retry_config = retry_config;
        self
    }

    /// Build the Compaction instance
    pub async fn build(self) -> Result<Compaction> {
        let config = self.config.ok_or_else(|| {
            crate::error::CompactionError::Execution("CompactionConfig is required".to_string())
        })?;

        let catalog = self.catalog.ok_or_else(|| {
            crate::error::CompactionError::Execution("Catalog is required".to_string())
        })?;

        let table_ident = self.table_ident.ok_or_else(|| {
            crate::error::CompactionError::Execution("TableIdent is required".to_string())
        })?;

        let compaction_type = self.compaction_type.unwrap_or(CompactionType::Full);

        if !catalog.table_exists(&table_ident).await? {
            return Err(crate::error::CompactionError::Execution(
                "Table does not exist".to_string(),
            ));
        }

        let executor = create_compaction_executor(self.executor_type);

        let metrics = Arc::new(Metrics::new(self.registry));

        let catalog_name = self.catalog_name.unwrap_or_default();

        let commit_retry_config = self.commit_retry_config;

        Ok(Compaction {
            config,
            executor,
            catalog,
            metrics,
            table_ident,
            compaction_type,
            catalog_name,
            commit_retry_config,
        })
    }
}

impl Default for CompactionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A Proxy for the compaction process, which handles the configuration, executor, and catalog.
pub struct Compaction {
    pub config: Arc<CompactionConfig>,
    pub executor: Box<dyn CompactionExecutor>,
    pub catalog: Arc<dyn Catalog>,
    pub metrics: Arc<Metrics>,
    pub table_ident: TableIdent,
    pub compaction_type: CompactionType,
    pub catalog_name: String,

    pub commit_retry_config: RewriteDataFilesCommitManagerRetryConfig,
}

struct CompacitonResult {
    stats: RewriteFilesStat,

    compaction_validator: Option<CompactionValidator>,
}

impl Compaction {
    /// Create a new CompactionBuilder for flexible configuration
    pub fn builder() -> CompactionBuilder {
        CompactionBuilder::new()
    }

    pub async fn compact(&self) -> Result<RewriteFilesStat> {
        let CompacitonResult {
            stats,
            compaction_validator,
        } = match self.compaction_type {
            CompactionType::Full => self.full_compact().await?,
        };

        // validate
        if let Some(mut compaction_validator) = compaction_validator {
            compaction_validator.validate().await?;

            // Todo: log the successful validation with more context
            tracing::info!(
                "Compaction validation completed successfully for table '{}'",
                self.table_ident
            );
        }

        Ok(stats)
    }

    async fn full_compact(&self) -> Result<CompacitonResult> {
        let table_label: std::borrow::Cow<'static, str> = self.table_ident.to_string().into();
        let catalog_name_label: std::borrow::Cow<'static, str> = self.catalog_name.clone().into();
        let label_vec: [std::borrow::Cow<'static, str>; 2] = [catalog_name_label, table_label];

        let now = std::time::Instant::now();

        let table = self.catalog.load_table(&self.table_ident).await?;
        let (data_files, delete_files) = get_old_files_from_table(table.clone()).await?;
        let mut input_file_scan_tasks = Some(get_tasks_from_table(table.clone()).await?);

        let file_io = table.file_io().clone();
        let schema = table.metadata().current_schema();
        let basic_schema_id = schema.schema_id();
        // TODO: support check partition spec
        let default_location_generator =
            DefaultLocationGenerator::new(table.metadata().clone()).unwrap();
        let rewrite_files_request = RewriteFilesRequest {
            file_io: file_io.clone(),
            schema: schema.clone(),
            input_file_scan_tasks: if self.config.enable_validate_compaction {
                input_file_scan_tasks.clone().unwrap()
            } else {
                input_file_scan_tasks.take().unwrap()
            },
            config: self.config.clone(),
            dir_path: default_location_generator.dir_path,
            partition_spec: table.metadata().default_partition_spec().clone(),
        };
        let RewriteFilesResponse {
            data_files: mut output_data_files,
            stat,
        } = match self.executor.rewrite_files(rewrite_files_request).await {
            Ok(response) => response,
            Err(e) => {
                self.metrics
                    .compaction_executor_error_counter
                    .counter(&label_vec)
                    .increase(1);
                return Err(e);
            }
        };

        let consistency_params = CommitConsistencyParams {
            starting_snapshot_id: table.metadata().current_snapshot_id().unwrap(),
            use_starting_sequence_number: true,
            basic_schema_id,
        };

        let commit_manager = RewriteDataFilesCommitManager::new(
            self.commit_retry_config.clone(),
            self.catalog.clone(),
            self.table_ident.clone(),
            self.catalog_name.clone(),
            self.metrics.clone(),
            consistency_params,
        );

        let commit_now = std::time::Instant::now();
        let output_data_files = if self.config.enable_validate_compaction {
            output_data_files.clone()
        } else {
            std::mem::take(&mut output_data_files)
        };
        let committed_table = commit_manager
            .rewrite_files(
                output_data_files.clone(),
                data_files.into_iter().chain(delete_files.into_iter()),
            )
            .await?;

        self.metrics
            .compaction_commit_duration
            .histogram(&label_vec)
            .record(commit_now.elapsed().as_secs_f64());

        self.metrics
            .compaction_duration
            .histogram(&label_vec)
            .record(now.elapsed().as_secs_f64());

        self.metrics
            .compaction_rewritten_bytes
            .counter(&label_vec)
            .increase(stat.rewritten_bytes);

        self.metrics
            .compaction_rewritten_files_count
            .counter(&label_vec)
            .increase(stat.rewritten_files_count as u64);

        self.metrics
            .compaction_added_files_count
            .counter(&label_vec)
            .increase(stat.added_files_count as u64);

        self.metrics
            .compaction_failed_data_files_count
            .counter(&label_vec)
            .increase(stat.failed_data_files_count as u64);

        let compaction_validator = if self.config.enable_validate_compaction {
            Some(
                CompactionValidator::new(
                    input_file_scan_tasks.unwrap(),
                    output_data_files,
                    self.config.clone(),
                    schema.clone(),
                    table.metadata().current_schema().clone(),
                    committed_table,
                    self.catalog_name.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        Ok(CompacitonResult {
            stats: RewriteFilesStat {
                rewritten_files_count: stat.rewritten_files_count,
                added_files_count: stat.added_files_count,
                rewritten_bytes: stat.rewritten_bytes,
                failed_data_files_count: stat.failed_data_files_count,
            },
            compaction_validator,
        })
    }

    pub async fn expire_snapshot(&self, table_ident: TableIdent) -> Result<()> {
        let table = self.catalog.load_table(&table_ident).await?;
        let txn = Transaction::new(&table);
        let txn = txn.expire_snapshot().apply().await?;
        txn.commit(self.catalog.as_ref()).await?;
        Ok(())
    }
}

async fn get_old_files_from_table(table: Table) -> Result<(Vec<DataFile>, Vec<DataFile>)> {
    let manifest_list = table
        .metadata()
        .current_snapshot()
        .unwrap()
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .unwrap();

    let mut data_file = vec![];
    let mut delete_file = vec![];
    for manifest_file in manifest_list.entries() {
        let a = manifest_file.load_manifest(table.file_io()).await.unwrap();
        let (entry, _) = a.into_parts();
        for i in entry {
            match i.content_type() {
                iceberg::spec::DataContentType::Data => {
                    data_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::EqualityDeletes => {
                    delete_file.push(i.data_file().clone());
                }
                iceberg::spec::DataContentType::PositionDeletes => {
                    delete_file.push(i.data_file().clone());
                }
            }
        }
    }
    Ok((data_file, delete_file))
}

async fn get_tasks_from_table(table: Table) -> Result<InputFileScanTasks> {
    let snapshot_id = table.metadata().current_snapshot_id().unwrap();

    let scan = table
        .scan()
        .snapshot_id(snapshot_id)
        .with_delete_file_processing_enabled(true)
        .build()?;
    let file_scan_stream = scan.plan_files().await?;

    let mut position_delete_files = HashMap::new();
    let mut data_files = vec![];
    let mut equality_delete_files = HashMap::new();

    #[for_await]
    for task in file_scan_stream {
        let task: FileScanTask = task?;
        match task.data_file_content {
            iceberg::spec::DataContentType::Data => {
                for delete_task in task.deletes.iter() {
                    match &delete_task.data_file_content {
                        iceberg::spec::DataContentType::PositionDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = vec![];
                            position_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        iceberg::spec::DataContentType::EqualityDeletes => {
                            let mut delete_task = delete_task.clone();
                            delete_task.project_field_ids = delete_task.equality_ids.clone();
                            equality_delete_files
                                .insert(delete_task.data_file_path.clone(), delete_task);
                        }
                        _ => {
                            unreachable!()
                        }
                    }
                }
                data_files.push(task);
            }
            _ => {
                unreachable!()
            }
        }
    }
    Ok(InputFileScanTasks {
        data_files,
        position_delete_files: position_delete_files.into_values().collect(),
        equality_delete_files: equality_delete_files.into_values().collect(),
    })
}

/// Configuration for the commit manager, including retry strategies.
#[derive(Debug, Clone)]
pub struct RewriteDataFilesCommitManagerRetryConfig {
    pub max_retries: u32, // This can be used to configure the backon strategy
    pub retry_initial_delay: Duration, // For exponential backoff
    pub retry_max_delay: Duration, // For exponential backoff
}

impl Default for RewriteDataFilesCommitManagerRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_initial_delay: Duration::from_secs(1),
            retry_max_delay: Duration::from_secs(10),
        }
    }
}

// Manages the commit process with retries
pub struct RewriteDataFilesCommitManager {
    config: RewriteDataFilesCommitManagerRetryConfig,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    starting_snapshot_id: i64, // The snapshot ID to start from, used for consistency
    use_starting_sequence_number: bool, // Whether to use the starting sequence number for commits

    catalog_name: String,  // Catalog name for metrics
    metrics: Arc<Metrics>, // Metrics for tracking commit operations

    basic_schema_id: i32, // Schema ID for the table, used for validation
}

pub struct CommitConsistencyParams {
    pub starting_snapshot_id: i64,
    pub use_starting_sequence_number: bool,
    pub basic_schema_id: i32,
}

/// Manages the commit process with retries
impl RewriteDataFilesCommitManager {
    pub fn new(
        config: RewriteDataFilesCommitManagerRetryConfig,
        catalog: Arc<dyn Catalog>,
        table_ident: TableIdent,
        catalog_name: String,
        metrics: Arc<Metrics>,
        consistency_params: CommitConsistencyParams,
    ) -> Self {
        Self {
            config,
            catalog,
            table_ident,
            starting_snapshot_id: consistency_params.starting_snapshot_id,
            use_starting_sequence_number: consistency_params.use_starting_sequence_number,
            catalog_name,
            metrics,
            basic_schema_id: consistency_params.basic_schema_id,
        }
    }

    /// Rewrites files in the table, handling retries and errors.
    pub async fn rewrite_files(
        &self,
        data_files: impl IntoIterator<Item = DataFile>,
        delete_files: impl IntoIterator<Item = DataFile>,
    ) -> Result<Table> {
        let data_files: Vec<DataFile> = data_files.into_iter().collect();
        let delete_files: Vec<DataFile> = delete_files.into_iter().collect();
        let operation = || {
            let catalog = self.catalog.clone();
            let table_ident = self.table_ident.clone();
            let data_files = data_files.clone();
            let delete_files = delete_files.clone();
            let use_starting_sequence_number = self.use_starting_sequence_number;
            let starting_snapshot_id = self.starting_snapshot_id;
            let metrics = self.metrics.clone();

            let table_label: std::borrow::Cow<'static, str> = self.table_ident.to_string().into();
            let catalog_name_label: std::borrow::Cow<'static, str> =
                self.catalog_name.clone().into();
            let label_vec: [std::borrow::Cow<'static, str>; 2] = [catalog_name_label, table_label];

            async move {
                // reload the table to get the latest state
                let table = catalog.load_table(&table_ident).await?;

                let schema_id = table.metadata().current_schema().schema_id();
                if schema_id != self.basic_schema_id {
                    return Err(iceberg::Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Schema ID mismatch: expected {}, found {}",
                            self.basic_schema_id, schema_id
                        ),
                    ));
                }

                let txn = Transaction::new(&table);

                // TODO: support validation of data files and delete files with starting snapshot before applying the rewrite
                let rewrite_action = if use_starting_sequence_number {
                    // TODO: avoid retry if the snapshot_id is not found
                    if let Some(snapshot) = table.metadata().snapshot_by_id(starting_snapshot_id) {
                        txn.rewrite_files(None, vec![])?
                            .add_data_files(data_files)?
                            .delete_files(delete_files)?
                            .new_data_file_sequence_number(snapshot.sequence_number())?
                    } else {
                        return Err(iceberg::Error::new(
                            ErrorKind::Unexpected,
                            format!(
                                "No snapshot found with the given snapshot_id {}",
                                starting_snapshot_id
                            ),
                        ));
                    }
                } else {
                    txn.rewrite_files(None, vec![])?
                        .add_data_files(data_files)?
                        .delete_files(delete_files)?
                };

                let txn = rewrite_action.apply().await?;
                match txn.commit(catalog.as_ref()).await {
                    Ok(table) => {
                        // Update metrics after a successful commit
                        metrics
                            .compaction_commit_counter
                            .counter(&label_vec)
                            .increase(1);
                        Ok(table)
                    }
                    Err(commit_err) => {
                        metrics
                            .compaction_commit_failed_counter
                            .counter(&label_vec)
                            .increase(1);

                        tracing::error!(
                            "Commit attempt failed for table '{}': {:?}. Will retry if applicable.",
                            table_ident,
                            commit_err
                        );
                        Err(commit_err)
                    }
                }
            }
        };

        let retry_strategy = ExponentialBuilder::default()
            .with_min_delay(self.config.retry_initial_delay)
            .with_max_delay(self.config.retry_max_delay)
            .with_max_times(self.config.max_retries as usize);

        operation
            .retry(retry_strategy)
            .when(|e| {
                matches!(e.kind(), iceberg::ErrorKind::DataInvalid)
                    || matches!(e.kind(), iceberg::ErrorKind::Unexpected)
            })
            .notify(|e, d| {
                // Notify the user about the error
                // TODO: add metrics
                tracing::info!("Retrying Compaction failed {:?} after {:?}", e, d);
            })
            .await
            .map_err(|e: iceberg::Error| CompactionError::from(e)) // Convert backon::Error to your CompactionError
    }
}
