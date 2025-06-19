/*
 * Copyright 2025 IC
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

use std::sync::Arc;

use crate::{
    CompactionConfig,
    error::{CompactionError, Result},
    executor::InputFileScanTasks,
};
use datafusion::{
    execution::SendableRecordBatchStream,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, Partitioning, execute_stream_partitioned,
        repartition::RepartitionExec,
    },
    prelude::{SessionConfig, SessionContext},
};
use iceberg::{
    arrow::schema_to_arrow_schema,
    io::FileIO,
    scan::FileScanTask,
    spec::{NestedField, PrimitiveType, Schema, Type},
};

use super::file_scan_task_table_provider::IcebergFileScanTaskTableProvider;

// System hidden columns used for Iceberg merge-on-read operations
pub const SYS_HIDDEN_SEQ_NUM: &str = "sys_hidden_seq_num";
pub const SYS_HIDDEN_FILE_PATH: &str = "sys_hidden_file_path";
pub const SYS_HIDDEN_POS: &str = "sys_hidden_pos";
const SYS_HIDDEN_COLS: [&str; 3] = [SYS_HIDDEN_SEQ_NUM, SYS_HIDDEN_FILE_PATH, SYS_HIDDEN_POS];

/// DataFusion processor for Iceberg compaction with merge-on-read optimization
pub struct DatafusionProcessor {
    table_register: DatafusionTableRegister,
    ctx: Arc<SessionContext>,
    config: Arc<CompactionConfig>,
}

impl DatafusionProcessor {
    pub fn new(config: Arc<CompactionConfig>, file_io: FileIO) -> Self {
        let session_config = SessionConfig::new()
            .with_target_partitions(config.target_partitions)
            .with_batch_size(config.max_record_batch_rows);
        let ctx = Arc::new(SessionContext::new_with_config(session_config));
        let table_register = DatafusionTableRegister::new(
            file_io,
            ctx.clone(),
            config.batch_parallelism,
            config.max_record_batch_rows,
        );
        Self {
            table_register,
            ctx,
            config,
        }
    }

    /// Registers all necessary tables (data files, position deletes, equality deletes) with DataFusion
    pub fn register_tables(&self, mut datafusion_task_ctx: DataFusionTaskContext) -> Result<()> {
        // Register data file table if present
        if let Some(datafile_schema) = datafusion_task_ctx.data_file_schema.take() {
            self.table_register.register_data_table_provider(
                &datafile_schema,
                datafusion_task_ctx.data_files.take().ok_or_else(|| {
                    CompactionError::Unexpected("Data files are not set".to_owned())
                })?,
                &datafusion_task_ctx.data_file_table_name(),
                datafusion_task_ctx.need_seq_num(),
                datafusion_task_ctx.need_file_path_and_pos(),
            )?;
        }

        // Register position delete table if present
        if let Some(position_delete_schema) = datafusion_task_ctx.position_delete_schema.take() {
            self.table_register.register_delete_table_provider(
                &position_delete_schema,
                datafusion_task_ctx
                    .position_delete_files
                    .take()
                    .ok_or_else(|| {
                        CompactionError::Unexpected("Position delete files are not set".to_owned())
                    })?,
                &datafusion_task_ctx.position_delete_table_name(),
            )?;
        }

        // Register equality delete tables if present
        if let Some(equality_delete_metadatas) =
            datafusion_task_ctx.equality_delete_metadatas.take()
        {
            for EqualityDeleteMetadata {
                equality_delete_schema,
                equality_delete_table_name,
                file_scan_tasks,
            } in equality_delete_metadatas
            {
                self.table_register.register_delete_table_provider(
                    &equality_delete_schema,
                    file_scan_tasks,
                    &equality_delete_table_name,
                )?;
            }
        }
        Ok(())
    }

    /// Executes the compaction query using DataFusion
    ///
    /// This method:
    /// 1. Registers all necessary tables with DataFusion
    /// 2. Creates and executes the merge-on-read SQL query
    /// 3. Applies repartitioning if needed for optimal parallelism
    /// 4. Returns streaming result batches and the input schema
    pub async fn execute(
        &self,
        mut datafusion_task_ctx: DataFusionTaskContext,
    ) -> Result<(Vec<SendableRecordBatchStream>, Schema)> {
        let input_schema = datafusion_task_ctx
            .input_schema
            .take()
            .ok_or_else(|| CompactionError::Unexpected("Input schema is not set".to_owned()))?;
        let exec_sql = datafusion_task_ctx.exec_sql.clone();

        self.register_tables(datafusion_task_ctx)?;

        let df = self.ctx.sql(&exec_sql).await?;
        let physical_plan = df.create_physical_plan().await?;

        // Conditionally create a new physical_plan if repartitioning is needed
        let plan_to_execute: Arc<dyn ExecutionPlan + 'static> =
            if physical_plan.output_partitioning().partition_count()
                != self.config.target_partitions
            {
                Arc::new(RepartitionExec::try_new(
                    physical_plan,
                    Partitioning::RoundRobinBatch(self.config.target_partitions),
                )?)
            } else {
                physical_plan
            };

        let batches = execute_stream_partitioned(plan_to_execute, self.ctx.task_ctx())?;

        Ok((batches, input_schema))
    }
}

pub struct DatafusionTableRegister {
    file_io: FileIO,
    ctx: Arc<SessionContext>,

    batch_parallelism: usize,
    max_record_batch_rows: usize,
}

impl DatafusionTableRegister {
    pub fn new(
        file_io: FileIO,
        ctx: Arc<SessionContext>,
        batch_parallelism: usize,
        max_record_batch_rows: usize,
    ) -> Self {
        DatafusionTableRegister {
            file_io,
            ctx,
            batch_parallelism,
            max_record_batch_rows,
        }
    }

    pub fn register_data_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Result<()> {
        self.register_table_provider_impl(
            schema,
            file_scan_tasks,
            table_name,
            need_seq_num,
            need_file_path_and_pos,
        )
    }

    pub fn register_delete_table_provider(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
    ) -> Result<()> {
        self.register_table_provider_impl(schema, file_scan_tasks, table_name, false, false)
    }

    fn register_table_provider_impl(
        &self,
        schema: &Schema,
        file_scan_tasks: Vec<FileScanTask>,
        table_name: &str,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Result<()> {
        let schema = schema_to_arrow_schema(schema)?;
        let data_file_table_provider = IcebergFileScanTaskTableProvider::new(
            file_scan_tasks,
            Arc::new(schema),
            self.file_io.clone(),
            need_seq_num,
            need_file_path_and_pos,
            self.batch_parallelism,
            self.max_record_batch_rows,
        );

        self.ctx
            .register_table(table_name, Arc::new(data_file_table_provider))?;

        Ok(())
    }
}

/// SQL Builder for generating merge-on-read SQL queries
struct SqlBuilder<'a> {
    /// Column names to be projected in the query
    project_names: &'a Vec<String>,

    /// Position delete table name
    position_delete_table_name: Option<String>,

    /// Data file table name
    data_file_table_name: Option<String>,

    /// Flag indicating if file path and position columns are needed
    equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,

    /// Flag indicating if position delete files are needed
    need_file_path_and_pos: bool,
}

impl<'a> SqlBuilder<'a> {
    /// Creates a new SQL Builder with the specified parameters
    fn new(
        project_names: &'a Vec<String>,
        position_delete_table_name: Option<String>,
        data_file_table_name: Option<String>,
        equality_delete_metadatas: &'a Vec<EqualityDeleteMetadata>,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            project_names,
            position_delete_table_name,
            data_file_table_name,
            equality_delete_metadatas,
            need_file_path_and_pos,
        }
    }

    /// Builds a merge-on-read SQL query
    ///
    /// This method constructs a SQL query that:
    /// 1. Selects the specified columns from the data file table
    /// 2. Optionally joins with position delete files to exclude deleted rows
    /// 3. Optionally joins with equality delete files to exclude rows based on equality conditions
    pub fn build_merge_on_read_sql(self) -> Result<String> {
        let data_file_table_name = self.data_file_table_name.as_ref().ok_or_else(|| {
            CompactionError::Execution("Data file table name is not provided".to_string())
        })?;

        // Determine which hidden columns are needed for join conditions
        let need_seq_num = !self.equality_delete_metadatas.is_empty();
        let need_file_path_and_pos = self.need_file_path_and_pos;

        // Early return for simple case: no deletes at all
        if !need_seq_num && !need_file_path_and_pos {
            return Ok(format!(
                "SELECT {} FROM {}",
                self.project_names.join(", "),
                data_file_table_name
            ));
        }

        // Build the complete column list including hidden columns for internal queries
        let mut internal_columns = self.project_names.clone();
        if need_seq_num {
            internal_columns.push(SYS_HIDDEN_SEQ_NUM.to_string());
        }
        if need_file_path_and_pos {
            internal_columns.push(SYS_HIDDEN_FILE_PATH.to_string());
            internal_columns.push(SYS_HIDDEN_POS.to_string());
        }

        // Start with a SELECT query that includes all necessary columns
        let mut query = format!(
            "SELECT {} FROM {}",
            internal_columns.join(", "),
            data_file_table_name
        );

        // Add position delete join if needed
        // This excludes rows that have been deleted by position
        if self.need_file_path_and_pos {
            let position_delete_table_name =
                self.position_delete_table_name.as_ref().ok_or_else(|| {
                    CompactionError::Execution(
                        "Position delete table name is not provided".to_string(),
                    )
                })?;

            let pos_join_conditions = format!(
                "{}.{} = {}.{} AND {}.{} = {}.{}",
                data_file_table_name,
                SYS_HIDDEN_FILE_PATH,
                position_delete_table_name,
                SYS_HIDDEN_FILE_PATH,
                data_file_table_name,
                SYS_HIDDEN_POS,
                position_delete_table_name,
                SYS_HIDDEN_POS
            );

            query = format!(
                "SELECT {} FROM {} RIGHT ANTI JOIN ({}) AS {} ON {}",
                internal_columns.join(", "), // Include hidden columns in outer SELECT
                position_delete_table_name,
                query,
                data_file_table_name,
                pos_join_conditions
            );
        }

        // Add equality delete join if needed
        // This excludes rows that match the equality conditions in the delete files
        if !self.equality_delete_metadatas.is_empty() {
            for eq_meta in self.equality_delete_metadatas {
                let eq_table_name = &eq_meta.equality_delete_table_name;
                let eq_join_conditions = eq_meta
                    .equality_delete_join_names()
                    .iter()
                    .map(|col_name| {
                        format!(
                            "{}.{} = {}.{}",
                            eq_table_name, col_name, data_file_table_name, col_name
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(" AND ");

                // Only add sequence number condition if we have equality deletes
                // (which means the data file table should have the seq_num column)
                let seq_condition = format!(
                    "{}.{} < {}.{}",
                    data_file_table_name, SYS_HIDDEN_SEQ_NUM, eq_table_name, SYS_HIDDEN_SEQ_NUM
                );

                let full_condition = if eq_join_conditions.is_empty() {
                    seq_condition
                } else {
                    format!("{} AND {}", eq_join_conditions, seq_condition)
                };

                query = format!(
                    "SELECT {} FROM {} RIGHT ANTI JOIN ({}) AS {} ON {}",
                    internal_columns.join(", "), // Include hidden columns in outer SELECT
                    eq_table_name,
                    query,
                    data_file_table_name,
                    full_condition
                );
            }
        }

        // Final SELECT to return only the project columns (without hidden columns)
        if need_seq_num || need_file_path_and_pos {
            query = format!(
                "SELECT {} FROM ({}) AS final_result",
                self.project_names.join(", "),
                query
            );
        }

        Ok(query)
    }
}

pub struct DataFusionTaskContext {
    pub(crate) data_file_schema: Option<Schema>,
    pub(crate) input_schema: Option<Schema>,
    pub(crate) data_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_files: Option<Vec<FileScanTask>>,
    #[allow(unused)]
    pub(crate) equality_delete_files: Option<Vec<FileScanTask>>,
    pub(crate) position_delete_schema: Option<Schema>,
    pub(crate) equality_delete_metadatas: Option<Vec<EqualityDeleteMetadata>>,
    pub(crate) exec_sql: String,
    pub(crate) table_prefix: String,
}

pub struct DataFusionTaskContextBuilder {
    schema: Arc<Schema>,
    data_files: Vec<FileScanTask>,
    position_delete_files: Vec<FileScanTask>,
    equality_delete_files: Vec<FileScanTask>,
    table_prefix: String,
}

impl DataFusionTaskContextBuilder {
    pub fn with_schema(mut self, schema: Arc<Schema>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_table_prefix(mut self, table_prefix: String) -> Self {
        self.table_prefix = table_prefix;
        self
    }

    pub fn with_input_data_files(mut self, input_file_scan_tasks: InputFileScanTasks) -> Self {
        self.data_files = input_file_scan_tasks.data_files;
        self.position_delete_files = input_file_scan_tasks.position_delete_files;
        self.equality_delete_files = input_file_scan_tasks.equality_delete_files;
        self
    }

    pub fn with_data_files(mut self, data_files: Vec<FileScanTask>) -> Self {
        self.data_files = data_files;
        self
    }

    pub fn with_position_delete_files(mut self, position_delete_files: Vec<FileScanTask>) -> Self {
        self.position_delete_files = position_delete_files;
        self
    }

    pub fn with_equality_delete_files(mut self, equality_delete_files: Vec<FileScanTask>) -> Self {
        self.equality_delete_files = equality_delete_files;
        self
    }

    fn build_position_schema() -> Result<Schema> {
        let position_delete_schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    SYS_HIDDEN_FILE_PATH,
                    Type::Primitive(PrimitiveType::String),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    SYS_HIDDEN_POS,
                    Type::Primitive(PrimitiveType::Long),
                    true,
                )),
            ])
            .build()?;
        Ok(position_delete_schema)
    }

    // build data fusion task context
    pub fn build(self) -> Result<DataFusionTaskContext> {
        let mut highest_field_id = self.schema.highest_field_id();
        // Build schema for position delete file, file_path + pos
        let position_delete_schema = Self::build_position_schema()?;
        // Build schema for equality delete file, equality_ids + seq_num
        let mut equality_ids: Option<Vec<i32>> = None;
        let mut equality_delete_metadatas = Vec::new();
        for (table_idx, task) in self.equality_delete_files.iter().enumerate() {
            if equality_ids
                .as_ref()
                .is_none_or(|ids| !ids.eq(&task.equality_ids))
            {
                // If ids are different or not assigned, create a new metadata
                let equality_delete_schema =
                    self.build_equality_delete_schema(&task.equality_ids, &mut highest_field_id)?;
                let equality_delete_table_name =
                    table_name::build_equality_delete_table_name(&self.table_prefix, table_idx);
                equality_delete_metadatas.push(EqualityDeleteMetadata::new(
                    equality_delete_schema,
                    equality_delete_table_name,
                ));
                equality_ids = Some(task.equality_ids.clone());
            }

            // Add the file scan task to the last metadata
            if let Some(last_metadata) = equality_delete_metadatas.last_mut() {
                last_metadata.add_file_scan_task(task.clone());
            }
        }

        let need_file_path_and_pos = !self.position_delete_files.is_empty();
        let need_seq_num = !equality_delete_metadatas.is_empty();

        // Build schema for data file, old schema + seq_num + file_path + pos
        let project_names: Vec<_> = self
            .schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.clone())
            .collect();
        let highest_field_id = self.schema.highest_field_id();
        let mut add_schema_fields = vec![];
        // add sequence number column if needed
        if need_seq_num {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 1,
                SYS_HIDDEN_SEQ_NUM,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // add file path and position column if needed
        if need_file_path_and_pos {
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 2,
                SYS_HIDDEN_FILE_PATH,
                Type::Primitive(PrimitiveType::String),
                true,
            )));
            add_schema_fields.push(Arc::new(NestedField::new(
                highest_field_id + 3,
                SYS_HIDDEN_POS,
                Type::Primitive(PrimitiveType::Long),
                true,
            )));
        }
        // data file schema is old schema + seq_num + file_path + pos. used for data file table provider
        let data_file_schema = self
            .schema
            .as_ref()
            .clone()
            .into_builder()
            .with_fields(add_schema_fields)
            .build()?;
        // input schema is old schema. used for data file writer
        let input_schema = self.schema.as_ref().clone();

        let sql_builder = SqlBuilder::new(
            &project_names,
            Some(table_name::build_position_delete_table_name(
                &self.table_prefix,
            )),
            Some(table_name::build_data_file_table_name(&self.table_prefix)),
            &equality_delete_metadatas,
            need_file_path_and_pos,
        );

        let exec_sql = sql_builder.build_merge_on_read_sql()?;

        Ok(DataFusionTaskContext {
            data_file_schema: Some(data_file_schema),
            input_schema: Some(input_schema),
            data_files: Some(self.data_files),
            position_delete_files: if need_file_path_and_pos {
                Some(self.position_delete_files)
            } else {
                None
            },
            equality_delete_files: if need_seq_num {
                Some(self.equality_delete_files)
            } else {
                None
            },
            position_delete_schema: if need_file_path_and_pos {
                Some(position_delete_schema)
            } else {
                None
            },
            equality_delete_metadatas: if need_seq_num {
                Some(equality_delete_metadatas)
            } else {
                None
            },
            exec_sql,
            table_prefix: self.table_prefix,
        })
    }

    /// Builds an equality delete schema based on the given equality_ids
    fn build_equality_delete_schema(
        &self,
        equality_ids: &[i32],
        highest_field_id: &mut i32,
    ) -> Result<Schema> {
        let mut equality_delete_fields = Vec::with_capacity(equality_ids.len());
        for id in equality_ids {
            let field = self
                .schema
                .field_by_id(*id)
                .ok_or_else(|| CompactionError::Execution("equality_ids not found".to_owned()))?;
            equality_delete_fields.push(field.clone());
        }
        *highest_field_id += 1;
        equality_delete_fields.push(Arc::new(NestedField::new(
            *highest_field_id,
            SYS_HIDDEN_SEQ_NUM,
            Type::Primitive(PrimitiveType::Long),
            true,
        )));

        Schema::builder()
            .with_fields(equality_delete_fields)
            .build()
            .map_err(CompactionError::Iceberg)
    }
}

impl DataFusionTaskContext {
    pub fn builder() -> Result<DataFusionTaskContextBuilder> {
        Ok(DataFusionTaskContextBuilder {
            schema: Arc::new(Schema::builder().build()?),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
            table_prefix: "".to_owned(),
        })
    }

    pub fn need_file_path_and_pos(&self) -> bool {
        // Must be consistent with builder logic: !self.position_delete_files.is_empty()
        // We check if position_delete_schema exists, which is set only when position deletes are present
        self.position_delete_schema.is_some()
    }

    pub fn need_seq_num(&self) -> bool {
        // Must be consistent with builder logic: !equality_delete_metadatas.is_empty()
        // We check if equality_delete_metadatas exists and is not empty
        self.equality_delete_metadatas
            .as_ref()
            .is_some_and(|v| !v.is_empty())
    }

    pub fn data_file_table_name(&self) -> String {
        table_name::build_data_file_table_name(&self.table_prefix)
    }

    pub fn position_delete_table_name(&self) -> String {
        table_name::build_position_delete_table_name(&self.table_prefix)
    }

    pub fn equality_delete_table_name(&self, table_idx: usize) -> String {
        table_name::build_equality_delete_table_name(&self.table_prefix, table_idx)
    }
}

/// Metadata for equality delete files
#[derive(Debug, Clone)]
pub(crate) struct EqualityDeleteMetadata {
    pub(crate) equality_delete_schema: Schema,
    pub(crate) equality_delete_table_name: String,
    pub(crate) file_scan_tasks: Vec<FileScanTask>,
}

impl EqualityDeleteMetadata {
    pub fn new(equality_delete_schema: Schema, equality_delete_table_name: String) -> Self {
        Self {
            equality_delete_schema,
            equality_delete_table_name,
            file_scan_tasks: Vec::new(),
        }
    }

    pub fn equality_delete_join_names(&self) -> Vec<&str> {
        self.equality_delete_schema
            .as_struct()
            .fields()
            .iter()
            .map(|i| i.name.as_str())
            .filter(|name| !SYS_HIDDEN_COLS.contains(name))
            .collect()
    }

    pub fn add_file_scan_task(&mut self, file_scan_task: FileScanTask) {
        self.file_scan_tasks.push(file_scan_task);
    }
}

mod table_name {
    pub const DATA_FILE_TABLE: &str = "data_file_table";
    pub const POSITION_DELETE_TABLE: &str = "position_delete_table";
    pub const EQUALITY_DELETE_TABLE: &str = "equality_delete_table";

    pub fn build_data_file_table_name(table_prefix: &str) -> String {
        format!("{}_{}", table_prefix, DATA_FILE_TABLE)
    }

    pub fn build_position_delete_table_name(table_prefix: &str) -> String {
        format!("{}_{}", table_prefix, POSITION_DELETE_TABLE)
    }

    // Builds the equality delete table name with a prefix and index
    // index is used to differentiate multiple equality delete tables (schema)
    pub fn build_equality_delete_table_name(table_prefix: &str, table_idx: usize) -> String {
        format!("{}_{}_{}", table_prefix, EQUALITY_DELETE_TABLE, table_idx)
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::datafusion::datafusion_processor::table_name::{
        DATA_FILE_TABLE, POSITION_DELETE_TABLE,
    };

    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    /// Test building SQL with no delete files
    #[test]
    fn test_build_merge_on_read_sql_no_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_join_names,
            false,
        );
        assert_eq!(
            builder.build_merge_on_read_sql().unwrap(),
            format!(
                "SELECT {} FROM {}",
                project_names.join(", "),
                DATA_FILE_TABLE
            )
        );
    }

    /// Test building SQL with position delete files
    #[test]
    fn test_build_merge_on_read_sql_with_position_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_join_names = Vec::new();

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_join_names,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = format!(
            "SELECT id, name FROM (SELECT id, name, sys_hidden_file_path, sys_hidden_pos FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_file_path, sys_hidden_pos FROM {}) AS {} ON {}.sys_hidden_file_path = {}.sys_hidden_file_path AND {}.sys_hidden_pos = {}.sys_hidden_pos) AS final_result",
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE
        );
        assert_eq!(sql, expected_sql);
    }

    /// Test building SQL with equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = format!(
            "SELECT id, name FROM (SELECT id, name, sys_hidden_seq_num FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num FROM {}) AS {} ON {}.id = {}.id AND {}.sys_hidden_seq_num < {}.sys_hidden_seq_num) AS final_result",
            equality_delete_table_name,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name
        );
        assert_eq!(sql, expected_sql);
    }

    /// Test building SQL with equality delete files AND sequence number comparison
    #[test]
    fn test_build_merge_on_read_sql_with_equality_deletes_and_seq_num() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];

        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = format!(
            "SELECT id, name FROM (SELECT id, name, sys_hidden_seq_num FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num FROM {}) AS {} ON {}.id = {}.id AND {}.sys_hidden_seq_num < {}.sys_hidden_seq_num) AS final_result",
            equality_delete_table_name,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name
        );
        assert_eq!(sql, expected_sql);
    }

    /// Test building SQL with both position AND equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_both_deletes() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_table_name = "test".to_owned();
        let equality_delete_metadatas = vec![EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![Arc::new(NestedField::new(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                    true,
                ))])
                .build()
                .unwrap(),
            equality_delete_table_name.clone(),
        )];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            true,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = format!(
            "SELECT id, name FROM (SELECT id, name, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM {}) AS {} ON {}.sys_hidden_file_path = {}.sys_hidden_file_path AND {}.sys_hidden_pos = {}.sys_hidden_pos) AS {} ON {}.id = {}.id AND {}.sys_hidden_seq_num < {}.sys_hidden_seq_num) AS final_result",
            equality_delete_table_name,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            POSITION_DELETE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name
        );
        assert_eq!(sql, expected_sql);
    }

    /// Test building SQL with multiple equality delete files
    #[test]
    fn test_build_merge_on_read_sql_with_multiple_equality_deletes_schema() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];

        let equality_delete_table_name_1 = "test_1".to_owned();
        let equality_delete_table_name_2 = "test_2".to_owned();
        let equality_delete_metadatas = vec![
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_1.clone(),
            ),
            EqualityDeleteMetadata::new(
                Schema::builder()
                    .with_fields(vec![Arc::new(NestedField::new(
                        2,
                        "name",
                        Type::Primitive(PrimitiveType::String),
                        true,
                    ))])
                    .build()
                    .unwrap(),
                equality_delete_table_name_2.clone(),
            ),
        ];

        let builder = SqlBuilder::new(
            &project_names,
            Some(POSITION_DELETE_TABLE.to_owned()),
            Some(DATA_FILE_TABLE.to_owned()),
            &equality_delete_metadatas,
            false,
        );
        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = format!(
            "SELECT id, name FROM (SELECT id, name, sys_hidden_seq_num FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num FROM {} RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num FROM {}) AS {} ON {}.id = {}.id AND {}.sys_hidden_seq_num < {}.sys_hidden_seq_num) AS {} ON {}.name = {}.name AND {}.sys_hidden_seq_num < {}.sys_hidden_seq_num) AS final_result",
            equality_delete_table_name_2,
            equality_delete_table_name_1,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name_1,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name_1,
            DATA_FILE_TABLE,
            equality_delete_table_name_2,
            DATA_FILE_TABLE,
            DATA_FILE_TABLE,
            equality_delete_table_name_2
        );
        assert_eq!(sql, expected_sql);
    }

    #[test]
    fn test_build_equality_delete_schema() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::new(
                    1,
                    "id",
                    iceberg::spec::Type::Primitive(PrimitiveType::Int),
                    true,
                )),
                Arc::new(NestedField::new(
                    2,
                    "name",
                    iceberg::spec::Type::Primitive(PrimitiveType::String),
                    true,
                )),
            ])
            .build()
            .unwrap();

        let mut highest_field_id = schema.highest_field_id();

        let builder = DataFusionTaskContextBuilder {
            schema: Arc::new(schema),
            data_files: vec![],
            position_delete_files: vec![],
            equality_delete_files: vec![],
            table_prefix: "".to_owned(),
        };

        let equality_ids = vec![1, 2];
        let equality_delete_schema = builder
            .build_equality_delete_schema(&equality_ids, &mut highest_field_id)
            .unwrap();

        assert_eq!(equality_delete_schema.as_struct().fields().len(), 3);
        assert_eq!(equality_delete_schema.as_struct().fields()[0].name, "id");
        assert_eq!(equality_delete_schema.as_struct().fields()[1].name, "name");
        assert_eq!(
            equality_delete_schema.as_struct().fields()[2].name,
            "sys_hidden_seq_num"
        );
        assert_eq!(highest_field_id, 3);
    }

    #[test]
    fn test_equality_delete_join_names() {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
        use std::sync::Arc;

        // schema
        let fields = vec![
            Arc::new(NestedField::new(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
                true,
            )),
            Arc::new(NestedField::new(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
                true,
            )),
            Arc::new(NestedField::new(
                3,
                "sys_hidden_seq_num",
                Type::Primitive(PrimitiveType::Long),
                true,
            )),
            Arc::new(NestedField::new(
                4,
                "sys_hidden_file_path",
                Type::Primitive(PrimitiveType::String),
                true,
            )),
        ];
        let schema = Schema::builder().with_fields(fields).build().unwrap();

        let meta = EqualityDeleteMetadata {
            equality_delete_schema: schema,
            equality_delete_table_name: "test_table".to_string(),
            file_scan_tasks: vec![],
        };

        let join_names = meta.equality_delete_join_names();
        assert_eq!(join_names, vec!["id", "name"]);
    }

    /// Test that verifies the fix for nested table alias issue in SQL generation
    ///
    /// This test ensures that when we have both position deletes and equality deletes,
    /// the generated SQL correctly includes hidden columns in all nested subqueries,
    /// preventing the "No field named _data_file_table.sys_hidden_seq_num" error.
    #[test]
    fn test_nested_table_alias_hidden_columns_fix() {
        let project_names = vec![
            "id".to_owned(),
            "item_name".to_owned(),
            "description".to_owned(),
        ];

        // Create equality delete metadata that requires sys_hidden_seq_num
        let equality_delete_metadata = EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    )),
                    Arc::new(NestedField::new(
                        4,
                        SYS_HIDDEN_SEQ_NUM,
                        Type::Primitive(PrimitiveType::Long),
                        true,
                    )),
                ])
                .build()
                .unwrap(),
            "_equality_delete_table_0".to_string(),
        );

        let equality_delete_metadatas = vec![equality_delete_metadata];

        // Test scenario: BOTH position deletes AND equality deletes
        // This creates the most complex nested SQL structure
        let builder = SqlBuilder::new(
            &project_names,
            Some("_position_delete_table".to_string()),
            Some("_data_file_table".to_string()),
            &equality_delete_metadatas,
            true, // need_file_path_and_pos = true (triggers position delete logic)
        );

        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = "SELECT id, item_name, description FROM (SELECT id, item_name, description, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM _equality_delete_table_0 RIGHT ANTI JOIN (SELECT id, item_name, description, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM _position_delete_table RIGHT ANTI JOIN (SELECT id, item_name, description, sys_hidden_seq_num, sys_hidden_file_path, sys_hidden_pos FROM _data_file_table) AS _data_file_table ON _data_file_table.sys_hidden_file_path = _position_delete_table.sys_hidden_file_path AND _data_file_table.sys_hidden_pos = _position_delete_table.sys_hidden_pos) AS _data_file_table ON _equality_delete_table_0.id = _data_file_table.id AND _data_file_table.sys_hidden_seq_num < _equality_delete_table_0.sys_hidden_seq_num) AS final_result";
        assert_eq!(sql, expected_sql);
    }

    /// Test that verifies SQL generation works correctly with only equality deletes
    ///
    /// This is a simpler case but still important to verify that hidden columns
    /// are properly handled when there's only one level of nesting.
    #[test]
    fn test_equality_deletes_only_hidden_columns() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];

        let equality_delete_metadata = EqualityDeleteMetadata::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::new(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Int),
                        true,
                    )),
                    Arc::new(NestedField::new(
                        3,
                        SYS_HIDDEN_SEQ_NUM,
                        Type::Primitive(PrimitiveType::Long),
                        true,
                    )),
                ])
                .build()
                .unwrap(),
            "_equality_delete_table_0".to_string(),
        );

        let equality_delete_metadatas = vec![equality_delete_metadata];

        // Test scenario: ONLY equality deletes (no position deletes)
        let builder = SqlBuilder::new(
            &project_names,
            None, // No position delete table
            Some("_data_file_table".to_string()),
            &equality_delete_metadatas,
            false, // need_file_path_and_pos = false
        );

        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = "SELECT id, name FROM (SELECT id, name, sys_hidden_seq_num FROM _equality_delete_table_0 RIGHT ANTI JOIN (SELECT id, name, sys_hidden_seq_num FROM _data_file_table) AS _data_file_table ON _equality_delete_table_0.id = _data_file_table.id AND _data_file_table.sys_hidden_seq_num < _equality_delete_table_0.sys_hidden_seq_num) AS final_result";
        assert_eq!(sql, expected_sql);
    }

    /// Test that verifies SQL generation works correctly with only position deletes
    ///
    /// This tests the case where we need file path and position columns but not sequence numbers.
    #[test]
    fn test_position_deletes_only_hidden_columns() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_metadatas = vec![]; // No equality deletes

        // Test scenario: ONLY position deletes (no equality deletes)
        let builder = SqlBuilder::new(
            &project_names,
            Some("_position_delete_table".to_string()),
            Some("_data_file_table".to_string()),
            &equality_delete_metadatas,
            true, // need_file_path_and_pos = true
        );

        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = "SELECT id, name FROM (SELECT id, name, sys_hidden_file_path, sys_hidden_pos FROM _position_delete_table RIGHT ANTI JOIN (SELECT id, name, sys_hidden_file_path, sys_hidden_pos FROM _data_file_table) AS _data_file_table ON _data_file_table.sys_hidden_file_path = _position_delete_table.sys_hidden_file_path AND _data_file_table.sys_hidden_pos = _position_delete_table.sys_hidden_pos) AS final_result";
        assert_eq!(sql, expected_sql);
    }

    /// Test that verifies SQL generation works correctly with no deletes
    ///
    /// This is the simplest case - should not add any hidden columns or wrap in final_result.
    #[test]
    fn test_no_deletes_no_hidden_columns() {
        let project_names = vec!["id".to_owned(), "name".to_owned()];
        let equality_delete_metadatas = vec![]; // No equality deletes

        // Test scenario: NO deletes at all
        let builder = SqlBuilder::new(
            &project_names,
            None, // No position delete table
            Some("_data_file_table".to_string()),
            &equality_delete_metadatas,
            false, // need_file_path_and_pos = false
        );

        let sql = builder.build_merge_on_read_sql().unwrap();

        let expected_sql = "SELECT id, name FROM _data_file_table";
        assert_eq!(sql, expected_sql);
    }
}
