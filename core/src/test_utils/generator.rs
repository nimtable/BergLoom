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

use crate::{CompactionError, error::Result, executor::datafusion::build_parquet_writer_builder};
use futures_async_stream::try_stream;
use iceberg::{
    arrow::{arrow_schema_to_schema, schema_to_arrow_schema},
    io::FileIO,
    spec::{DataFile, Schema},
    table::Table,
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
            sort_position_delete_writer::{
                POSITION_DELETE_SCHEMA, SortPositionDeleteWriterBuilder,
            },
        },
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
        },
        function_writer::equality_delta_writer::{
            DELETE_OP, EqualityDeltaWriterBuilder, INSERT_OP,
        },
    },
};
use parquet::file::properties::WriterProperties;
use rand::{Rng, distr::Alphanumeric};
use std::sync::Arc;

use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    },
    compute::filter,
    datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema},
};
use futures::StreamExt;

const DEFAULT_DATA_FILE_ROW_COUNT: usize = 10000;
const DEFAULT_EQUALITY_DELETE_ROW_COUNT: usize = 200;
const DEFAULT_POSITION_DELETE_ROW_COUNT: usize = 300;
const DEFAULT_DATA_FILE_NUM: usize = 10;
const DEFAULT_BATCH_SIZE: usize = 512;
const DEFAULT_STRING_LENGTH: usize = 16;
const DEFAULT_DATA_FILE_PREFIX: &str = "test_berg_loom";
const DEFAULT_DATA_SUBDIR: &str = "/data";

pub type EqualityDeleteDeltaWriterBuilder = EqualityDeltaWriterBuilder<
    DataFileWriterBuilder<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>>,
    SortPositionDeleteWriterBuilder<
        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
    >,
    EqualityDeleteFileWriterBuilder<
        ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>,
    >,
>;

pub struct RecordBatchGenerator {
    pub num_rows: usize,
    pub batch_size: usize,
    pub schema: ArrowSchema,
}

impl RecordBatchGenerator {
    /// Creates a new RecordBatchGenerator with specified parameters
    ///
    /// # Arguments
    /// * `num_rows` - Total number of rows to generate across all batches
    /// * `batch_size` - Number of rows per batch
    /// * `schema` - Arrow schema defining the structure of the data
    pub fn new(num_rows: usize, batch_size: usize, schema: ArrowSchema) -> Self {
        Self {
            num_rows,
            batch_size,
            schema,
        }
    }

    /// Generates record batches asynchronously as a stream
    ///
    /// This method yields record batches of the specified batch size until
    /// all rows have been generated. The last batch may contain fewer rows
    /// if the total number of rows is not evenly divisible by batch size.
    #[try_stream(boxed, ok = RecordBatch, error = CompactionError)]
    pub async fn generate(&self) {
        let mut num_rows = self.num_rows;
        loop {
            if num_rows > self.batch_size {
                let batch = self.generate_batch(self.batch_size).unwrap();
                num_rows -= self.batch_size;
                yield batch;
            } else if num_rows > 0 {
                let batch = self.generate_batch(num_rows).unwrap();
                num_rows = 0;
                yield batch;
            } else {
                break;
            }
        }
    }

    /// Generates a single record batch with random data
    ///
    /// # Arguments
    /// * `batch_size` - Number of rows to generate in this batch
    ///
    /// # Returns
    /// A RecordBatch containing randomly generated data according to the schema
    pub fn generate_batch(&self, batch_size: usize) -> Result<RecordBatch> {
        let arrays = self
            .schema
            .fields()
            .iter()
            .map(|field| match field.data_type() {
                datafusion::arrow::datatypes::DataType::Boolean => Arc::new(BooleanArray::from(
                    (0..batch_size)
                        .map(|_| rand::random::<bool>())
                        .collect::<Vec<bool>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Int8 => Arc::new(Int8Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<i8>())
                        .collect::<Vec<i8>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Int16 => Arc::new(Int16Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<i16>())
                        .collect::<Vec<i16>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Int32 => Arc::new(Int32Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<i32>())
                        .collect::<Vec<i32>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<i64>())
                        .collect::<Vec<i64>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::UInt8 => Arc::new(UInt8Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<u8>())
                        .collect::<Vec<u8>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::UInt16 => Arc::new(UInt16Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<u16>())
                        .collect::<Vec<u16>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::UInt32 => Arc::new(UInt32Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<u32>())
                        .collect::<Vec<u32>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::UInt64 => Arc::new(UInt64Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<u64>())
                        .collect::<Vec<u64>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Float32 => Arc::new(Float32Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<f32>())
                        .collect::<Vec<f32>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Float64 => Arc::new(Float64Array::from(
                    (0..batch_size)
                        .map(|_| rand::random::<f64>())
                        .collect::<Vec<f64>>(),
                )) as ArrayRef,
                datafusion::arrow::datatypes::DataType::Utf8 => Arc::new(StringArray::from(
                    (0..batch_size)
                        .map(|_| generate_string(DEFAULT_STRING_LENGTH))
                        .collect::<Vec<String>>(),
                )) as ArrayRef,
                _ => unimplemented!("Unsupported data type: {:?}", field.data_type()),
            })
            .collect::<Vec<ArrayRef>>();
        RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)
            .map_err(|e| CompactionError::Test(e.to_string()))
    }
}

/// Generates a random string of variable length up to the specified maximum
///
/// # Arguments
/// * `len` - Maximum length of the generated string
///
/// # Returns
/// A randomly generated alphanumeric string
pub fn generate_string(len: usize) -> String {
    let len = rand::rng().random_range(0..=len);
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// Configuration for file generation with default values
#[derive(Clone)]
pub struct FileGeneratorConfig {
    /// Number of rows per data file
    pub data_file_row_count: usize,
    /// Number of equality delete rows to generate
    pub equality_delete_row_count: usize,
    /// Number of position delete rows to generate
    pub position_delete_row_count: usize,
    /// Total number of data files to generate
    pub data_file_num: usize,
    /// Number of rows per batch during generation
    pub batch_size: usize,
}

impl Default for FileGeneratorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl FileGeneratorConfig {
    /// Creates a new FileGeneratorConfig with default settings
    pub fn new() -> Self {
        Self {
            data_file_row_count: DEFAULT_DATA_FILE_ROW_COUNT,
            equality_delete_row_count: DEFAULT_EQUALITY_DELETE_ROW_COUNT,
            position_delete_row_count: DEFAULT_POSITION_DELETE_ROW_COUNT,
            data_file_num: DEFAULT_DATA_FILE_NUM,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Sets the number of rows per data file
    pub fn with_data_file_row_count(mut self, data_file_row_count: usize) -> Self {
        self.data_file_row_count = data_file_row_count;
        self
    }

    /// Sets the number of equality delete rows to generate
    pub fn with_equality_delete_row_count(mut self, equality_delete_row_count: usize) -> Self {
        self.equality_delete_row_count = equality_delete_row_count;
        self
    }

    /// Sets the number of position delete rows to generate
    pub fn with_position_delete_row_count(mut self, position_delete_row_count: usize) -> Self {
        self.position_delete_row_count = position_delete_row_count;
        self
    }

    /// Sets the total number of data files to generate
    pub fn with_data_file_num(mut self, data_file_num: usize) -> Self {
        self.data_file_num = data_file_num;
        self
    }

    /// Sets the batch size for record generation
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

pub struct FileGenerator {
    pub record_batch_generator: RecordBatchGenerator,
    pub config: FileGeneratorConfig,
    pub schema: Arc<Schema>,
    pub writer_config: WriterConfig,
}

#[derive(Clone)]
pub struct WriterConfig {
    pub data_file_prefix: String,
    pub dir_path: String,
    pub file_io: FileIO,
    pub equality_ids: Vec<i32>,
}

impl WriterConfig {
    /// Creates a new WriterConfig from a table with default settings
    ///
    /// # Arguments
    /// * `table` - The Iceberg table to use for configuration
    pub fn new(table: &Table) -> Self {
        Self {
            data_file_prefix: DEFAULT_DATA_FILE_PREFIX.to_owned(),
            file_io: table.file_io().clone(),
            dir_path: format!("{}{}", table.metadata().location(), DEFAULT_DATA_SUBDIR),
            equality_ids: vec![1],
        }
    }
}

impl FileGenerator {
    /// Creates a new FileGenerator with the specified configuration
    ///
    /// # Arguments
    /// * `config` - Configuration for file generation
    /// * `schema` - Iceberg schema for the generated data
    /// * `writer_config` - Configuration for file writers
    ///
    /// # Returns
    /// A configured FileGenerator instance
    pub fn new(
        config: FileGeneratorConfig,
        schema: Arc<Schema>,
        writer_config: WriterConfig,
    ) -> Result<Self> {
        let arrow_schema = schema_to_arrow_schema(&schema)?;
        let record_batch_generator = RecordBatchGenerator::new(
            config.data_file_row_count * config.data_file_num,
            config.batch_size,
            arrow_schema,
        );
        Ok(Self {
            record_batch_generator,
            config,
            schema,
            writer_config,
        })
    }

    /// Builds an equality delete delta writer builder for managing different types of writes
    ///
    /// This method creates a writer builder that can handle:
    /// - Data file writes
    /// - Position delete writes  
    /// - Equality delete writes
    ///
    /// # Returns
    /// A configured EqualityDeleteDeltaWriterBuilder
    async fn build_equality_delete_delta_writer_builder(
        &self,
    ) -> Result<EqualityDeleteDeltaWriterBuilder> {
        let WriterConfig {
            data_file_prefix,
            dir_path,
            file_io,
            equality_ids,
        } = self.writer_config.clone();
        let parquet_writer_builder = build_parquet_writer_builder(
            data_file_prefix.clone(),
            dir_path.clone(),
            self.schema.clone(),
            file_io.clone(),
            WriterProperties::default(),
        )
        .await?;
        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);

        let parquet_writer_builder = build_parquet_writer_builder(
            data_file_prefix.clone(),
            dir_path.clone(),
            POSITION_DELETE_SCHEMA.clone(),
            file_io.clone(),
            WriterProperties::default(),
        )
        .await?;
        let position_delete_file_writer_builder = SortPositionDeleteWriterBuilder::new(
            parquet_writer_builder.clone(),
            self.config.position_delete_row_count,
            None,
            None,
        );
        let equality_delete_writer_config =
            EqualityDeleteWriterConfig::new(equality_ids.clone(), self.schema.clone(), None, 0)?;
        let parquet_writer_builder = build_parquet_writer_builder(
            data_file_prefix.clone(),
            dir_path.clone(),
            Arc::new(arrow_schema_to_schema(
                equality_delete_writer_config.projected_arrow_schema_ref(),
            )?),
            file_io.clone(),
            WriterProperties::default(),
        )
        .await?;
        let equality_delete_file_writer_builder = EqualityDeleteFileWriterBuilder::new(
            parquet_writer_builder.clone(),
            EqualityDeleteWriterConfig::new(equality_ids.clone(), self.schema.clone(), None, 0)?,
        );

        let iceberg_writer_builder = EqualityDeltaWriterBuilder::new(
            data_file_writer_builder,
            position_delete_file_writer_builder,
            equality_delete_file_writer_builder,
            equality_ids,
        );
        Ok(iceberg_writer_builder)
    }

    /// Generates data files with random data, equality deletes, and position deletes
    ///
    /// This method orchestrates the generation of:
    /// 1. Data files with random records
    /// 2. Equality delete files that mark certain records for deletion
    /// 3. Position delete files that mark specific row positions for deletion
    ///
    /// The generation process creates batches of data and applies delete operations
    /// at specified rates to simulate real-world data patterns.
    ///
    /// # Returns
    /// A vector of DataFile objects representing the generated files
    pub async fn generate(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = Vec::new();

        let equality_delete_delta_writer_builder =
            self.build_equality_delete_delta_writer_builder().await?;
        let mut equality_delete_delta_writer =
            equality_delete_delta_writer_builder.clone().build().await?;

        let equality_delete_rate =
            self.config.data_file_row_count / self.config.equality_delete_row_count + 1;
        let position_delete_rate =
            self.config.data_file_row_count / self.config.position_delete_row_count + 1;

        let mut data_file_num = 0;

        let mut stream = self.record_batch_generator.generate().boxed();
        let schema_with_extra_op_column = {
            let arrow_schema = schema_to_arrow_schema(&self.schema)?;
            let mut new_fields = arrow_schema.fields().iter().cloned().collect::<Vec<_>>();
            new_fields.push(Arc::new(ArrowField::new(
                "op".to_owned(),
                ArrowDataType::Int32,
                false,
            )));
            Arc::new(ArrowSchema::new(new_fields))
        };

        let build_delete_batch = |batch: &RecordBatch, delete_rate: usize, num_rows: usize| {
            let mask = BooleanArray::from_iter((0..num_rows).map(|i| Some(i % delete_rate == 0)));

            let mut filtered_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| filter(col, &mask).unwrap())
                .collect();
            filtered_columns.push(Arc::new(Int32Array::from(vec![
                DELETE_OP;
                filtered_columns[0].len()
            ])) as ArrayRef);
            RecordBatch::try_new(schema_with_extra_op_column.clone(), filtered_columns)
                .map_err(|e| CompactionError::Test(e.to_string()))
        };

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let num_rows = batch.num_rows();

            if data_file_num + num_rows > self.config.data_file_row_count {
                data_files.extend(equality_delete_delta_writer.close().await?);
                equality_delete_delta_writer =
                    equality_delete_delta_writer_builder.clone().build().await?;
                data_file_num = 0;
            }

            // 1. add equality delete

            let equality_delete_batch = build_delete_batch(&batch, equality_delete_rate, num_rows)?;
            equality_delete_delta_writer
                .write(equality_delete_batch)
                .await?;

            // 2. add data file
            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(Int32Array::from(vec![INSERT_OP; num_rows])) as ArrayRef);
            let batch_with_op = RecordBatch::try_new(schema_with_extra_op_column.clone(), columns)
                .map_err(|e| CompactionError::Test(e.to_string()))?;
            equality_delete_delta_writer.write(batch_with_op).await?;

            // 3. add position delete
            let position_delete_batch = build_delete_batch(&batch, position_delete_rate, num_rows)?;
            equality_delete_delta_writer
                .write(position_delete_batch)
                .await?;
        }
        data_files.extend(equality_delete_delta_writer.close().await?);
        Ok(data_files)
    }
}
