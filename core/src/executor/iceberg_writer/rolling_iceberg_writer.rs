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

use datafusion::arrow::array::RecordBatch;
use iceberg::Result;
use iceberg::{
    spec::DataFile,
    writer::{CurrentFileStatus, IcebergWriter, IcebergWriterBuilder},
};

#[derive(Clone)]
/// RollingIcebergWriter wraps an IcebergWriter and splits output files by target size.
pub struct RollingIcebergWriter<B, D> {
    /// Builder for creating new inner writers.
    inner_writer_builder: B,
    /// The current active writer.
    inner_writer: D,
    /// Target file size in bytes. When exceeded, a new file is started.
    target_file_size: usize,
    /// Collected data files that have been closed.
    data_files: Vec<DataFile>,
    /// Current written size of the active file.
    current_written_size: usize,
}

#[async_trait::async_trait]
impl<B, D> IcebergWriter for RollingIcebergWriter<B, D>
where
    B: IcebergWriterBuilder<R = D>,
    D: IcebergWriter + CurrentFileStatus,
{
    /// Write a RecordBatch. If the current file size plus the new batch size
    /// exceeds the target, close the current file and start a new one.
    async fn write(&mut self, input: RecordBatch) -> Result<()> {
        let input_size = input.get_array_memory_size();
        // If adding this batch would exceed the target file size, close current file and start a new one.
        if need_build_new_file(self.current_written_size, input_size, self.target_file_size) {
            let data_files = self.inner_writer.close().await?;
            self.data_files.extend(data_files);
            self.inner_writer = self.inner_writer_builder.clone().build().await?;
            self.current_written_size = 0;
        }
        // Write the batch to the current writer.
        self.inner_writer.write(input).await?;
        self.current_written_size += input_size;
        Ok(())
    }

    /// Close the writer, ensuring all data files are finalized and returned.
    async fn close(&mut self) -> Result<Vec<DataFile>> {
        let mut data_files = std::mem::take(&mut self.data_files);
        data_files.extend(self.inner_writer.close().await?);
        Ok(data_files)
    }
}

pub fn need_build_new_file(
    current_written_size: usize,
    input_size: usize,
    target_file_size: usize,
) -> bool {
    // If the current file size is less than 10% of the target size, don't build a new file.
    if current_written_size < target_file_size / 10 {
        return false;
    }
    // If the total size of the current file and the new batch would exceed 1.5x the target size, build a new file.
    if current_written_size + input_size > target_file_size * 3 / 2 {
        return true;
    }
    // If the total size of the current file and the new batch would exceed the target size, build a new file.
    if current_written_size + input_size > target_file_size
        && current_written_size > target_file_size * 7 / 10
    {
        return true;
    }
    false
}

#[derive(Clone)]
/// Builder for RollingIcebergWriter.
pub struct RollingIcebergWriterBuilder<B> {
    inner_builder: B,
    target_file_size: usize,
}

impl<B> RollingIcebergWriterBuilder<B> {
    /// Create a new RollingIcebergWriterBuilder.
    pub fn new(inner_builder: B, target_file_size: usize) -> Self {
        Self {
            inner_builder,
            target_file_size,
        }
    }
}

#[async_trait::async_trait]
impl<B> IcebergWriterBuilder for RollingIcebergWriterBuilder<B>
where
    B: IcebergWriterBuilder,
    B::R: IcebergWriter + CurrentFileStatus,
{
    type R = RollingIcebergWriter<B, B::R>;

    /// Build a new RollingIcebergWriter.
    async fn build(self) -> Result<Self::R> {
        Ok(RollingIcebergWriter {
            inner_writer_builder: self.inner_builder.clone(),
            inner_writer: self.inner_builder.build().await?,
            target_file_size: self.target_file_size,
            data_files: Vec::new(),
            current_written_size: 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_need_build_new_file_total_size_exceeds_threshold() {
        let target_size = 1000;

        // Test when total size exceeds 1.5x target size
        assert!(need_build_new_file(800, 800, target_size)); // 1600 > 1500
        assert!(need_build_new_file(1000, 600, target_size)); // 1600 > 1500
    }

    #[test]
    fn test_need_build_new_file_normal_cases() {
        let target_size = 1000;

        // Case 1: Current file size > 70% and total size would exceed target
        assert!(need_build_new_file(800, 300, target_size));

        // Case 2: Current file size > 70% but total size would not exceed target
        assert!(!need_build_new_file(800, 100, target_size));

        // Case 3: Current file size < 70% even though total size would exceed target
        assert!(!need_build_new_file(600, 500, target_size));
    }

    #[test]
    fn test_need_build_new_file_edge_cases() {
        let target_size = 1000;

        // Empty file case
        assert!(!need_build_new_file(0, 2000, target_size));

        // Exactly at 70% threshold
        assert!(!need_build_new_file(700, 400, target_size));

        // Just over 70% threshold
        assert!(need_build_new_file(701, 400, target_size));

        // Exactly at 1.5x threshold
        assert!(!need_build_new_file(0, 1500, target_size));

        // Just over 1.5x threshold
        assert!(!need_build_new_file(1, 1501, target_size));
    }
}
