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

use serde::Deserialize;
use serde_with::serde_as;

const DEFAULT_PREFIX: &str = "10";
const DEFAULT_BATCH_PARALLELISM: usize = 4;
const DEFAULT_TARGET_PARTITIONS: usize = 4;
const DEFAULT_TARGET_FILE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GB
const DEFAULT_VALIDATE_COMPACTION: bool = false;

#[serde_as]
#[derive(Debug, Deserialize)]
pub struct CompactionConfig {
    pub batch_parallelism: usize,
    pub target_partitions: usize,
    pub data_file_prefix: String,
    pub target_file_size: u64,
    pub enable_validate_compaction: bool,
}

impl CompactionConfig {
    pub fn builder() -> CompactionConfigBuilder {
        CompactionConfigBuilder::default()
    }
}

#[derive(Default)]
pub struct CompactionConfigBuilder {
    batch_parallelism: Option<usize>,
    target_partitions: Option<usize>,
    data_file_prefix: Option<String>,
    target_file_size: Option<u64>,
    enable_validate_compaction: Option<bool>,
}

impl CompactionConfigBuilder {
    pub fn batch_parallelism(mut self, value: usize) -> Self {
        self.batch_parallelism = Some(value);
        self
    }

    pub fn target_partitions(mut self, value: usize) -> Self {
        self.target_partitions = Some(value);
        self
    }

    pub fn data_file_prefix(mut self, value: String) -> Self {
        self.data_file_prefix = Some(value);
        self
    }

    pub fn target_file_size(mut self, value: u64) -> Self {
        self.target_file_size = Some(value);
        self
    }

    pub fn enable_validate_compaction(mut self, value: bool) -> Self {
        self.enable_validate_compaction = Some(value);
        self
    }

    pub fn build(self) -> CompactionConfig {
        CompactionConfig {
            batch_parallelism: self.batch_parallelism.unwrap_or(DEFAULT_BATCH_PARALLELISM),
            target_partitions: self.target_partitions.unwrap_or(DEFAULT_TARGET_PARTITIONS),
            data_file_prefix: self.data_file_prefix.unwrap_or(DEFAULT_PREFIX.to_owned()),
            target_file_size: self.target_file_size.unwrap_or(DEFAULT_TARGET_FILE_SIZE),
            enable_validate_compaction: self
                .enable_validate_compaction
                .unwrap_or(DEFAULT_VALIDATE_COMPACTION),
        }
    }
}
