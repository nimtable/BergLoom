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

use mixtrics::metrics::{BoxedCounterVec, BoxedHistogramVec, BoxedRegistry, Buckets};

pub struct Metrics {
    pub compaction_commit_counter: BoxedCounterVec,
    pub compaction_duration: BoxedHistogramVec,

    pub compaction_rewritten_bytes: BoxedCounterVec,
    pub compaction_rewritten_files_count: BoxedCounterVec,
    pub compaction_added_files_count: BoxedCounterVec,
    pub compaction_failed_data_files_count: BoxedCounterVec,

    pub compaction_commit_duration: BoxedHistogramVec,

    pub compaction_commit_failed_counter: BoxedCounterVec,
    pub compaction_executor_error_counter: BoxedCounterVec,
}

impl Metrics {
    pub fn new(registry: BoxedRegistry) -> Self {
        let compaction_commit_counter = registry.register_counter_vec(
            "compaction_commit_counter".into(),
            "BergLoom compaction total commit counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_duration = registry.register_histogram_vec_with_buckets(
            "compaction_duration".into(),
            "BergLoom compaction duration in seconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                1.0, 2.0, 20, // Start at 1 second, double each bucket, up to 20 buckets
            ),
        );

        let compaction_rewritten_bytes = registry.register_counter_vec(
            "compaction_rewritten_bytes".into(),
            "BergLoom compaction rewritten bytes".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_rewritten_files_count = registry.register_counter_vec(
            "compaction_rewritten_files_count".into(),
            "BergLoom compaction rewritten files count".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_added_files_count = registry.register_counter_vec(
            "compaction_added_files_count".into(),
            "BergLoom compaction added files count".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_failed_data_files_count = registry.register_counter_vec(
            "compaction_failed_data_files_count".into(),
            "BergLoom compaction failed data files count".into(),
            &["catalog_name", "table_ident"],
        );

        // 10ms 100ms 1s 10s 100s
        let compaction_commit_duration = registry.register_histogram_vec_with_buckets(
            "compaction_commit_duration".into(),
            "BergLoom compaction commit duration in milliseconds".into(),
            &["catalog_name", "table_ident"],
            Buckets::exponential(
                10.0, 10.0, 5, // Start at 10ms, multiply each bucket by 10, up to 5 buckets
            ),
        );

        let compaction_commit_failed_counter = registry.register_counter_vec(
            "compaction_commit_failed_counter".into(),
            "BergLoom compaction commit failed counts".into(),
            &["catalog_name", "table_ident"],
        );

        let compaction_executor_error_counter = registry.register_counter_vec(
            "compaction_executor_error_counter".into(),
            "BergLoom compaction executor error counts".into(),
            &["catalog_name", "table_ident"],
        );

        Self {
            compaction_commit_counter,
            compaction_duration,
            compaction_rewritten_bytes,
            compaction_rewritten_files_count,
            compaction_added_files_count,
            compaction_failed_data_files_count,
            compaction_commit_duration,
            compaction_commit_failed_counter,
            compaction_executor_error_counter,
        }
    }
}
