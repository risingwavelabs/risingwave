// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::core::{AtomicI64, AtomicU64, GenericCounterVec, GenericGaugeVec};
use prometheus::{
    register_int_counter_vec_with_registry, register_int_gauge_vec_with_registry, Registry,
};

#[derive(Debug)]
pub struct EnumeratorMetrics {
    pub registry: Registry,
    pub high_watermark: GenericGaugeVec<AtomicI64>,
}

impl EnumeratorMetrics {
    pub fn new(registry: Registry) -> Self {
        let high_watermark = register_int_gauge_vec_with_registry!(
            "high_watermark",
            "High watermark for a exec per partition",
            &["source_id", "partition"],
            registry,
        )
        .unwrap();
        EnumeratorMetrics {
            registry,
            high_watermark,
        }
    }

    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}

impl Default for EnumeratorMetrics {
    fn default() -> Self {
        EnumeratorMetrics::new(Registry::new())
    }
}

#[derive(Debug)]
pub struct SourceMetrics {
    pub registry: Registry,
    pub partition_input_count: GenericCounterVec<AtomicU64>,
    pub partition_input_bytes: GenericCounterVec<AtomicU64>,
    /// User error reporting
    pub user_source_error_count: GenericCounterVec<AtomicU64>,
    /// Report latest message id
    pub latest_message_id: GenericGaugeVec<AtomicI64>,
}

impl SourceMetrics {
    pub fn new(registry: Registry) -> Self {
        let partition_input_count = register_int_counter_vec_with_registry!(
            "partition_input_count",
            "Total number of rows that have been input from specific partition",
            &["actor_id", "source_id", "partition"],
            registry
        )
        .unwrap();
        let partition_input_bytes = register_int_counter_vec_with_registry!(
            "partition_input_bytes",
            "Total bytes that have been input from specific partition",
            &["actor_id", "source_id", "partition"],
            registry
        )
        .unwrap();
        let user_source_error_count = register_int_counter_vec_with_registry!(
            "user_source_error_count",
            "Source errors in the system, queryable by tags",
            &[
                "error_type",
                "error_msg",
                "executor_name",
                "fragment_id",
                "table_id"
            ],
            registry,
        )
        .unwrap();
        let latest_message_id = register_int_gauge_vec_with_registry!(
            "latest_message_id",
            "Latest message id for a exec per partition",
            &["source_id", "actor_id", "partition"],
            registry,
        )
        .unwrap();
        SourceMetrics {
            registry,
            partition_input_count,
            partition_input_bytes,
            user_source_error_count,
            latest_message_id,
        }
    }

    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}

impl Default for SourceMetrics {
    fn default() -> Self {
        SourceMetrics::new(Registry::new())
    }
}
