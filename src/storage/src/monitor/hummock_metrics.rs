// Copyright 2025 RisingWave Labs
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

use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    Histogram, Registry, exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

/// [`HummockMetrics`] stores the performance and IO metrics of hummock storage.
#[derive(Debug, Clone)]
pub struct HummockMetrics {
    pub get_new_sst_ids_counts: GenericCounter<AtomicU64>,
    pub report_compaction_task_counts: GenericCounter<AtomicU64>,
    pub get_new_sst_ids_latency: Histogram,
    pub report_compaction_task_latency: Histogram,
}

pub static GLOBAL_HUMMOCK_METRICS: LazyLock<HummockMetrics> =
    LazyLock::new(|| HummockMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl HummockMetrics {
    fn new(registry: &Registry) -> Self {
        // ----- Hummock -----
        let get_new_sst_ids_counts = register_int_counter_with_registry!(
            "state_store_get_new_sst_ids_counts",
            "Total number of get_new_table_id requests that have been issued to state store",
            registry
        )
        .unwrap();
        let report_compaction_task_counts = register_int_counter_with_registry!(
            "state_store_report_compaction_task_counts",
            "Total number of report_compaction_task requests that have been issued to state store",
            registry
        )
        .unwrap();

        // 10ms ~ max 2.7h
        let time_buckets = exponential_buckets(0.01, 10.0, 7).unwrap();
        // gRPC latency

        // --
        let get_new_sst_ids_latency_opts = histogram_opts!(
            "state_store_get_new_sst_ids_latency",
            "Total latency of get new table id that have been issued to state store",
            time_buckets.clone()
        );
        let get_new_sst_ids_latency =
            register_histogram_with_registry!(get_new_sst_ids_latency_opts, registry).unwrap();

        // --
        let report_compaction_task_latency_opts = histogram_opts!(
            "state_store_report_compaction_task_latency",
            "Total latency of report compaction task that have been issued to state store",
            time_buckets
        );
        let report_compaction_task_latency =
            register_histogram_with_registry!(report_compaction_task_latency_opts, registry)
                .unwrap();

        Self {
            get_new_sst_ids_counts,
            report_compaction_task_counts,
            get_new_sst_ids_latency,
            report_compaction_task_latency,
        }
    }

    /// Creates a new `HummockStateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        GLOBAL_HUMMOCK_METRICS.clone()
    }
}
