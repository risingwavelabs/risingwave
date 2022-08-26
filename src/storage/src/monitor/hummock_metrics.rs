// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry, Histogram, Registry,
};

/// [`HummockMetrics`] stores the performance and IO metrics of hummock storage.
#[derive(Debug)]
pub struct HummockMetrics {
    pub pin_version_counts: GenericCounter<AtomicU64>,
    pub unpin_version_before_counts: GenericCounter<AtomicU64>,
    pub unpin_version_counts: GenericCounter<AtomicU64>,
    pub pin_snapshot_counts: GenericCounter<AtomicU64>,
    pub unpin_snapshot_counts: GenericCounter<AtomicU64>,
    pub get_new_sst_ids_counts: GenericCounter<AtomicU64>,
    pub report_compaction_task_counts: GenericCounter<AtomicU64>,

    pub pin_version_latency: Histogram,
    pub unpin_version_before_latency: Histogram,
    pub unpin_version_latency: Histogram,
    pub pin_snapshot_latency: Histogram,
    pub unpin_snapshot_latency: Histogram,
    pub get_new_sst_ids_latency: Histogram,
    pub report_compaction_task_latency: Histogram,
}

impl HummockMetrics {
    pub fn new(registry: Registry) -> Self {
        // ----- Hummock -----
        // gRPC count
        let pin_version_counts = register_int_counter_with_registry!(
            "state_store_pin_version_counts",
            "Total number of pin_version_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
        let unpin_version_before_counts = register_int_counter_with_registry!(
            "state_store_unpin_version_before_counts",
            "Total number of unpin_version_before_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
        let unpin_version_counts = register_int_counter_with_registry!(
            "state_store_unpin_version_counts",
            "Total number of unpin_version_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
        let pin_snapshot_counts = register_int_counter_with_registry!(
            "state_store_pin_snapshot_counts",
            "Total number of pin_snapshot_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
        let unpin_snapshot_counts = register_int_counter_with_registry!(
            "state_store_unpin_snapshot_counts",
            "Total number of unpin_snapshot_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
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

        // gRPC latency
        // --
        let pin_version_latency_opts = histogram_opts!(
            "state_store_pin_version_latency",
            "Total latency of pin version that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let pin_version_latency =
            register_histogram_with_registry!(pin_version_latency_opts, registry).unwrap();

        // --
        let unpin_version_latency_opts = histogram_opts!(
            "state_store_unpin_version_latency",
            "Total latency of unpin version that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let unpin_version_latency =
            register_histogram_with_registry!(unpin_version_latency_opts, registry).unwrap();

        // --
        let unpin_version_before_latency_opts = histogram_opts!(
            "state_store_unpin_version_before_latency",
            "Total latency of unpin version before that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let unpin_version_before_latency =
            register_histogram_with_registry!(unpin_version_before_latency_opts, registry).unwrap();

        // --
        let pin_snapshot_latency_opts = histogram_opts!(
            "state_store_pin_snapshot_latency",
            "Total latency of pin snapshot that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let pin_snapshot_latency =
            register_histogram_with_registry!(pin_snapshot_latency_opts, registry).unwrap();

        // --
        let unpin_snapshot_latency_opts = histogram_opts!(
            "state_store_unpin_snapshot_latency",
            "Total latency of unpin snapshot that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let unpin_snapshot_latency =
            register_histogram_with_registry!(unpin_snapshot_latency_opts, registry).unwrap();

        // --
        let get_new_sst_ids_latency_opts = histogram_opts!(
            "state_store_get_new_sst_ids_latency",
            "Total latency of get new table id that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let get_new_sst_ids_latency =
            register_histogram_with_registry!(get_new_sst_ids_latency_opts, registry).unwrap();

        // --
        let report_compaction_task_latency_opts = histogram_opts!(
            "state_store_report_compaction_task_latency",
            "Total latency of report compaction task that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let report_compaction_task_latency =
            register_histogram_with_registry!(report_compaction_task_latency_opts, registry)
                .unwrap();

        Self {
            pin_version_counts,
            unpin_version_before_counts,
            unpin_version_counts,
            pin_snapshot_counts,
            unpin_snapshot_counts,
            get_new_sst_ids_counts,
            report_compaction_task_counts,

            pin_version_latency,
            unpin_version_before_latency,
            unpin_version_latency,
            pin_snapshot_latency,
            unpin_snapshot_latency,
            get_new_sst_ids_latency,
            report_compaction_task_latency,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
