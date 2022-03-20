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
//
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    histogram_opts, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, Registry,
};

pub const DEFAULT_BUCKETS: &[f64; 11] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub const PIN_VERSION_LATENCY_SCALE: f64 = 0.1;
pub const UNPIN_VERSION_LATENCY_SCALE: f64 = 0.1;
pub const PIN_SNAPSHOT_LATENCY_SCALE: f64 = 0.1;
pub const UNPIN_SNAPSHOT_LATENCY_SCALE: f64 = 0.1;
pub const ADD_TABLE_LATENCT_SCALE: f64 = 0.1;
pub const GET_NEW_TABLE_ID_LATENCY_SCALE: f64 = 0.1;
pub const GET_COMPATION_TASK_LATENCY_SCALE: f64 = 0.1;
pub const REPORT_COMPATION_TASK_LATENCY_SCALE: f64 = 0.1;

/// Define all metrics.
#[macro_export]
macro_rules! for_all_hummock_metrics {
    ($macro:tt) => {
        $macro! {
            pin_version_counts: GenericCounter<AtomicU64>,
            unpin_version_counts: GenericCounter<AtomicU64>,
            pin_snapshot_counts: GenericCounter<AtomicU64>,
            unpin_snapshot_counts: GenericCounter<AtomicU64>,
            add_tables_counts: GenericCounter<AtomicU64>,
            get_new_table_id_counts: GenericCounter<AtomicU64>,
            report_compaction_task_counts: GenericCounter<AtomicU64>,

            pin_version_latency: Histogram,
            unpin_version_latency: Histogram,
            pin_snapshot_latency: Histogram,
            unpin_snapshot_latency: Histogram,
            add_tables_latency: Histogram,
            get_new_table_id_latency: Histogram,
            report_compaction_task_latency: Histogram,
        }
    };
}

macro_rules! define_hummock_metrics {
    ($( $name:ident: $type:ty ),* ,) => {
        /// [`StateStoreMetrics`] stores the performance and IO metrics of `XXXStore` such as
        /// `RocksDBStateStore` and `TikvStateStore`.
        /// In practice, keep in mind that this represents the whole Hummock utilizations of
        /// a `RisingWave` instance. More granular utilizations of per `materialization view`
        /// job or a executor should be collected by views like `StateStats` and `JobStats`.
        #[derive(Debug)]
        pub struct HummockMetrics {
            $( pub $name: $type, )*
        }
    }

}
for_all_hummock_metrics! { define_hummock_metrics }

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
        let add_tables_counts = register_int_counter_with_registry!(
            "state_store_add_tables_counts",
            "Total number of add_tables_counts requests that have been issued to state store",
            registry
        )
        .unwrap();
        let get_new_table_id_counts = register_int_counter_with_registry!(
            "state_store_get_new_table_id_counts",
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
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * PIN_VERSION_LATENCY_SCALE)
            .to_vec();
        let pin_version_latency_opts = histogram_opts!(
            "state_store_pin_version_latency",
            "Total latency of pin version that have been issued to state store",
            buckets
        );
        let pin_version_latency =
            register_histogram_with_registry!(pin_version_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * UNPIN_VERSION_LATENCY_SCALE)
            .to_vec();
        let unpin_version_latency_opts = histogram_opts!(
            "state_store_unpin_version_latency",
            "Total latency of unpin version that have been issued to state store",
            buckets
        );
        let unpin_version_latency =
            register_histogram_with_registry!(unpin_version_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * PIN_SNAPSHOT_LATENCY_SCALE)
            .to_vec();
        let pin_snapshot_latency_opts = histogram_opts!(
            "state_store_pin_snapshot_latency",
            "Total latency of pin snapshot that have been issued to state store",
            buckets
        );
        let pin_snapshot_latency =
            register_histogram_with_registry!(pin_snapshot_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * UNPIN_SNAPSHOT_LATENCY_SCALE)
            .to_vec();
        let unpin_snapshot_latency_opts = histogram_opts!(
            "state_store_unpin_snapshot_latency",
            "Total latency of unpin snapshot that have been issued to state store",
            buckets
        );
        let unpin_snapshot_latency =
            register_histogram_with_registry!(unpin_snapshot_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * ADD_TABLE_LATENCT_SCALE)
            .to_vec();
        let add_tables_latency_opts = histogram_opts!(
            "state_store_add_tables_latency",
            "Total latency of add tables that have been issued to state store",
            buckets
        );
        let add_tables_latency =
            register_histogram_with_registry!(add_tables_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * GET_NEW_TABLE_ID_LATENCY_SCALE)
            .to_vec();
        let get_new_table_id_latency_opts = histogram_opts!(
            "state_store_get_new_table_id_latency",
            "Total latency of get new table id that have been issued to state store",
            buckets
        );
        let get_new_table_id_latency =
            register_histogram_with_registry!(get_new_table_id_latency_opts, registry).unwrap();

        // --
        let buckets = DEFAULT_BUCKETS
            .map(|x| x * REPORT_COMPATION_TASK_LATENCY_SCALE)
            .to_vec();
        let report_compaction_task_latency_opts = histogram_opts!(
            "state_store_report_compaction_task_latency",
            "Total latency of report compaction task that have been issued to state store",
            buckets
        );
        let report_compaction_task_latency =
            register_histogram_with_registry!(report_compaction_task_latency_opts, registry)
                .unwrap();
        Self {
            pin_version_counts,
            unpin_version_counts,
            pin_snapshot_counts,
            unpin_snapshot_counts,
            add_tables_counts,
            get_new_table_id_counts,
            report_compaction_task_counts,

            pin_version_latency,
            unpin_version_latency,
            pin_snapshot_latency,
            unpin_snapshot_latency,
            add_tables_latency,
            get_new_table_id_latency,
            report_compaction_task_latency,
        }
    }

    /// Create a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
