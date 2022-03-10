use std::sync::Arc;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    histogram_opts, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, Registry,
};

pub const DEFAULT_BUCKETS: &[f64; 11] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub const GET_KEY_SIZE_SCALE: f64 = 200.0;
pub const GET_VALUE_SIZE_SCALE: f64 = 200.0;
pub const GET_LATENCY_SCALE: f64 = 0.01;
pub const GET_SNAPSHOT_LATENCY_SCALE: f64 = 0.0001;

pub const BATCH_WRITE_SIZE_SCALE: f64 = 20000.0;
pub const BATCH_WRITE_LATENCY_SCALE: f64 = 0.1;
pub const BATCH_WRITE_BUILD_TABLE_LATENCY_SCALE: f64 = 0.0001;
pub const BATCH_WRITE_ADD_L0_LATENCT_SCALE: f64 = 0.00001;

pub const RANGE_SCAN_SIZE_SCALE: f64 = 10000.0;
pub const RANGE_SCAN_LATENCY_SCALE: f64 = 0.1;

pub const ITER_NEXT_LATENCY_SCALE: f64 = 0.0001;
pub const ITER_SEEK_LATENCY_SCALE: f64 = 0.0001;
pub const ITER_NEXT_SIZE_SCALE: f64 = 400.0;

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
macro_rules! for_all_metrics {
    ($macro:tt) => {
        $macro! {
            get_latency: Histogram,
            get_key_size: Histogram,
            get_value_size: Histogram,
            get_counts: GenericCounter<AtomicU64>,
            get_snapshot_latency: Histogram,

            range_scan_counts: GenericCounter<AtomicU64>,
            reverse_range_scan_counts: GenericCounter<AtomicU64>,
            range_scan_size: Histogram,
            range_scan_latency: Histogram,

            batched_write_counts: GenericCounter<AtomicU64>,
            batch_write_tuple_counts: GenericCounter<AtomicU64>,
            batch_write_latency: Histogram,
            batch_write_size: Histogram,
            batch_write_build_table_latency: Histogram,
            batch_write_add_l0_latency: Histogram,

            iter_counts: GenericCounter<AtomicU64>,
            iter_next_counts: GenericCounter<AtomicU64>,
            iter_seek_latency: Histogram,
            iter_next_latency: Histogram,
            iter_next_size: Histogram,

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

macro_rules! define_state_store_stats {
    ($( $name:ident: $type:ty ),* ,) => {
        /// [`StateStoreStats`] stores the performance and IO metrics of `XXXStore` such as
        /// `RocksDBStateStore` and `TikvStateStore`.
        /// In practice, keep in mind that this represents the whole Hummock utilizations of
        /// a `RisingWave` instance. More granular utilizations of per `materialization view`
        /// job or a executor should be collected by views like `StateStats` and `JobStats`.
        #[derive(Debug)]
        pub struct StateStoreStats {
            $( pub $name: $type, )*
        }
    }

}
for_all_metrics! { define_state_store_stats }

lazy_static::lazy_static! {
    pub static ref
        DEFAULT_STATE_STORE_STATS: Arc<StateStoreStats> = Arc::new(StateStoreStats::new(prometheus::default_registry()));
}

impl StateStoreStats {
    pub fn new(registry: &Registry) -> Self {
        // ----- get -----
        let buckets = DEFAULT_BUCKETS.map(|x| x * GET_KEY_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store",
            buckets
        );
        let get_key_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * GET_VALUE_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_get_value_size",
            "Total value bytes that have been requested from remote storage",
            buckets
        );
        let get_value_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * GET_LATENCY_SCALE).to_vec();
        // let get_latency_buckets = vec![1.0];
        let get_latency_opts = histogram_opts!(
            "state_store_get_latency",
            "Total latency of get that have been issued to state store",
            buckets
        );
        let get_latency = register_histogram_with_registry!(get_latency_opts, registry).unwrap();

        let get_counts = register_int_counter_with_registry!(
            "state_store_get_counts",
            "Total number of get requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * GET_SNAPSHOT_LATENCY_SCALE)
            .to_vec();
        let get_snapshot_latency_opts = histogram_opts!(
            "state_store_get_snapshot_latency",
            "Total latency of get snapshot that have been issued to state store",
            buckets
        );
        let get_snapshot_latency =
            register_histogram_with_registry!(get_snapshot_latency_opts, registry).unwrap();

        // ----- range_scan -----
        let reverse_range_scan_counts = register_int_counter_with_registry!(
            "state_store_reverse_range_scan_counts",
            "Total number of reverse range scan requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        let range_scan_counts = register_int_counter_with_registry!(
            "state_store_range_scan_counts",
            "Total number of range scan requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * RANGE_SCAN_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_range_scan_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            buckets
        );
        let range_scan_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * RANGE_SCAN_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_range_scan_latency",
            "Total time of scan that have been issued to state store",
            buckets
        );
        let range_scan_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // ----- write_batch -----
        let batched_write_counts = register_int_counter_with_registry!(
            "state_store_batched_write_counts",
            "Total number of batched write requests that have been issued to state store",
            registry
        )
        .unwrap();

        let batch_write_tuple_counts = register_int_counter_with_registry!(
            "state_store_batched_write_tuple_counts",
            "Total number of batched write kv pairs requests that have been issued to state store",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_batched_write_latency",
            "Total time of batched write that have been issued to state store",
            buckets
        );
        let batch_write_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * BATCH_WRITE_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_batched_write_size",
            "Total size of batched write that have been issued to state store",
            buckets
        );
        let batch_write_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_BUILD_TABLE_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_batch_write_build_table_latency",
            "Total time of batch_write_build_table that have been issued to state store",
            buckets
        );
        let batch_write_build_table_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_ADD_L0_LATENCT_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_batch_write_add_l0_ssts_latency",
            "Total time of add_l0_ssts that have been issued to state store",
            buckets
        );
        let batch_write_add_l0_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // ----- iter -----
        let iter_counts = register_int_counter_with_registry!(
            "state_store_iter_counts",
            "Total number of iter requests that have been issued to state store",
            registry
        )
        .unwrap();

        let iter_next_counts = register_int_counter_with_registry!(
            "state_store_iter_next_counts",
            "Total number of iter.next requests that have been issued to state store",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * ITER_SEEK_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_iter_seek_latency",
            "total latency on seeking the start of key range",
            buckets
        );
        let iter_seek_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * ITER_NEXT_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_iter_next_latency",
            "total latency on a next calls",
            buckets
        );
        let iter_next_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * ITER_NEXT_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_iter_next_size",
            "Total bytes gotten from state store iterator next(), for calculating read throughput",
            buckets
        );
        let iter_next_size = register_histogram_with_registry!(opts, registry).unwrap();

        // ----- gRPC -----
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
            get_latency,
            get_key_size,
            get_value_size,
            get_counts,
            get_snapshot_latency,

            range_scan_counts,
            reverse_range_scan_counts,
            range_scan_size,
            range_scan_latency,

            batched_write_counts,
            batch_write_tuple_counts,
            batch_write_latency,
            batch_write_size,
            batch_write_build_table_latency,
            batch_write_add_l0_latency,

            iter_counts,
            iter_next_counts,
            iter_seek_latency,
            iter_next_latency,
            iter_next_size,

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
        Self::new(&Registry::new())
    }
}
