use std::sync::Arc;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    histogram_opts, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, Registry,
};

pub const DEFAULT_BUCKETS: &[f64; 11] = &[
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

pub const GET_LATENCY_SCALE: f64 = 0.0001;
pub const GET_SNAPSHOT_LATENCY_SCALE: f64 = 0.0001;
pub const WRITE_BATCH_LATENCY_SCALE: f64 = 0.0001;
pub const BUILD_TABLE_LATENCY_SCALE: f64 = 0.0001;
pub const ADD_L0_LATENCT_SCALE: f64 = 0.00001;
pub const ITER_NEXT_LATENCY_SCALE: f64 = 0.0001;
pub const ITER_SEEK_LATENCY_SCALE: f64 = 0.0001;

/// `StateStoreStats` stores the performance and IO metrics of `XXXStorage` such as
/// In practice, keep in mind that this represents the whole Hummock utilizations of
/// a `RisingWave` instance. More granular utilizations of per `materialization view`
/// job or a executor should be collected by views like `StateStats` and `JobStats`.
pub struct StateStoreStats {
    /// Overall utilizations.
    pub get_bytes: GenericCounter<AtomicU64>,
    pub get_latency: Histogram,
    pub get_key_size: Histogram,
    pub get_value_size: Histogram,
    pub get_counts: GenericCounter<AtomicU64>,
    pub get_snapshot_latency: Histogram,

    pub put_bytes: GenericCounter<AtomicU64>,
    pub range_scan_counts: GenericCounter<AtomicU64>,

    pub batched_write_counts: GenericCounter<AtomicU64>,
    pub batch_write_tuple_counts: GenericCounter<AtomicU64>,
    pub batch_write_latency: Histogram,
    pub batch_write_size: Histogram,
    pub batch_write_build_table_latency: Histogram,
    pub batch_write_add_l0_latency: Histogram,

    pub iter_counts: GenericCounter<AtomicU64>,
    pub iter_next_counts: GenericCounter<AtomicU64>,
    pub iter_seek_latency: Histogram,
    pub iter_next_latency: Histogram,
}

lazy_static::lazy_static! {
  pub static ref
  DEFAULT_STATE_STORE_STATS: Arc<StateStoreStats> = Arc::new(StateStoreStats::new(prometheus::default_registry()));
}

impl StateStoreStats {
    pub fn new(registry: &Registry) -> Self {
        // get
        let get_bytes = register_int_counter_with_registry!(
            "state_store_get_bytes",
            "Total number of bytes that have been requested from remote storage",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store"
        );
        let get_key_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_get_value_size",
            "Total value bytes that have been requested from remote storage",
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
            "get_snapshot_latency",
            "Total latency of get snapshot that have been issued to state store",
            buckets
        );
        let get_snapshot_latency =
            register_histogram_with_registry!(get_snapshot_latency_opts, registry).unwrap();

        // put
        let put_bytes = register_int_counter_with_registry!(
            "state_store_put_bytes",
            "Total number of bytes that have been transmitted to remote storage",
            registry
        )
        .unwrap();

        let range_scan_counts = register_int_counter_with_registry!(
            "state_store_range_scan_counts",
            "Total number of range scan requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        // write_batch
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
            .map(|x| x * WRITE_BATCH_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_batched_write_latency",
            "Total time of batched write that have been issued to state store",
            buckets
        );
        let batch_write_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_batched_write_size",
            "Total size of batched write that have been issued to state store"
        );
        let batch_write_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BUILD_TABLE_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_batch_write_build_table_latency",
            "Total time of batch_write_build_table that have been issued to state store",
            buckets
        );
        let batch_write_build_table_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * ADD_L0_LATENCT_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_batch_write_add_l0_ssts_latency",
            "Total time of add_l0_ssts that have been issued to state store",
            buckets
        );
        let batch_write_add_l0_latency = register_histogram_with_registry!(opts, registry).unwrap();

        // iter
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

        Self {
            get_bytes,
            get_latency,
            get_key_size,
            get_value_size,
            get_counts,
            get_snapshot_latency,
            put_bytes,
            range_scan_counts,
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
        }
    }
}

// TODO(xiangyhu): use macro to process the stats
// macro_rules! process_stats {
//   (struct $name:ident { $($fname:ident : $ftype:ty),* }) => {
//       struct $name {
//           $($fname : $ftype),*
//       }

//       impl $name {
//           fn field_names() -> &'static [&'static str] {
//               static NAMES: &'static [&'static str] = &[$(stringify!($fname)),*];
//               NAMES
//           }

//           pub fn new() -> Self {
//             $fname: $ftype::new(0);
//           }
//       }
//   }
// }
