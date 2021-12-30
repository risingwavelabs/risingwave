use std::sync::Arc;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{register_int_counter_with_registry, Registry};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::Int64Type;

/// `HummockStats` stores the performance and IO metrics of `HummockStorage`.
/// In practice, keep in mind that this represents the whole Hummock utilizations of
/// a `RisingWave` instance. More granular utilizations of per `materialization view`
/// job or a executor should be collected by views like `StateStats` and `JobStats`.
pub struct HummockStats {
    /// Overall utilizations.
    pub get_bytes: GenericCounter<AtomicU64>,
    pub put_bytes: GenericCounter<AtomicU64>,
    pub point_get_counts: GenericCounter<AtomicU64>,
    pub range_scan_counts: GenericCounter<AtomicU64>,
    pub batched_write_counts: GenericCounter<AtomicU64>,
}

lazy_static::lazy_static! {
  pub static ref DEFAULT_HUMMOCK_STATS: Arc<HummockStats> = Arc::new(HummockStats::new(prometheus::default_registry()));
}

impl HummockStats {
    pub fn new(registry: &Registry) -> Self {
        let get_bytes = register_int_counter_with_registry!(
            "hummock_get_bytes",
            "Total number of bytes that have been requested from remote storage",
            registry
        )
        .unwrap();

        let put_bytes = register_int_counter_with_registry!(
            "hummock_put_bytes",
            "Total number of bytes that have been transmitted to remote storage",
            registry
        )
        .unwrap();

        let point_get_counts = register_int_counter_with_registry!(
            "hummock_point_get_counts",
            "Total number of get requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        let range_scan_counts = register_int_counter_with_registry!(
            "hummock_range_scan_counts",
            "Total number of range scan requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        let batched_write_counts = register_int_counter_with_registry!(
            "hummock_batched_write_counts",
            "Total number of batched write requests that have been issued to Hummock Storage",
            registry
        )
        .unwrap();

        Self {
            get_bytes,
            put_bytes,
            point_get_counts,
            range_scan_counts,
            batched_write_counts,
        }
    }

    pub fn to_schema() -> Schema {
        let schema = Schema::new(vec![
            Field::new(Int64Type::create(false)),
            Field::new(Int64Type::create(false)),
            Field::new(Int64Type::create(false)),
            Field::new(Int64Type::create(false)),
            Field::new(Int64Type::create(false)),
        ]);

        schema
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
