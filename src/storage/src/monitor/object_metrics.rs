use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_with_registry, Histogram, HistogramVec,
    Registry,
};

use super::Print;

macro_rules! for_all_metrics {
    ($macro:ident) => {
        $macro! {
            write_bytes: GenericCounter<AtomicU64>,
            read_bytes: GenericCounter<AtomicU64>,
            operation_latency: HistogramVec,
        }
    };
}

macro_rules! define_state_store_metrics {
    ($( $name:ident: $type:ty ),* ,) => {
        /// [`StateStoreMetrics`] stores the performance and IO metrics of `XXXStore` such as
        /// `RocksDBStateStore` and `TikvStateStore`.
        /// In practice, keep in mind that this represents the whole Hummock utilization of
        /// a `RisingWave` instance. More granular utilization of per `materialization view`
        /// job or an executor should be collected by views like `StateStats` and `JobStats`.
        #[derive(Debug)]
        pub struct ObjectStoreMetrics {
            $( pub $name: $type, )*
        }

        impl Print for ObjectStoreMetrics {
            fn print(&self) {
                $( self.$name.print(); )*
            }
        }
    }

}

for_all_metrics! { define_state_store_metrics }

impl ObjectStoreMetrics {
    pub fn new(registry: Registry) -> Self {
        let read_bytes = register_int_counter_with_registry!(
            "object_store_read_bytes",
            "Total bytes of requests read from object store",
            registry
        )
        .unwrap();
        let write_bytes = register_int_counter_with_registry!(
            "object_store_write_bytes",
            "Total bytes of requests read from object store",
            registry
        )
        .unwrap();

        let latency_opts = histogram_opts!(
            "object_store_operation_latency",
            "Total latency of operation on object store",
            exponential_buckets(0.0001, 2.0, 20).unwrap(), // max 52s
        );
        let operation_latency =
            register_histogram_vec_with_registry!(latency_opts, &["type"], registry).unwrap();

        Self {
            write_bytes,
            read_bytes,
            operation_latency,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
