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
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_int_counter_with_registry, HistogramVec, Registry,
};
use risingwave_common::monitor::Print;

macro_rules! for_all_metrics {
    ($macro:ident) => {
        $macro! {
            write_bytes: GenericCounter<AtomicU64>,
            read_bytes: GenericCounter<AtomicU64>,
            operation_latency: HistogramVec,
            operation_size: HistogramVec,
        }
    };
}

macro_rules! define_object_store_metrics {
    ($( $name:ident: $type:ty ),* ,) => {
        /// [`ObjectStoreMetrics`] stores the performance and IO metrics of `ObjectStore` such as
        /// `S3` and `MinIO`.
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

for_all_metrics! { define_object_store_metrics }

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
            exponential_buckets(0.0001, 2.0, 21).unwrap(), // max 104s
        );
        let operation_latency =
            register_histogram_vec_with_registry!(latency_opts, &["media_type", "type"], registry)
                .unwrap();
        let mut buckets = vec![];
        for i in 0..4 {
            buckets.push((4096 << (i * 2)) as f64);
        }
        for i in 0..4 {
            buckets.push((4096 << (i + 10)) as f64);
        }
        let mut step = *buckets.last().unwrap(); // 32MB
        for _ in 0..4 {
            let base = *buckets.last().unwrap() + step;
            for i in 0..4 {
                buckets.push(base + step * i as f64);
            }
            step *= 2.0;
        }
        let bytes_opts = histogram_opts!(
            "object_store_operation_bytes",
            "size of operation result on object store",
            buckets, // max 1952MB
        );
        let operation_size =
            register_histogram_vec_with_registry!(bytes_opts, &["type"], registry).unwrap();

        Self {
            write_bytes,
            read_bytes,
            operation_latency,
            operation_size,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
