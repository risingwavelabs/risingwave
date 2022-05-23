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

use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry, Histogram,
    Registry,
};

use super::{monitor_process, Print};

/// Define all metrics.
#[macro_export]
macro_rules! for_all_metrics {
    ($macro:ident) => {
        $macro! {
            get_duration: Histogram,
            get_key_size: Histogram,
            get_value_size: Histogram,
            get_shared_buffer_hit_counts: GenericCounter<AtomicU64>,

            bloom_filter_true_negative_counts: GenericCounter<AtomicU64>,
            bloom_filter_might_positive_counts: GenericCounter<AtomicU64>,

            range_scan_size: Histogram,
            range_scan_duration: Histogram,
            range_backward_scan_size: Histogram,
            range_backward_scan_duration: Histogram,

            write_batch_tuple_counts: GenericCounter<AtomicU64>,
            write_batch_duration: Histogram,
            write_batch_size: Histogram,
            write_build_l0_sst_duration: Histogram,
            write_build_l0_bytes: GenericCounter<AtomicU64>,

            iter_merge_sstable_counts: Histogram,
            iter_merge_seek_duration: Histogram,

            sst_store_block_request_counts: GenericCounterVec<AtomicU64>,

            shared_buffer_to_l0_duration: Histogram,
            shared_buffer_to_sstable_size: Histogram,

            compaction_upload_sst_counts: GenericCounter<AtomicU64>,
            compaction_read_bytes: GenericCounter<AtomicU64>,
            compaction_write_bytes: GenericCounter<AtomicU64>,
            compact_sst_duration: Histogram,
            compact_task_duration: Histogram,
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
        pub struct StateStoreMetrics {
            $( pub $name: $type, )*
        }

        impl Print for StateStoreMetrics {
           fn print(&self) {
                $( self.$name.print(); )*
           }
        }
    }

}

for_all_metrics! { define_state_store_metrics }

impl StateStoreMetrics {
    pub fn new(registry: Registry) -> Self {
        // ----- get -----
        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let get_key_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_get_value_size",
            "Total value bytes that have been requested from remote storage",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let get_value_size = register_histogram_with_registry!(opts, registry).unwrap();

        let get_duration_opts = histogram_opts!(
            "state_store_get_duration",
            "Total latency of get that have been issued to state store",
            exponential_buckets(0.00001, 2.0, 21).unwrap() // max 10s
        );
        let get_duration = register_histogram_with_registry!(get_duration_opts, registry).unwrap();

        let get_shared_buffer_hit_counts = register_int_counter_with_registry!(
            "state_store_get_shared_buffer_hit_counts",
            "Total number of get requests that have been fulfilled by shared buffer",
            registry
        )
        .unwrap();

        let bloom_filter_true_negative_counts = register_int_counter_with_registry!(
            "state_store_bloom_filter_true_negative_counts",
            "Total number of sstables that have been considered true negative by bloom filters",
            registry
        )
        .unwrap();

        let bloom_filter_might_positive_counts = register_int_counter_with_registry!(
            "state_store_bloom_filter_might_positive_counts",
            "Total number of sst tables that have been considered possibly positive by bloom filters",
            registry
        )
        .unwrap();

        // ----- range_scan -----
        let opts = histogram_opts!(
            "state_store_range_scan_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let range_scan_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_range_scan_duration",
            "Total time of scan that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let range_scan_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_range_backward_scan_size",
            "Total bytes scanned backwards from HummockStorage",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let range_backward_scan_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_range_backward_scan_duration",
            "Total time of backward scan that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let range_backward_scan_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        // ----- write_batch -----
        let write_batch_tuple_counts = register_int_counter_with_registry!(
            "state_store_write_batch_tuple_counts",
            "Total number of batched write kv pairs requests that have been issued to state store",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_write_batch_duration",
            "Total time of batched write that have been issued to state store. With shared buffer on, this is the latency writing to the shared buffer",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let write_batch_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_write_batch_size",
            "Total size of batched write that have been issued to state store",
            exponential_buckets(10.0, 2.0, 25).unwrap() // max 160MB
        );
        let write_batch_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_write_build_l0_sst_duration",
            "Total time of batch_write_build_table that have been issued to state store",
            exponential_buckets(0.001, 2.0, 16).unwrap() // max 32s
        );
        let write_build_l0_sst_duration =
            register_histogram_with_registry!(opts, registry).unwrap();
        let write_build_l0_bytes = register_int_counter_with_registry!(
            "state_store_write_build_l0_bytes",
            "Total size of compaction files size that have been written to object store from shared buffer",
            registry
        ).unwrap();
        let opts = histogram_opts!(
            "state_store_shared_buffer_to_l0_duration",
            "Histogram of time spent from compacting shared buffer to remote storage",
            exponential_buckets(0.01, 2.0, 16).unwrap() // max 327s
        );
        let shared_buffer_to_l0_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_shared_buffer_to_sstable_size",
            "Histogram of batch size compacted from shared buffer to remote storage",
            exponential_buckets(10.0, 2.0, 25).unwrap() // max 160MB
        );
        let shared_buffer_to_sstable_size =
            register_histogram_with_registry!(opts, registry).unwrap();

        // ----- iter -----
        let opts = histogram_opts!(
            "state_store_iter_merge_sstable_counts",
            "Number of child iterators merged into one MergeIterator",
            exponential_buckets(1.0, 2.0, 17).unwrap() // max 65536 times
        );
        let iter_merge_sstable_counts = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_merge_seek_duration",
            "Seek() time conducted by MergeIterators",
            exponential_buckets(1.0, 2.0, 17).unwrap() // max 65536 times
        );
        let iter_merge_seek_duration = register_histogram_with_registry!(opts, registry).unwrap();

        // ----- sst store -----
        let sst_store_block_request_counts = register_int_counter_vec_with_registry!(
            "state_store_sst_store_block_request_counts",
            "Total number of sst block requests that have been issued to sst store",
            &["type"],
            registry
        )
        .unwrap();

        // --
        let compaction_upload_sst_counts = register_int_counter_with_registry!(
            "state_store_compaction_upload_sst_counts",
            "Total number of sst uploads during compaction",
            registry
        )
        .unwrap();
        let compaction_read_bytes = register_int_counter_with_registry!(
            "state_store_compaction_read_bytes",
            "Total size of file size that read from object store in compaction job",
            registry
        )
        .unwrap();
        let compaction_write_bytes = register_int_counter_with_registry!(
            "state_store_compaction_write_bytes",
            "Total size of compaction files size that have been written to object store",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_compact_sst_duration",
            "Total time of compact_key_range that have been issued to state store",
            exponential_buckets(0.001, 2.0, 16).unwrap() // max 32s
        );
        let compact_sst_duration = register_histogram_with_registry!(opts, registry).unwrap();
        let opts = histogram_opts!(
            "state_store_compact_task_duration",
            "Total time of compact that have been issued to state store",
            exponential_buckets(0.001, 2.0, 16).unwrap() // max 32s
        );
        let compact_task_duration = register_histogram_with_registry!(opts, registry).unwrap();

        monitor_process(&registry).unwrap();
        Self {
            get_duration,
            get_key_size,
            get_value_size,
            get_shared_buffer_hit_counts,

            bloom_filter_true_negative_counts,
            bloom_filter_might_positive_counts,

            range_scan_size,
            range_scan_duration,
            range_backward_scan_size,
            range_backward_scan_duration,
            write_batch_tuple_counts,
            write_batch_duration,
            write_batch_size,
            write_build_l0_sst_duration,
            write_build_l0_bytes,
            iter_merge_sstable_counts,
            iter_merge_seek_duration,
            sst_store_block_request_counts,
            shared_buffer_to_l0_duration,
            shared_buffer_to_sstable_size,

            compaction_upload_sst_counts,
            compaction_read_bytes,
            compaction_write_bytes,
            compact_sst_duration,
            compact_task_duration,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
