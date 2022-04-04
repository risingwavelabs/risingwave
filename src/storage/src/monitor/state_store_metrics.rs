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
pub const BATCH_WRITE_BUILD_TABLE_LATENCY_SCALE: f64 = 1.0;
pub const BATCH_WRITE_ADD_L0_LATENCT_SCALE: f64 = 0.00001;

pub const RANGE_SCAN_SIZE_SCALE: f64 = 10000.0;
pub const RANGE_SCAN_LATENCY_SCALE: f64 = 0.1;

pub const ITER_MERGE_SST_COUNTS: f64 = 200.0;
pub const ITER_SEEK_LATENCY_SCALE: f64 = 0.0001;
pub const ITER_NEXT_SIZE_SCALE: f64 = 400.0;

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
            range_reverse_scan_size: Histogram,
            range_reverse_scan_duration: Histogram,

            write_batch_tuple_counts: GenericCounter<AtomicU64>,
            write_batch_duration: Histogram,
            write_batch_size: Histogram,
            write_build_l0_sst_duration: Histogram,

            iter_merge_sstable_counts: Histogram,
            iter_merge_seek_duration: Histogram,

            sst_store_block_request_counts: GenericCounter<AtomicU64>,
            sst_store_get_remote_duration: Histogram,
            sst_store_put_remote_duration: Histogram,

            shared_buffer_to_l0_duration: Histogram,
            shared_buffer_to_sstable_size: Histogram,

            compaction_upload_sst_counts: GenericCounter<AtomicU64>,
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
    }

}
for_all_metrics! { define_state_store_metrics }

impl StateStoreMetrics {
    pub fn new(registry: Registry) -> Self {
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

        let get_duration_opts = histogram_opts!(
            "state_store_get_duration",
            "Total latency of get that have been issued to state store",
            buckets
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
            "Total number of sst tables that have been considered true negative by bloom filters.",
            registry
        )
        .unwrap();

        let bloom_filter_might_positive_counts = register_int_counter_with_registry!(
            "state_store_bloom_filter_might_positive_counts",
            "Total number of sst tables that have been considered possibly positive by bloom filters.",
            registry
        )
        .unwrap();

        // ----- range_scan -----
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
            "state_store_range_scan_duration",
            "Total time of scan that have been issued to state store",
            buckets
        );
        let range_scan_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * RANGE_SCAN_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_range_reverse_scan_size",
            "Total bytes scanned reversely from HummockStorage",
            buckets
        );
        let range_reverse_scan_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * RANGE_SCAN_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_range_reverse_scan_duration",
            "Total time of reverse scan that have been issued to state store",
            buckets
        );
        let range_reverse_scan_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        // ----- write_batch -----
        let write_batch_tuple_counts = register_int_counter_with_registry!(
            "state_store_write_batch_tuple_counts",
            "Total number of batched write kv pairs requests that have been issued to state store",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_write_batch_duration",
            "Total time of batched write that have been issued to state store. With shared buffer on, this is the latency writing to the shared buffer.",
            buckets
        );
        let write_batch_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * BATCH_WRITE_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_write_batch_size",
            "Total size of batched write that have been issued to state store",
            buckets
        );
        let write_batch_size = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_BUILD_TABLE_LATENCY_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_write_build_l0_sst_duration",
            "Total time of batch_write_build_table that have been issued to state store",
            buckets
        );
        let write_build_l0_sst_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS
            .map(|x| x * BATCH_WRITE_ADD_L0_LATENCT_SCALE)
            .to_vec();
        let opts = histogram_opts!(
            "state_store_shared_buffer_to_l0_duration",
            "Histogram of time spent from compacting shared buffer to remote storage.",
            buckets
        );
        let shared_buffer_to_l0_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.to_vec();
        let opts = histogram_opts!(
            "state_store_shared_buffer_to_sstable_size",
            "Histogram of batch size compacted from shared buffer to remote storage.",
            buckets
        );
        let shared_buffer_to_sstable_size =
            register_histogram_with_registry!(opts, registry).unwrap();

        // ----- iter -----
        let buckets = DEFAULT_BUCKETS.map(|x| x * ITER_MERGE_SST_COUNTS).to_vec();
        let opts = histogram_opts!(
            "state_store_iter_merge_sstable_counts",
            "Number of child iterators merged into one MergeIterator.",
            buckets
        );
        let iter_merge_sstable_counts = register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * ITER_NEXT_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_iter_merge_seek_duration",
            "Seek() time conducted by MergeIterators.",
            buckets
        );
        let iter_merge_seek_duration = register_histogram_with_registry!(opts, registry).unwrap();

        // ----- sst store -----
        let sst_store_block_request_counts = register_int_counter_with_registry!(
            "state_store_sst_store_block_request_counts",
            "Total number of sst block requests that have been issued to sst store",
            registry
        )
        .unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * ITER_NEXT_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_sst_store_get_remote_duration",
            "Time spent fetching blocks from remote object store.",
            buckets
        );
        let sst_store_get_remote_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let buckets = DEFAULT_BUCKETS.map(|x| x * ITER_NEXT_SIZE_SCALE).to_vec();
        let opts = histogram_opts!(
            "state_store_sst_store_put_remote_duration",
            "Time spent putting blocks to remote object store.",
            buckets
        );
        let sst_store_put_remote_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        // --
        let compaction_upload_sst_counts = register_int_counter_with_registry!(
            "state_store_compaction_upload_sst_counts",
            "Total number of sst uploads during compaction",
            registry
        )
        .unwrap();

        Self {
            get_duration,
            get_key_size,
            get_value_size,
            get_shared_buffer_hit_counts,
            bloom_filter_true_negative_counts,
            bloom_filter_might_positive_counts,

            range_scan_size,
            range_scan_duration,
            range_reverse_scan_size,
            range_reverse_scan_duration,

            write_batch_tuple_counts,
            write_batch_duration,
            write_batch_size,
            write_build_l0_sst_duration,

            iter_merge_sstable_counts,
            iter_merge_seek_duration,

            sst_store_block_request_counts,
            sst_store_get_remote_duration,
            sst_store_put_remote_duration,

            shared_buffer_to_l0_duration,
            shared_buffer_to_sstable_size,
            compaction_upload_sst_counts,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
