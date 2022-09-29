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

use std::sync::Arc;

use prometheus::core::{AtomicU64, Collector, Desc, GenericCounter, GenericCounterVec};
use prometheus::{
    exponential_buckets, histogram_opts, proto, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, HistogramVec,
    IntGauge, Opts, Registry,
};
use risingwave_common::monitor::Print;

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
            bloom_filter_check_counts: GenericCounter<AtomicU64>,

            range_scan_size: Histogram,
            range_scan_duration: Histogram,
            range_backward_scan_size: Histogram,
            range_backward_scan_duration: Histogram,

            iter_size: Histogram,
            iter_item: Histogram,
            iter_duration: Histogram,
            iter_scan_duration: Histogram,
            iter_in_process_counts: GenericCounter<AtomicU64>,
            iter_scan_key_counts: GenericCounterVec<AtomicU64>,

            write_batch_tuple_counts: GenericCounter<AtomicU64>,
            write_batch_duration: Histogram,
            write_batch_size: Histogram,
            write_build_l0_sst_duration: Histogram,
            write_build_l0_bytes: GenericCounter<AtomicU64>,
            write_l0_size_per_epoch: Histogram,

            iter_merge_sstable_counts: HistogramVec,

            sst_store_block_request_counts: GenericCounterVec<AtomicU64>,

            shared_buffer_to_l0_duration: Histogram,
            shared_buffer_to_sstable_size: Histogram,

            compaction_upload_sst_counts: GenericCounter<AtomicU64>,
            compact_preload_count: GenericCounter<AtomicU64>,
            compact_read_current_level: GenericCounterVec<AtomicU64>,
            compact_read_next_level: GenericCounterVec<AtomicU64>,
            compact_read_sstn_current_level: GenericCounterVec<AtomicU64>,
            compact_read_sstn_next_level: GenericCounterVec<AtomicU64>,
            compact_write_bytes: GenericCounterVec<AtomicU64>,
            compact_write_sstn: GenericCounterVec<AtomicU64>,
            compact_sst_duration: Histogram,
            compact_task_duration: HistogramVec,
            compact_task_pending_num: IntGauge,
            get_table_id_total_time_duration: Histogram,
            remote_read_time: Histogram,

            sstable_bloom_filter_size: Histogram,
            sstable_file_size: Histogram,

            sstable_avg_key_size: Histogram,
            sstable_avg_value_size: Histogram,
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

        let bloom_filter_check_counts = register_int_counter_with_registry!(
            "state_bloom_filter_check_counts",
            "Total number of read request to check bloom filters",
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

        let opts = histogram_opts!(
            "state_store_iter_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let iter_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_item",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            exponential_buckets(1.0, 2.0, 20).unwrap() // max 2^20 items
        );
        let iter_item = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_duration",
            "Histogram of iterator scan and initialization time that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let iter_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_scan_duration",
            "Histogram of iterator scan time that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let iter_scan_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let iter_in_process_counts = register_int_counter_with_registry!(
            "state_store_iter_in_process_counts",
            "Total number of iter_in_process that have been issued to state store",
            registry
        )
        .unwrap();

        let iter_scan_key_counts = register_int_counter_vec_with_registry!(
            "state_store_iter_scan_key_counts",
            "Total number of keys read by iterator",
            &["type"],
            registry
        )
        .unwrap();

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
            "state_store_write_l0_size_per_epoch",
            "Total size of upload to l0 every epoch",
            exponential_buckets(10.0, 2.0, 25).unwrap()
        );
        let write_l0_size_per_epoch = register_histogram_with_registry!(opts, registry).unwrap();

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
        let iter_merge_sstable_counts =
            register_histogram_vec_with_registry!(opts, &["type"], registry).unwrap();

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

        let opts = histogram_opts!(
            "state_store_compact_sst_duration",
            "Total time of compact_key_range that have been issued to state store",
            exponential_buckets(0.001, 1.6, 28).unwrap() // max 520s
        );
        let compact_sst_duration = register_histogram_with_registry!(opts, registry).unwrap();
        let opts = histogram_opts!(
            "state_store_compact_task_duration",
            "Total time of compact that have been issued to state store",
            exponential_buckets(0.1, 1.6, 28).unwrap() // max 52000s
        );
        let compact_task_duration =
            register_histogram_vec_with_registry!(opts, &["level"], registry).unwrap();
        let opts = histogram_opts!(
            "state_store_get_table_id_total_time_duration",
            "Total time of compact that have been issued to state store",
            exponential_buckets(0.1, 1.6, 28).unwrap() // max 52000s
        );
        let get_table_id_total_time_duration =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_remote_read_time_per_task",
            "Total time of operations which read from remote storage when enable prefetch",
            exponential_buckets(0.001, 1.6, 28).unwrap() // max 520s
        );
        let remote_read_time = register_histogram_with_registry!(opts, registry).unwrap();
        let compact_read_current_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_curr",
            "KBs read from current level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_next_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_next",
            "KBs read from next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_write_bytes = register_int_counter_vec_with_registry!(
            "storage_level_compact_write",
            "KBs written into next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_sstn_current_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_curr",
            "num of SSTs read from current level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_read_sstn_next_level = register_int_counter_vec_with_registry!(
            "storage_level_compact_read_sstn_next",
            "num of SSTs read from next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_write_sstn = register_int_counter_vec_with_registry!(
            "storage_level_compact_write_sstn",
            "num of SSTs written into next level during history compactions to next level",
            &["group", "level_index"],
            registry
        )
        .unwrap();

        let compact_task_pending_num = register_int_gauge_with_registry!(
            "storage_compact_task_pending_num",
            "the num of storage compact parallelism",
            registry
        )
        .unwrap();
        let compact_preload_count = register_int_counter_with_registry!(
            "state_store_preload_counts",
            "Total number of operation to preload",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "state_store_sstable_bloom_filter_size",
            "Total bytes gotten from sstable_bloom_filter, for observing bloom_filter size",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );

        let sstable_bloom_filter_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sstable_file_size",
            "Total bytes gotten from sstable_file_size, for observing sstable_file_size",
            exponential_buckets(1.0, 2.0, 31).unwrap() // max 1G
        );

        let sstable_file_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sstable_avg_key_size",
            "Total bytes gotten from sstable_avg_key_size, for observing sstable_avg_key_size",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );

        let sstable_avg_key_size = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sstable_avg_value_size",
            "Total bytes gotten from sstable_avg_value_size, for observing sstable_avg_value_size",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );

        let sstable_avg_value_size = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            get_duration,
            get_key_size,
            get_value_size,
            get_shared_buffer_hit_counts,

            bloom_filter_true_negative_counts,
            bloom_filter_check_counts,

            range_scan_size,
            range_scan_duration,
            range_backward_scan_size,
            range_backward_scan_duration,
            iter_size,
            iter_item,
            iter_duration,
            iter_scan_duration,
            iter_in_process_counts,
            iter_scan_key_counts,
            write_batch_tuple_counts,
            write_batch_duration,
            write_batch_size,
            write_build_l0_sst_duration,
            write_build_l0_bytes,
            write_l0_size_per_epoch,
            iter_merge_sstable_counts,
            sst_store_block_request_counts,
            shared_buffer_to_l0_duration,
            shared_buffer_to_sstable_size,

            compaction_upload_sst_counts,
            compact_write_bytes,
            compact_read_current_level,
            compact_read_next_level,
            compact_read_sstn_current_level,
            compact_read_sstn_next_level,
            compact_write_sstn,
            compact_sst_duration,
            compact_task_duration,
            compact_task_pending_num,
            compact_preload_count,

            get_table_id_total_time_duration,
            remote_read_time,

            sstable_bloom_filter_size,
            sstable_file_size,

            sstable_avg_key_size,
            sstable_avg_value_size,
        }
    }

    /// Creates a new `StateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}

pub trait MemoryCollector: Sync + Send {
    fn get_meta_memory_usage(&self) -> u64;
    fn get_data_memory_usage(&self) -> u64;
    fn get_total_memory_usage(&self) -> u64;
}

struct StateStoreCollector {
    memory_collector: Arc<dyn MemoryCollector>,
    descs: Vec<Desc>,
    block_cache_size: IntGauge,
    meta_cache_size: IntGauge,
    limit_memory_size: IntGauge,
}

impl StateStoreCollector {
    pub fn new(memory_collector: Arc<dyn MemoryCollector>) -> Self {
        let mut descs = Vec::new();

        let block_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_block_cache_size",
            "the size of cache for data block cache",
        ))
        .unwrap();
        descs.extend(block_cache_size.desc().into_iter().cloned());

        let meta_cache_size = IntGauge::with_opts(Opts::new(
            "state_store_meta_cache_size",
            "the size of cache for meta file cache",
        ))
        .unwrap();
        descs.extend(meta_cache_size.desc().into_iter().cloned());
        let limit_memory_size = IntGauge::with_opts(Opts::new(
            "state_store_limit_memory_size",
            "the size of cache for meta file cache",
        ))
        .unwrap();
        descs.extend(limit_memory_size.desc().into_iter().cloned());

        Self {
            memory_collector,
            descs,
            block_cache_size,
            meta_cache_size,
            limit_memory_size,
        }
    }
}

impl Collector for StateStoreCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.block_cache_size
            .set(self.memory_collector.get_data_memory_usage() as i64);
        self.meta_cache_size
            .set(self.memory_collector.get_meta_memory_usage() as i64);
        self.limit_memory_size
            .set(self.memory_collector.get_total_memory_usage() as i64);

        // collect MetricFamilies.
        let mut mfs = Vec::with_capacity(3);
        mfs.extend(self.block_cache_size.collect());
        mfs.extend(self.meta_cache_size.collect());
        mfs.extend(self.limit_memory_size.collect());
        mfs
    }
}

use std::io::{Error, ErrorKind, Result};

pub fn monitor_cache(
    memory_collector: Arc<dyn MemoryCollector>,
    registry: &Registry,
) -> Result<()> {
    let collector = StateStoreCollector::new(memory_collector);
    registry
        .register(Box::new(collector))
        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
}
