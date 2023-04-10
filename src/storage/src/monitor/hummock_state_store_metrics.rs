// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use prometheus::core::{AtomicU64, Collector, Desc, GenericCounterVec};
use prometheus::{
    exponential_buckets, histogram_opts, proto, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_with_registry, HistogramVec,
    IntGauge, Opts, Registry,
};

/// [`HummockStateStoreMetrics`] stores the performance and IO metrics of `XXXStore` such as
/// `RocksDBStateStore` and `TikvStateStore`.
/// In practice, keep in mind that this represents the whole Hummock utilization of
/// a `RisingWave` instance. More granular utilization of per `materialization view`
/// job or an executor should be collected by views like `StateStats` and `JobStats`.
#[derive(Debug)]
pub struct HummockStateStoreMetrics {
    pub bloom_filter_true_negative_counts: GenericCounterVec<AtomicU64>,
    pub bloom_filter_check_counts: GenericCounterVec<AtomicU64>,
    pub iter_merge_sstable_counts: HistogramVec,
    pub sst_store_block_request_counts: GenericCounterVec<AtomicU64>,
    pub iter_scan_key_counts: GenericCounterVec<AtomicU64>,
    pub get_shared_buffer_hit_counts: GenericCounterVec<AtomicU64>,
    pub remote_read_time: HistogramVec,
    pub iter_fetch_meta_duration: HistogramVec,
    pub iter_fetch_meta_cache_unhits: IntGauge,
    pub iter_slow_fetch_meta_cache_unhits: IntGauge,

    pub read_req_bloom_filter_positive_counts: GenericCounterVec<AtomicU64>,
    pub read_req_positive_but_non_exist_counts: GenericCounterVec<AtomicU64>,
    pub read_req_check_bloom_filter_counts: GenericCounterVec<AtomicU64>,

    pub write_batch_tuple_counts: GenericCounterVec<AtomicU64>,
    pub write_batch_duration: HistogramVec,
    pub write_batch_size: HistogramVec,
}

impl HummockStateStoreMetrics {
    pub fn new(registry: Registry) -> Self {
        let bloom_filter_true_negative_counts = register_int_counter_vec_with_registry!(
            "state_store_bloom_filter_true_negative_counts",
            "Total number of sstables that have been considered true negative by bloom filters",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let bloom_filter_check_counts = register_int_counter_vec_with_registry!(
            "state_bloom_filter_check_counts",
            "Total number of read request to check bloom filters",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        // ----- iter -----
        let opts = histogram_opts!(
            "state_store_iter_merge_sstable_counts",
            "Number of child iterators merged into one MergeIterator",
            exponential_buckets(1.0, 2.0, 17).unwrap() // max 65536 times
        );
        let iter_merge_sstable_counts =
            register_histogram_vec_with_registry!(opts, &["table_id", "type"], registry).unwrap();

        // ----- sst store -----
        let sst_store_block_request_counts = register_int_counter_vec_with_registry!(
            "state_store_sst_store_block_request_counts",
            "Total number of sst block requests that have been issued to sst store",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let iter_scan_key_counts = register_int_counter_vec_with_registry!(
            "state_store_iter_scan_key_counts",
            "Total number of keys read by iterator",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let get_shared_buffer_hit_counts = register_int_counter_vec_with_registry!(
            "state_store_get_shared_buffer_hit_counts",
            "Total number of get requests that have been fulfilled by shared buffer",
            &["table_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_remote_read_time_per_task",
            "Total time of operations which read from remote storage when enable prefetch",
            exponential_buckets(0.001, 1.6, 28).unwrap() // max 520s
        );
        let remote_read_time =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_fetch_meta_duration",
            "Histogram of iterator fetch SST meta time that have been issued to state store",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let iter_fetch_meta_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let iter_fetch_meta_cache_unhits = register_int_gauge_with_registry!(
            "state_store_iter_fetch_meta_cache_unhits",
            "Number of SST meta cache unhit during one iterator meta fetch",
            registry
        )
        .unwrap();

        let iter_slow_fetch_meta_cache_unhits = register_int_gauge_with_registry!(
            "state_store_iter_slow_fetch_meta_cache_unhits",
            "Number of SST meta cache unhit during a iterator meta fetch which is slow (costs >5 seconds)",
            registry
        )
        .unwrap();

        // ----- write_batch -----
        let write_batch_tuple_counts = register_int_counter_vec_with_registry!(
            "state_store_write_batch_tuple_counts",
            "Total number of batched write kv pairs requests that have been issued to state store",
            &["table_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
                "state_store_write_batch_duration",
                "Total time of batched write that have been issued to state store. With shared buffer on, this is the latency writing to the shared buffer",
                exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
            );
        let write_batch_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_write_batch_size",
            "Total size of batched write that have been issued to state store",
            exponential_buckets(10.0, 2.0, 25).unwrap() // max 160MB
        );
        let write_batch_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let read_req_bloom_filter_positive_counts = register_int_counter_vec_with_registry!(
            "state_store_read_req_bloom_filter_positive_counts",
            "Total number of read request with at least one SST bloom filter check returns positive",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let read_req_positive_but_non_exist_counts = register_int_counter_vec_with_registry!(
            "state_store_read_req_positive_but_non_exist_counts",
            "Total number of read request on non-existent key/prefix with at least one SST bloom filter check returns positive",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        let read_req_check_bloom_filter_counts = register_int_counter_vec_with_registry!(
            "state_store_read_req_check_bloom_filter_counts",
            "Total number of read request that checks bloom filter with a prefix hint",
            &["table_id", "type"],
            registry
        )
        .unwrap();

        Self {
            bloom_filter_true_negative_counts,
            bloom_filter_check_counts,
            iter_merge_sstable_counts,
            sst_store_block_request_counts,
            iter_scan_key_counts,
            get_shared_buffer_hit_counts,
            remote_read_time,
            iter_fetch_meta_duration,
            iter_fetch_meta_cache_unhits,
            iter_slow_fetch_meta_cache_unhits,
            read_req_bloom_filter_positive_counts,
            read_req_positive_but_non_exist_counts,
            read_req_check_bloom_filter_counts,
            write_batch_tuple_counts,
            write_batch_duration,
            write_batch_size,
        }
    }

    /// Creates a new `HummockStateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}

pub trait MemoryCollector: Sync + Send {
    fn get_meta_memory_usage(&self) -> u64;
    fn get_data_memory_usage(&self) -> u64;
    fn get_uploading_memory_usage(&self) -> u64;
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
            .set(self.memory_collector.get_uploading_memory_usage() as i64);

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
