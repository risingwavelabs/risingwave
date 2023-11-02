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

use risingwave_common::config::{extract_storage_memory_config, RwConfig, StorageMemoryConfig};
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::system_param::system_params_for_test;

#[derive(Clone, Debug)]
pub struct StorageOpts {
    /// The size of parallel task for one compact/flush job.
    pub parallel_compact_size_mb: u32,
    /// Target size of the Sstable.
    pub sstable_size_mb: u32,
    /// Size of each block in bytes in SST.
    pub block_size_kb: u32,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    pub share_buffers_sync_parallelism: u32,
    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    pub share_buffer_compaction_worker_threads_number: u32,
    /// Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
    /// is enough space.
    pub shared_buffer_capacity_mb: usize,
    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    pub shared_buffer_flush_ratio: f32,
    /// The threshold for the number of immutable memtables to merge to a new imm.
    pub imm_merge_threshold: usize,
    /// Remote directory for storing data and metadata objects.
    pub data_directory: String,
    /// Whether to enable write conflict detection
    pub write_conflict_detection_enabled: bool,
    /// Capacity of sstable block cache.
    pub block_cache_capacity_mb: usize,
    /// Capacity of sstable meta cache.
    pub meta_cache_capacity_mb: usize,
    /// Percent of the ratio of high priority data in block-cache
    pub high_priority_ratio: usize,
    pub disable_remote_compactor: bool,
    /// Number of tasks shared buffer can upload in parallel.
    pub share_buffer_upload_concurrency: usize,
    /// Capacity of sstable meta cache.
    pub compactor_memory_limit_mb: usize,
    /// compactor streaming iterator recreate timeout.
    pub compact_iter_recreate_timeout_ms: u64,
    /// Number of SST ids fetched from meta per RPC
    pub sstable_id_remote_fetch_number: u32,
    /// Whether to enable streaming upload for sstable.
    pub min_sst_size_for_streaming_upload: u64,
    /// Max sub compaction task numbers
    pub max_sub_compaction: u32,
    pub max_concurrent_compaction_task_number: u64,

    pub data_file_cache_dir: String,
    pub data_file_cache_capacity_mb: usize,
    pub data_file_cache_file_capacity_mb: usize,
    pub data_file_cache_device_align: usize,
    pub data_file_cache_device_io_size: usize,
    pub data_file_cache_flushers: usize,
    pub data_file_cache_reclaimers: usize,
    pub data_file_cache_recover_concurrency: usize,
    pub data_file_cache_lfu_window_to_cache_size_ratio: usize,
    pub data_file_cache_lfu_tiny_lru_capacity_ratio: f64,
    pub data_file_cache_insert_rate_limit_mb: usize,
    pub data_file_cache_reclaim_rate_limit_mb: usize,
    pub data_file_cache_ring_buffer_capacity_mb: usize,
    pub data_file_cache_catalog_bits: usize,
    pub data_file_cache_compression: String,

    pub cache_refill_data_refill_levels: Vec<u32>,
    pub cache_refill_timeout_ms: u64,
    pub cache_refill_concurrency: usize,
    pub cache_refill_recent_filter_layers: usize,
    pub cache_refill_recent_filter_rotate_interval_ms: usize,
    pub cache_refill_unit: usize,
    pub cache_refill_threshold: f64,

    pub meta_file_cache_dir: String,
    pub meta_file_cache_capacity_mb: usize,
    pub meta_file_cache_file_capacity_mb: usize,
    pub meta_file_cache_device_align: usize,
    pub meta_file_cache_device_io_size: usize,
    pub meta_file_cache_flushers: usize,
    pub meta_file_cache_reclaimers: usize,
    pub meta_file_cache_recover_concurrency: usize,
    pub meta_file_cache_lfu_window_to_cache_size_ratio: usize,
    pub meta_file_cache_lfu_tiny_lru_capacity_ratio: f64,
    pub meta_file_cache_insert_rate_limit_mb: usize,
    pub meta_file_cache_reclaim_rate_limit_mb: usize,
    pub meta_file_cache_ring_buffer_capacity_mb: usize,
    pub meta_file_cache_catalog_bits: usize,
    pub meta_file_cache_compression: String,

    /// The storage url for storing backups.
    pub backup_storage_url: String,
    /// The storage directory for storing backups.
    pub backup_storage_directory: String,
    /// max time which wait for preload. 0 represent do not do any preload.
    pub max_preload_wait_time_mill: u64,
    /// object store streaming read timeout.
    pub object_store_streaming_read_timeout_ms: u64,
    /// object store streaming upload timeout.
    pub object_store_streaming_upload_timeout_ms: u64,
    /// object store upload timeout.
    pub object_store_upload_timeout_ms: u64,
    /// object store read timeout.
    pub object_store_read_timeout_ms: u64,

    pub object_store_recv_buffer_size: Option<usize>,
    pub compactor_max_sst_key_count: u64,
    pub compactor_max_task_multiplier: f32,
    pub compactor_max_sst_size: u64,
    /// enable FastCompactorRunner.
    pub enable_fast_compaction: bool,
}

impl Default for StorageOpts {
    fn default() -> Self {
        let c = RwConfig::default();
        let p = system_params_for_test();
        let s = extract_storage_memory_config(&c);
        Self::from((&c, &p.into(), &s))
    }
}

impl From<(&RwConfig, &SystemParamsReader, &StorageMemoryConfig)> for StorageOpts {
    fn from((c, p, s): (&RwConfig, &SystemParamsReader, &StorageMemoryConfig)) -> Self {
        Self {
            parallel_compact_size_mb: p.parallel_compact_size_mb(),
            sstable_size_mb: p.sstable_size_mb(),
            block_size_kb: p.block_size_kb(),
            bloom_false_positive: p.bloom_false_positive(),
            share_buffers_sync_parallelism: c.storage.share_buffers_sync_parallelism,
            share_buffer_compaction_worker_threads_number: c
                .storage
                .share_buffer_compaction_worker_threads_number,
            shared_buffer_capacity_mb: s.shared_buffer_capacity_mb,
            shared_buffer_flush_ratio: c.storage.shared_buffer_flush_ratio,
            imm_merge_threshold: c.storage.imm_merge_threshold,
            data_directory: p.data_directory().to_string(),
            write_conflict_detection_enabled: c.storage.write_conflict_detection_enabled,
            high_priority_ratio: s.high_priority_ratio_in_percent,
            block_cache_capacity_mb: s.block_cache_capacity_mb,
            meta_cache_capacity_mb: s.meta_cache_capacity_mb,
            disable_remote_compactor: c.storage.disable_remote_compactor,
            share_buffer_upload_concurrency: c.storage.share_buffer_upload_concurrency,
            compactor_memory_limit_mb: s.compactor_memory_limit_mb,
            sstable_id_remote_fetch_number: c.storage.sstable_id_remote_fetch_number,
            min_sst_size_for_streaming_upload: c.storage.min_sst_size_for_streaming_upload,
            max_sub_compaction: c.storage.max_sub_compaction,
            max_concurrent_compaction_task_number: c.storage.max_concurrent_compaction_task_number,
            data_file_cache_dir: c.storage.data_file_cache.dir.clone(),
            data_file_cache_capacity_mb: c.storage.data_file_cache.capacity_mb,
            data_file_cache_file_capacity_mb: c.storage.data_file_cache.file_capacity_mb,
            data_file_cache_device_align: c.storage.data_file_cache.device_align,
            data_file_cache_device_io_size: c.storage.data_file_cache.device_io_size,
            data_file_cache_flushers: c.storage.data_file_cache.flushers,
            data_file_cache_reclaimers: c.storage.data_file_cache.reclaimers,
            data_file_cache_recover_concurrency: c.storage.data_file_cache.recover_concurrency,
            data_file_cache_lfu_window_to_cache_size_ratio: c
                .storage
                .data_file_cache
                .lfu_window_to_cache_size_ratio,
            data_file_cache_lfu_tiny_lru_capacity_ratio: c
                .storage
                .data_file_cache
                .lfu_tiny_lru_capacity_ratio,
            data_file_cache_insert_rate_limit_mb: c.storage.data_file_cache.insert_rate_limit_mb,
            data_file_cache_reclaim_rate_limit_mb: c.storage.data_file_cache.reclaim_rate_limit_mb,
            data_file_cache_ring_buffer_capacity_mb: c
                .storage
                .data_file_cache
                .ring_buffer_capacity_mb,
            data_file_cache_catalog_bits: c.storage.data_file_cache.catalog_bits,
            data_file_cache_compression: c.storage.data_file_cache.compression.clone(),
            meta_file_cache_dir: c.storage.meta_file_cache.dir.clone(),
            meta_file_cache_capacity_mb: c.storage.meta_file_cache.capacity_mb,
            meta_file_cache_file_capacity_mb: c.storage.meta_file_cache.file_capacity_mb,
            meta_file_cache_device_align: c.storage.meta_file_cache.device_align,
            meta_file_cache_device_io_size: c.storage.meta_file_cache.device_io_size,
            meta_file_cache_flushers: c.storage.meta_file_cache.flushers,
            meta_file_cache_reclaimers: c.storage.meta_file_cache.reclaimers,
            meta_file_cache_recover_concurrency: c.storage.meta_file_cache.recover_concurrency,
            meta_file_cache_lfu_window_to_cache_size_ratio: c
                .storage
                .meta_file_cache
                .lfu_window_to_cache_size_ratio,
            meta_file_cache_lfu_tiny_lru_capacity_ratio: c
                .storage
                .meta_file_cache
                .lfu_tiny_lru_capacity_ratio,
            meta_file_cache_insert_rate_limit_mb: c.storage.meta_file_cache.insert_rate_limit_mb,
            meta_file_cache_reclaim_rate_limit_mb: c.storage.meta_file_cache.reclaim_rate_limit_mb,
            meta_file_cache_ring_buffer_capacity_mb: c
                .storage
                .meta_file_cache
                .ring_buffer_capacity_mb,
            meta_file_cache_catalog_bits: c.storage.meta_file_cache.catalog_bits,
            meta_file_cache_compression: c.storage.meta_file_cache.compression.clone(),
            cache_refill_data_refill_levels: c.storage.cache_refill.data_refill_levels.clone(),
            cache_refill_timeout_ms: c.storage.cache_refill.timeout_ms,
            cache_refill_concurrency: c.storage.cache_refill.concurrency,
            cache_refill_recent_filter_layers: c.storage.cache_refill.recent_filter_layers,
            cache_refill_recent_filter_rotate_interval_ms: c
                .storage
                .cache_refill
                .recent_filter_rotate_interval_ms,
            cache_refill_unit: c.storage.cache_refill.unit,
            cache_refill_threshold: c.storage.cache_refill.threshold,
            max_preload_wait_time_mill: c.storage.max_preload_wait_time_mill,
            object_store_streaming_read_timeout_ms: c
                .storage
                .object_store_streaming_read_timeout_ms,
            compact_iter_recreate_timeout_ms: c.storage.compact_iter_recreate_timeout_ms,
            object_store_streaming_upload_timeout_ms: c
                .storage
                .object_store_streaming_upload_timeout_ms,
            object_store_read_timeout_ms: c.storage.object_store_read_timeout_ms,
            object_store_upload_timeout_ms: c.storage.object_store_upload_timeout_ms,
            backup_storage_url: p.backup_storage_url().to_string(),
            backup_storage_directory: p.backup_storage_directory().to_string(),
            object_store_recv_buffer_size: c.storage.object_store_recv_buffer_size,
            compactor_max_sst_key_count: c.storage.compactor_max_sst_key_count,
            compactor_max_task_multiplier: c.storage.compactor_max_task_multiplier,
            compactor_max_sst_size: c.storage.compactor_max_sst_size,
            enable_fast_compaction: c.storage.enable_fast_compaction,
        }
    }
}
