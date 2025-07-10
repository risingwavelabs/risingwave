// Copyright 2025 RisingWave Labs
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

use foyer::{Compression, RecoverMode, RuntimeOptions, Throttle};
use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;

use super::object_store::ObjectStoreConfig;
use super::types::{EvictionConfig, Unrecognized, MAX_BLOCK_CACHE_SHARD_BITS, MAX_META_CACHE_SHARD_BITS, MIN_BUFFER_SIZE_PER_SHARD};
use crate::config::RwConfig;

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StorageConfig {
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    #[serde(default = "default::storage::share_buffers_sync_parallelism")]
    pub share_buffers_sync_parallelism: u32,

    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    #[serde(default = "default::storage::share_buffer_compaction_worker_threads_number")]
    pub share_buffer_compaction_worker_threads_number: u32,

    /// Configure the maximum shared buffer size in MB explicitly. Writes attempting to exceed the capacity
    /// will stall until there is enough space. The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `meta_cache_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub shared_buffer_capacity_mb: Option<usize>,

    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    #[serde(default = "default::storage::shared_buffer_flush_ratio")]
    pub shared_buffer_flush_ratio: f32,

    /// The minimum total flush size of shared buffer spill. When a shared buffer spilled is trigger,
    /// the total flush size across multiple epochs should be at least higher than this size.
    #[serde(default = "default::storage::shared_buffer_min_batch_flush_size_mb")]
    pub shared_buffer_min_batch_flush_size_mb: usize,

    /// The threshold for the number of immutable memtables to merge to a new imm.
    #[serde(default = "default::storage::imm_merge_threshold")]
    #[deprecated]
    pub imm_merge_threshold: usize,

    /// Whether to enable write conflict detection
    #[serde(default = "default::storage::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache: CacheConfig,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_capacity_mb` instead.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.meta_cache_capacity_mb` instead.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_eviction.high_priority_ratio_in_percent` with `storage.cache.block_cache_eviction.algorithm = "Lru"` instead.
    #[serde(default)]
    pub high_priority_ratio_in_percent: Option<usize>,

    /// max memory usage for large query
    #[serde(default)]
    pub prefetch_buffer_capacity_mb: Option<usize>,

    #[serde(default = "default::storage::max_cached_recent_versions_number")]
    pub max_cached_recent_versions_number: usize,

    /// max prefetch block number
    #[serde(default = "default::storage::max_prefetch_block_number")]
    pub max_prefetch_block_number: usize,

    #[serde(default = "default::storage::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::storage::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    #[serde(default)]
    pub compactor_memory_limit_mb: Option<usize>,

    /// Compactor calculates the maximum number of tasks that can be executed on the node based on
    /// `worker_num` and `compactor_max_task_multiplier`.
    /// `max_pull_task_count` = `worker_num` * `compactor_max_task_multiplier`
    #[serde(default = "default::storage::compactor_max_task_multiplier")]
    pub compactor_max_task_multiplier: f32,

    /// The percentage of memory available when compactor is deployed separately.
    /// `non_reserved_memory_bytes` = `system_memory_available_bytes` * `compactor_memory_available_proportion`
    #[serde(default = "default::storage::compactor_memory_available_proportion")]
    pub compactor_memory_available_proportion: f64,

    /// Number of SST ids fetched from meta per RPC
    #[serde(default = "default::storage::sstable_id_remote_fetch_number")]
    pub sstable_id_remote_fetch_number: u32,

    #[serde(default = "default::storage::min_sstable_size_mb")]
    pub min_sstable_size_mb: u32,

    #[serde(default)]
    #[config_doc(nested)]
    pub data_file_cache: FileCacheConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta_file_cache: FileCacheConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache_refill: CacheRefillConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::storage::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    #[serde(default = "default::storage::max_concurrent_compaction_task_number")]
    pub max_concurrent_compaction_task_number: u64,

    #[serde(default = "default::storage::max_preload_wait_time_mill")]
    pub max_preload_wait_time_mill: u64,

    #[serde(default = "default::storage::max_version_pinning_duration_sec")]
    pub max_version_pinning_duration_sec: u64,

    #[serde(default = "default::storage::compactor_max_sst_key_count")]
    pub compactor_max_sst_key_count: u64,
    // DEPRECATED: This config will be deprecated in the future version, use `storage.compactor_iter_max_io_retry_times` instead.
    #[serde(default = "default::storage::compact_iter_recreate_timeout_ms")]
    pub compact_iter_recreate_timeout_ms: u64,
    #[serde(default = "default::storage::compactor_max_sst_size")]
    pub compactor_max_sst_size: u64,
    #[serde(default = "default::storage::enable_fast_compaction")]
    pub enable_fast_compaction: bool,
    #[serde(default = "default::storage::check_compaction_result")]
    pub check_compaction_result: bool,
    #[serde(default = "default::storage::max_preload_io_retry_times")]
    pub max_preload_io_retry_times: usize,
    #[serde(default = "default::storage::compactor_fast_max_compact_delete_ratio")]
    pub compactor_fast_max_compact_delete_ratio: u32,
    #[serde(default = "default::storage::compactor_fast_max_compact_task_size")]
    pub compactor_fast_max_compact_task_size: u64,
    #[serde(default = "default::storage::compactor_iter_max_io_retry_times")]
    pub compactor_iter_max_io_retry_times: usize,

    /// Deprecated: The window size of table info statistic history.
    #[serde(default = "default::storage::table_info_statistic_history_times")]
    #[deprecated]
    pub table_info_statistic_history_times: usize,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// The spill threshold for mem table.
    #[serde(default = "default::storage::mem_table_spill_threshold")]
    pub mem_table_spill_threshold: usize,

    /// The concurrent uploading number of `SSTables` of builder
    #[serde(default = "default::storage::compactor_concurrent_uploading_sst_count")]
    pub compactor_concurrent_uploading_sst_count: Option<usize>,

    #[serde(default = "default::storage::compactor_max_overlap_sst_count")]
    pub compactor_max_overlap_sst_count: usize,

    /// The maximum number of meta files that can be preloaded.
    /// If the number of meta files exceeds this value, the compactor will try to compute parallelism only through `SstableInfo`, no longer preloading `SstableMeta`.
    /// This is to prevent the compactor from consuming too much memory, but it may cause the compactor to be less efficient.
    #[serde(default = "default::storage::compactor_max_preload_meta_file_count")]
    pub compactor_max_preload_meta_file_count: usize,

    #[serde(default = "default::storage::vector_file_block_size_kb")]
    pub vector_file_block_size_kb: usize,

    /// Object storage configuration
    /// 1. General configuration
    /// 2. Some special configuration of Backend
    /// 3. Retry and timeout configuration
    #[serde(default)]
    pub object_store: ObjectStoreConfig,

    #[serde(default = "default::storage::time_travel_version_cache_capacity")]
    pub time_travel_version_cache_capacity: u64,

    // iceberg compaction
    #[serde(default = "default::storage::iceberg_compaction_target_file_size_mb")]
    pub iceberg_compaction_target_file_size_mb: u32,
    #[serde(default = "default::storage::iceberg_compaction_enable_validate")]
    pub iceberg_compaction_enable_validate: bool,
    #[serde(default = "default::storage::iceberg_compaction_max_record_batch_rows")]
    pub iceberg_compaction_max_record_batch_rows: usize,
    #[serde(default = "default::storage::iceberg_compaction_min_size_per_partition_mb")]
    pub iceberg_compaction_min_size_per_partition_mb: u32,
    #[serde(default = "default::storage::iceberg_compaction_max_file_count_per_partition")]
    pub iceberg_compaction_max_file_count_per_partition: u32,

    #[serde(default = "default::storage::iceberg_compaction_write_parquet_max_row_group_rows")]
    pub iceberg_compaction_write_parquet_max_row_group_rows: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheConfig {
    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `meta_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the block cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub block_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub block_cache_eviction: CacheEvictionConfig,

    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the meta cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub meta_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub meta_cache_eviction: CacheEvictionConfig,

    #[serde(default = "default::storage::vector_block_cache_capacity_mb")]
    pub vector_block_cache_capacity_mb: usize,
    #[serde(default = "default::storage::vector_block_cache_shard_num")]
    pub vector_block_cache_shard_num: usize,
    #[serde(default)]
    #[config_doc(omitted)]
    pub vector_block_cache_eviction_config: CacheEvictionConfig,
    #[serde(default = "default::storage::vector_meta_cache_capacity_mb")]
    pub vector_meta_cache_capacity_mb: usize,
    #[serde(default = "default::storage::vector_meta_cache_shard_num")]
    pub vector_meta_cache_shard_num: usize,
    #[serde(default)]
    #[config_doc(omitted)]
    pub vector_meta_cache_eviction_config: CacheEvictionConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CacheEvictionConfig {
    Lru {
        high_priority_ratio_in_percent: Option<usize>,
    },
    Lfu {
        window_capacity_ratio_in_percent: Option<usize>,
        protected_capacity_ratio_in_percent: Option<usize>,
        cmsketch_eps: Option<f64>,
        cmsketch_confidence: Option<f64>,
    },
    #[default]
    S3Fifo {
        small_queue_capacity_ratio_in_percent: Option<usize>,
        ghost_queue_capacity_ratio_in_percent: Option<usize>,
        small_to_main_freq_threshold: Option<u8>,
    },
}

impl Default for CacheEvictionConfig {
    fn default() -> Self {
        Self::S3Fifo {
            small_queue_capacity_ratio_in_percent: None,
            ghost_queue_capacity_ratio_in_percent: None,
            small_to_main_freq_threshold: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheRefillConfig {
    /// `SSTable` levels to refill.
    #[serde(default = "default::cache_refill::data_refill_levels")]
    pub data_refill_levels: Vec<u32>,

    /// Cache refill maximum timeout to apply version delta.
    #[serde(default = "default::cache_refill::timeout_ms")]
    pub timeout_ms: u64,

    /// Inflight data cache refill tasks.
    #[serde(default = "default::cache_refill::concurrency")]
    pub concurrency: usize,

    /// Block count that a data cache refill request fetches.
    #[serde(default = "default::cache_refill::unit")]
    pub unit: usize,

    /// Data cache refill unit admission ratio.
    ///
    /// Only unit whose blocks are admitted above the ratio will be refilled.
    #[serde(default = "default::cache_refill::threshold")]
    pub threshold: f64,

    /// Recent filter layer count.
    #[serde(default = "default::cache_refill::recent_filter_layers")]
    pub recent_filter_layers: usize,

    /// Recent filter layer rotate interval.
    #[serde(default = "default::cache_refill::recent_filter_rotate_interval_ms")]
    pub recent_filter_rotate_interval_ms: usize,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache::dir")]
    pub dir: String,

    #[serde(default = "default::file_cache::capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache::file_capacity_mb")]
    pub file_capacity_mb: usize,

    #[serde(default = "default::file_cache::flushers")]
    pub flushers: usize,

    #[serde(default = "default::file_cache::reclaimers")]
    pub reclaimers: usize,

    #[serde(default = "default::file_cache::recover_concurrency")]
    pub recover_concurrency: usize,

    /// Deprecated soon. Please use `throttle` to do I/O throttling instead.
    #[serde(default = "default::file_cache::insert_rate_limit_mb")]
    pub insert_rate_limit_mb: usize,

    #[serde(default = "default::file_cache::indexer_shards")]
    pub indexer_shards: usize,

    #[serde(default = "default::file_cache::compression")]
    pub compression: Compression,

    #[serde(default = "default::file_cache::flush_buffer_threshold_mb")]
    pub flush_buffer_threshold_mb: Option<usize>,

    #[serde(default = "default::file_cache::throttle")]
    pub throttle: Throttle,

    #[serde(default = "default::file_cache::fifo_probation_ratio")]
    pub fifo_probation_ratio: f64,

    /// Recover mode.
    ///
    /// Options:
    ///
    /// - "None": Do not recover disk cache.
    /// - "Quiet": Recover disk cache and skip errors.
    /// - "Strict": Recover disk cache and panic on errors.
    ///
    /// More details, see [`RecoverMode::None`], [`RecoverMode::Quiet`] and [`RecoverMode::Strict`],
    #[serde(default = "default::file_cache::recover_mode")]
    pub recover_mode: RecoverMode,

    #[serde(default = "default::file_cache::runtime_config")]
    pub runtime_config: RuntimeOptions,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

pub struct StorageMemoryConfig {
    pub block_cache_capacity_mb: usize,
    pub block_cache_shard_num: usize,
    pub meta_cache_capacity_mb: usize,
    pub meta_cache_shard_num: usize,
    pub vector_block_cache_capacity_mb: usize,
    pub vector_block_cache_shard_num: usize,
    pub vector_meta_cache_capacity_mb: usize,
    pub vector_meta_cache_shard_num: usize,
    pub shared_buffer_capacity_mb: usize,
    pub compactor_memory_limit_mb: usize,
    pub prefetch_buffer_capacity_mb: usize,
    pub block_cache_eviction_config: EvictionConfig,
    pub meta_cache_eviction_config: EvictionConfig,
    pub vector_block_cache_eviction_config: EvictionConfig,
    pub vector_meta_cache_eviction_config: EvictionConfig,
    pub block_file_cache_flush_buffer_threshold_mb: usize,
    pub meta_file_cache_flush_buffer_threshold_mb: usize,
}

pub fn extract_storage_memory_config(s: &RwConfig) -> StorageMemoryConfig {
    let mut block_cache_capacity_mb = default::storage::block_cache_capacity_mb();
    let mut meta_cache_capacity_mb = default::storage::meta_cache_capacity_mb();
    let mut shared_buffer_capacity_mb = default::storage::shared_buffer_capacity_mb();
    let mut compactor_memory_limit_mb = default::storage::compactor_memory_limit_mb();
    let mut prefetch_buffer_capacity_mb = 0;

    // Extract cache capacity from the deprecated settings first.
    if let Some(capacity_mb) = s.storage.block_cache_capacity_mb {
        block_cache_capacity_mb = capacity_mb;
    }
    if let Some(capacity_mb) = s.storage.meta_cache_capacity_mb {
        meta_cache_capacity_mb = capacity_mb;
    }

    // Override with the new settings if available.
    if let Some(capacity_mb) = s.storage.cache.block_cache_capacity_mb {
        block_cache_capacity_mb = capacity_mb;
    }
    if let Some(capacity_mb) = s.storage.cache.meta_cache_capacity_mb {
        meta_cache_capacity_mb = capacity_mb;
    }

    if let Some(capacity_mb) = s.storage.shared_buffer_capacity_mb {
        shared_buffer_capacity_mb = capacity_mb;
    }

    if let Some(capacity_mb) = s.storage.compactor_memory_limit_mb {
        compactor_memory_limit_mb = capacity_mb;
    }

    if let Some(capacity_mb) = s.storage.prefetch_buffer_capacity_mb {
        prefetch_buffer_capacity_mb = capacity_mb;
    }

    let block_cache_shard_num = s
        .storage
        .cache
        .block_cache_shard_num
        .unwrap_or_else(|| {
            (block_cache_capacity_mb / MIN_BUFFER_SIZE_PER_SHARD)
                .clamp(1, (1 << MAX_BLOCK_CACHE_SHARD_BITS))
        });

    let meta_cache_shard_num = s
        .storage
        .cache
        .meta_cache_shard_num
        .unwrap_or_else(|| {
            (meta_cache_capacity_mb / MIN_BUFFER_SIZE_PER_SHARD)
                .clamp(1, (1 << MAX_META_CACHE_SHARD_BITS))
        });

    let block_cache_eviction_config = match &s.storage.cache.block_cache_eviction {
        CacheEvictionConfig::Lru {
            high_priority_ratio_in_percent,
        } => {
            let ratio = high_priority_ratio_in_percent
                .unwrap_or(default::storage::high_priority_ratio_in_percent());
            foyer::LruConfig {
                high_priority_pool_ratio: ratio as f64 / 100.0,
            }
            .into()
        }
        CacheEvictionConfig::Lfu {
            window_capacity_ratio_in_percent,
            protected_capacity_ratio_in_percent,
            cmsketch_eps,
            cmsketch_confidence,
        } => {
            let window_capacity_ratio = window_capacity_ratio_in_percent
                .unwrap_or(default::storage::window_capacity_ratio_in_percent());
            let protected_capacity_ratio = protected_capacity_ratio_in_percent
                .unwrap_or(default::storage::protected_capacity_ratio_in_percent());
            foyer::LfuConfig {
                window_capacity_ratio: window_capacity_ratio as f64 / 100.0,
                protected_capacity_ratio: protected_capacity_ratio as f64 / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }
            .into()
        }
        CacheEvictionConfig::S3Fifo {
            small_queue_capacity_ratio_in_percent,
            ghost_queue_capacity_ratio_in_percent,
            small_to_main_freq_threshold,
        } => {
            let small_queue_capacity_ratio = small_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent());
            let ghost_queue_capacity_ratio = ghost_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent());
            foyer::S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio as f64 / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio as f64 / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }
            .into()
        }
    };

    let meta_cache_eviction_config = match &s.storage.cache.meta_cache_eviction {
        CacheEvictionConfig::Lru {
            high_priority_ratio_in_percent,
        } => {
            let ratio = high_priority_ratio_in_percent
                .unwrap_or(default::storage::high_priority_ratio_in_percent());
            foyer::LruConfig {
                high_priority_pool_ratio: ratio as f64 / 100.0,
            }
            .into()
        }
        CacheEvictionConfig::Lfu {
            window_capacity_ratio_in_percent,
            protected_capacity_ratio_in_percent,
            cmsketch_eps,
            cmsketch_confidence,
        } => {
            let window_capacity_ratio = window_capacity_ratio_in_percent
                .unwrap_or(default::storage::window_capacity_ratio_in_percent());
            let protected_capacity_ratio = protected_capacity_ratio_in_percent
                .unwrap_or(default::storage::protected_capacity_ratio_in_percent());
            foyer::LfuConfig {
                window_capacity_ratio: window_capacity_ratio as f64 / 100.0,
                protected_capacity_ratio: protected_capacity_ratio as f64 / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }
            .into()
        }
        CacheEvictionConfig::S3Fifo {
            small_queue_capacity_ratio_in_percent,
            ghost_queue_capacity_ratio_in_percent,
            small_to_main_freq_threshold,
        } => {
            let small_queue_capacity_ratio = small_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent());
            let ghost_queue_capacity_ratio = ghost_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent());
            foyer::S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio as f64 / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio as f64 / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }
            .into()
        }
    };

    let vector_block_cache_eviction_config = match &s.storage.cache.vector_block_cache_eviction_config {
        CacheEvictionConfig::Lru {
            high_priority_ratio_in_percent,
        } => {
            let ratio = high_priority_ratio_in_percent
                .unwrap_or(default::storage::high_priority_ratio_in_percent());
            foyer::LruConfig {
                high_priority_pool_ratio: ratio as f64 / 100.0,
            }
            .into()
        }
        CacheEvictionConfig::Lfu {
            window_capacity_ratio_in_percent,
            protected_capacity_ratio_in_percent,
            cmsketch_eps,
            cmsketch_confidence,
        } => {
            let window_capacity_ratio = window_capacity_ratio_in_percent
                .unwrap_or(default::storage::window_capacity_ratio_in_percent());
            let protected_capacity_ratio = protected_capacity_ratio_in_percent
                .unwrap_or(default::storage::protected_capacity_ratio_in_percent());
            foyer::LfuConfig {
                window_capacity_ratio: window_capacity_ratio as f64 / 100.0,
                protected_capacity_ratio: protected_capacity_ratio as f64 / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }
            .into()
        }
        CacheEvictionConfig::S3Fifo {
            small_queue_capacity_ratio_in_percent,
            ghost_queue_capacity_ratio_in_percent,
            small_to_main_freq_threshold,
        } => {
            let small_queue_capacity_ratio = small_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent());
            let ghost_queue_capacity_ratio = ghost_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent());
            foyer::S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio as f64 / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio as f64 / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }
            .into()
        }
    };

    let vector_meta_cache_eviction_config = match &s.storage.cache.vector_meta_cache_eviction_config {
        CacheEvictionConfig::Lru {
            high_priority_ratio_in_percent,
        } => {
            let ratio = high_priority_ratio_in_percent
                .unwrap_or(default::storage::high_priority_ratio_in_percent());
            foyer::LruConfig {
                high_priority_pool_ratio: ratio as f64 / 100.0,
            }
            .into()
        }
        CacheEvictionConfig::Lfu {
            window_capacity_ratio_in_percent,
            protected_capacity_ratio_in_percent,
            cmsketch_eps,
            cmsketch_confidence,
        } => {
            let window_capacity_ratio = window_capacity_ratio_in_percent
                .unwrap_or(default::storage::window_capacity_ratio_in_percent());
            let protected_capacity_ratio = protected_capacity_ratio_in_percent
                .unwrap_or(default::storage::protected_capacity_ratio_in_percent());
            foyer::LfuConfig {
                window_capacity_ratio: window_capacity_ratio as f64 / 100.0,
                protected_capacity_ratio: protected_capacity_ratio as f64 / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }
            .into()
        }
        CacheEvictionConfig::S3Fifo {
            small_queue_capacity_ratio_in_percent,
            ghost_queue_capacity_ratio_in_percent,
            small_to_main_freq_threshold,
        } => {
            let small_queue_capacity_ratio = small_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent());
            let ghost_queue_capacity_ratio = ghost_queue_capacity_ratio_in_percent
                .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent());
            foyer::S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio as f64 / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio as f64 / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }
            .into()
        }
    };

    let block_file_cache_flush_buffer_threshold_mb = s
        .storage
        .data_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::block_file_cache_flush_buffer_threshold_mb());

    let meta_file_cache_flush_buffer_threshold_mb = s
        .storage
        .meta_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::meta_file_cache_flush_buffer_threshold_mb());

    StorageMemoryConfig {
        block_cache_capacity_mb,
        block_cache_shard_num,
        meta_cache_capacity_mb,
        meta_cache_shard_num,
        vector_block_cache_capacity_mb: s.storage.cache.vector_block_cache_capacity_mb,
        vector_block_cache_shard_num: s.storage.cache.vector_block_cache_shard_num,
        vector_meta_cache_capacity_mb: s.storage.cache.vector_meta_cache_capacity_mb,
        vector_meta_cache_shard_num: s.storage.cache.vector_meta_cache_shard_num,
        shared_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        block_cache_eviction_config,
        meta_cache_eviction_config,
        vector_block_cache_eviction_config,
        vector_meta_cache_eviction_config,
        block_file_cache_flush_buffer_threshold_mb,
        meta_file_cache_flush_buffer_threshold_mb,
    }
}

mod default {
    use foyer::{Compression, RecoverMode, RuntimeOptions, Throttle};

    pub mod storage {
        pub fn share_buffers_sync_parallelism() -> u32 { 2 }
        pub fn share_buffer_compaction_worker_threads_number() -> u32 { 4 }
        pub fn shared_buffer_capacity_mb() -> usize { 1024 }
        pub fn shared_buffer_flush_ratio() -> f32 { 0.8 }
        pub fn shared_buffer_min_batch_flush_size_mb() -> usize { 800 }
        #[deprecated] pub fn imm_merge_threshold() -> usize { 4 }
        pub fn write_conflict_detection_enabled() -> bool { true }
        pub fn max_cached_recent_versions_number() -> usize { 100 }
        pub fn block_cache_capacity_mb() -> usize { 512 }
        pub fn high_priority_ratio_in_percent() -> usize { 50 }
        pub fn window_capacity_ratio_in_percent() -> usize { 10 }
        pub fn protected_capacity_ratio_in_percent() -> usize { 50 }
        pub fn cmsketch_eps() -> f64 { 0.001 }
        pub fn cmsketch_confidence() -> f64 { 0.9 }
        pub fn small_queue_capacity_ratio_in_percent() -> usize { 10 }
        pub fn ghost_queue_capacity_ratio_in_percent() -> usize { 90 }
        pub fn small_to_main_freq_threshold() -> u8 { 2 }
        pub fn meta_cache_capacity_mb() -> usize { 128 }
        pub fn disable_remote_compactor() -> bool { false }
        pub fn share_buffer_upload_concurrency() -> usize { 8 }
        pub fn compactor_memory_limit_mb() -> usize { 512 }
        pub fn compactor_max_task_multiplier() -> f32 { 2.5 }
        pub fn compactor_memory_available_proportion() -> f64 { 0.8 }
        pub fn sstable_id_remote_fetch_number() -> u32 { 10 }
        pub fn min_sstable_size_mb() -> u32 { 32 }
        pub fn min_sst_size_for_streaming_upload() -> u64 { 32 * 1024 * 1024 }
        pub fn max_concurrent_compaction_task_number() -> u64 { 16 }
        pub fn max_preload_wait_time_mill() -> u64 { 0 }
        pub fn max_version_pinning_duration_sec() -> u64 { 3 * 3600 }
        pub fn compactor_max_sst_key_count() -> u64 { 2097152 }
        pub fn compact_iter_recreate_timeout_ms() -> u64 { 60000 }
        pub fn compactor_iter_max_io_retry_times() -> usize { 8 }
        pub fn compactor_max_sst_size() -> u64 { 512 * 1024 * 1024 }
        pub fn enable_fast_compaction() -> bool { true }
        pub fn check_compaction_result() -> bool { false }
        pub fn max_preload_io_retry_times() -> usize { 3 }
        pub fn mem_table_spill_threshold() -> usize { 4194304 }
        pub fn compactor_fast_max_compact_delete_ratio() -> u32 { 40 }
        pub fn compactor_fast_max_compact_task_size() -> u64 { 2147483648 }
        pub fn max_prefetch_block_number() -> usize { 16 }
        pub fn compactor_concurrent_uploading_sst_count() -> Option<usize> { None }
        pub fn compactor_max_overlap_sst_count() -> usize { 10 }
        pub fn compactor_max_preload_meta_file_count() -> usize { 1024 }
        pub fn vector_file_block_size_kb() -> usize { 16 }
        pub fn vector_block_cache_capacity_mb() -> usize { 64 }
        pub fn vector_block_cache_shard_num() -> usize { 64 }
        pub fn vector_meta_cache_capacity_mb() -> usize { 64 }
        pub fn vector_meta_cache_shard_num() -> usize { 16 }
        #[deprecated] pub fn table_info_statistic_history_times() -> usize { 240 }
        pub fn block_file_cache_flush_buffer_threshold_mb() -> usize { 32 }
        pub fn meta_file_cache_flush_buffer_threshold_mb() -> usize { 8 }
        pub fn time_travel_version_cache_capacity() -> u64 { 100 }
        pub fn iceberg_compaction_target_file_size_mb() -> u32 { 128 }
        pub fn iceberg_compaction_enable_validate() -> bool { false }
        pub fn iceberg_compaction_max_record_batch_rows() -> usize { 4096 }
        pub fn iceberg_compaction_write_parquet_max_row_group_rows() -> usize { 10000 }
        pub fn iceberg_compaction_min_size_per_partition_mb() -> u32 { 128 }
        pub fn iceberg_compaction_max_file_count_per_partition() -> u32 { 10 }
    }

    pub mod file_cache {
        use super::*;
        #[allow(clippy::disallowed_methods)]
        pub fn dir() -> String { std::env::temp_dir().join("foyer").to_string_lossy().to_string() }
        pub fn capacity_mb() -> usize { 256 }
        pub fn file_capacity_mb() -> usize { 64 }
        pub fn flushers() -> usize { 1 }
        pub fn reclaimers() -> usize { 1 }
        pub fn recover_concurrency() -> usize { 8 }
        pub fn insert_rate_limit_mb() -> usize { 0 }
        pub fn indexer_shards() -> usize { 64 }
        pub fn compression() -> Compression { Compression::None }
        pub fn flush_buffer_threshold_mb() -> Option<usize> { None }
        pub fn fifo_probation_ratio() -> f64 { 0.1 }
        pub fn recover_mode() -> RecoverMode { RecoverMode::Quiet }
        pub fn runtime_config() -> RuntimeOptions { RuntimeOptions::default() }
        pub fn throttle() -> Throttle { Throttle::new(0, 0) }
    }

    pub mod cache_refill {
        pub fn data_refill_levels() -> Vec<u32> { vec![] }
        pub fn timeout_ms() -> u64 { 6000 }
        pub fn concurrency() -> usize { 4 }
        pub fn unit() -> usize { 64 }
        pub fn threshold() -> f64 { 0.5 }
        pub fn recent_filter_layers() -> usize { 6 }
        pub fn recent_filter_rotate_interval_ms() -> usize { 10000 }
    }
}