// Copyright 2024 RisingWave Labs
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

//! This module defines the structure of the configuration file `risingwave.toml`.
//!
//! [`RwConfig`] corresponds to the whole config file and each other config struct corresponds to a
//! section in `risingwave.toml`.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;

use anyhow::Context;
use clap::ValueEnum;
use educe::Educe;
use foyer::memory::{LfuConfig, LruConfig, S3FifoConfig};
use risingwave_common_proc_macro::ConfigDoc;
pub use risingwave_common_proc_macro::OverrideConfig;
use risingwave_pb::meta::SystemParams;
use serde::{Deserialize, Serialize, Serializer};
use serde_default::DefaultFromSerde;
use serde_json::Value;

use crate::for_all_params;
use crate::hash::VirtualNode;

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB

/// Unrecognized fields in a config section. Generic over the config section type to provide better
/// error messages.
///
/// The current implementation will log warnings if there are unrecognized fields.
#[derive(Educe)]
#[educe(Clone, Default)]
pub struct Unrecognized<T: 'static> {
    inner: BTreeMap<String, Value>,
    _marker: std::marker::PhantomData<&'static T>,
}

impl<T> std::fmt::Debug for Unrecognized<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Unrecognized<T> {
    /// Returns all unrecognized fields as a map.
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.inner
    }
}

impl<'de, T> Deserialize<'de> for Unrecognized<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = BTreeMap::deserialize(deserializer)?;
        if !inner.is_empty() {
            tracing::warn!(
                "unrecognized fields in `{}`: {:?}",
                std::any::type_name::<T>(),
                inner.keys()
            );
        }
        Ok(Unrecognized {
            inner,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> Serialize for Unrecognized<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

pub fn load_config(path: &str, cli_override: impl OverrideConfig) -> RwConfig
where
{
    let mut config = if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        RwConfig::default()
    } else {
        let config_str = fs::read_to_string(path)
            .with_context(|| format!("failed to open config file at `{path}`"))
            .unwrap();
        toml::from_str(config_str.as_str())
            .context("failed to parse config file")
            .unwrap()
    };
    cli_override.r#override(&mut config);
    config
}

pub trait OverrideConfig {
    fn r#override(&self, config: &mut RwConfig);
}

impl<'a, T: OverrideConfig> OverrideConfig for &'a T {
    fn r#override(&self, config: &mut RwConfig) {
        T::r#override(self, config)
    }
}

/// For non-user-facing components where the CLI arguments do not override the config file.
#[derive(Clone, Copy)]
pub struct NoOverride;

impl OverrideConfig for NoOverride {
    fn r#override(&self, _config: &mut RwConfig) {}
}

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Educe, Clone, Serialize, Deserialize, Default, ConfigDoc)]
#[educe(Debug)]
pub struct RwConfig {
    #[serde(default)]
    #[config_doc(nested)]
    pub server: ServerConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta: MetaConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub batch: BatchConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub storage: StorageConfig,

    #[serde(default)]
    #[educe(Debug(ignore))]
    #[config_doc(nested)]
    pub system: SystemConfig,

    #[serde(flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

serde_with::with_prefix!(meta_prefix "meta_");
serde_with::with_prefix!(streaming_prefix "stream_");
serde_with::with_prefix!(batch_prefix "batch_");

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    #[default]
    Mem,
    Etcd,
    Sql,
}

/// The section `[meta]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaConfig {
    /// Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they
    /// are dangling.
    #[serde(default = "default::meta::min_sst_retention_time_sec")]
    pub min_sst_retention_time_sec: u64,

    /// Interval of automatic hummock full GC.
    #[serde(default = "default::meta::full_gc_interval_sec")]
    pub full_gc_interval_sec: u64,

    /// The spin interval when collecting global GC watermark in hummock.
    #[serde(default = "default::meta::collect_gc_watermark_spin_interval_sec")]
    pub collect_gc_watermark_spin_interval_sec: u64,

    /// Schedule compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_compaction_interval_sec")]
    pub periodic_compaction_interval_sec: u64,

    /// Interval of invoking a vacuum job, to remove stale metadata from meta store and objects
    /// from object store.
    #[serde(default = "default::meta::vacuum_interval_sec")]
    pub vacuum_interval_sec: u64,

    /// The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of
    /// meta node.
    #[serde(default = "default::meta::vacuum_spin_interval_ms")]
    pub vacuum_spin_interval_ms: u64,

    /// Interval of hummock version checkpoint.
    #[serde(default = "default::meta::hummock_version_checkpoint_interval_sec")]
    pub hummock_version_checkpoint_interval_sec: u64,

    /// If enabled, `SSTable` object file and version delta will be retained.
    ///
    /// `SSTable` object file need to be deleted via full GC.
    ///
    /// version delta need to be manually deleted.
    #[serde(default = "default::meta::enable_hummock_data_archive")]
    pub enable_hummock_data_archive: bool,

    /// The minimum delta log number a new checkpoint should compact, otherwise the checkpoint
    /// attempt is rejected.
    #[serde(default = "default::meta::min_delta_log_num_for_hummock_version_checkpoint")]
    pub min_delta_log_num_for_hummock_version_checkpoint: u64,

    /// Maximum allowed heartbeat interval in seconds.
    #[serde(default = "default::meta::max_heartbeat_interval_sec")]
    pub max_heartbeat_interval_secs: u32,

    /// Whether to enable fail-on-recovery. Should only be used in e2e tests.
    #[serde(default)]
    pub disable_recovery: bool,

    /// Whether to disable adaptive-scaling feature.
    #[serde(default)]
    pub disable_automatic_parallelism_control: bool,

    /// The number of streaming jobs per scaling operation.
    #[serde(default = "default::meta::parallelism_control_batch_size")]
    pub parallelism_control_batch_size: usize,

    /// The period of parallelism control trigger.
    #[serde(default = "default::meta::parallelism_control_trigger_period_sec")]
    pub parallelism_control_trigger_period_sec: u64,

    /// The first delay of parallelism control.
    #[serde(default = "default::meta::parallelism_control_trigger_first_delay_sec")]
    pub parallelism_control_trigger_first_delay_sec: u64,

    #[serde(default = "default::meta::meta_leader_lease_secs")]
    pub meta_leader_lease_secs: u64,

    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// It is mainly useful for playgrounds.
    #[serde(default)]
    pub dangerous_max_idle_secs: Option<u64>,

    /// The default global parallelism for all streaming jobs, if user doesn't specify the
    /// parallelism, this value will be used. `FULL` means use all available parallelism units,
    /// otherwise it's a number.
    #[serde(default = "default::meta::default_parallelism")]
    pub default_parallelism: DefaultParallelism,

    /// Whether to enable deterministic compaction scheduling, which
    /// will disable all auto scheduling of compaction tasks.
    /// Should only be used in e2e tests.
    #[serde(default)]
    pub enable_compaction_deterministic: bool,

    /// Enable sanity check when SSTs are committed.
    #[serde(default)]
    pub enable_committed_sst_sanity_check: bool,

    #[serde(default = "default::meta::node_num_monitor_interval_sec")]
    pub node_num_monitor_interval_sec: u64,

    #[serde(default = "default::meta::backend")]
    pub backend: MetaBackend,

    /// Schedule `space_reclaim` compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_space_reclaim_compaction_interval_sec")]
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// Schedule `ttl_reclaim` compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_ttl_reclaim_compaction_interval_sec")]
    pub periodic_ttl_reclaim_compaction_interval_sec: u64,

    #[serde(default = "default::meta::periodic_tombstone_reclaim_compaction_interval_sec")]
    pub periodic_tombstone_reclaim_compaction_interval_sec: u64,

    #[serde(default = "default::meta::periodic_split_compact_group_interval_sec")]
    pub periodic_split_compact_group_interval_sec: u64,

    #[serde(default = "default::meta::move_table_size_limit")]
    pub move_table_size_limit: u64,

    #[serde(default = "default::meta::split_group_size_limit")]
    pub split_group_size_limit: u64,

    #[serde(default = "default::meta::cut_table_size_limit")]
    pub cut_table_size_limit: u64,

    #[serde(default, flatten)]
    pub unrecognized: Unrecognized<Self>,

    /// Whether config object storage bucket lifecycle to purge stale data.
    #[serde(default)]
    pub do_not_config_object_storage_lifecycle: bool,

    #[serde(default = "default::meta::partition_vnode_count")]
    pub partition_vnode_count: u32,

    #[serde(default = "default::meta::table_write_throughput_threshold")]
    pub table_write_throughput_threshold: u64,

    #[serde(default = "default::meta::min_table_split_write_throughput")]
    /// If the size of one table is smaller than `min_table_split_write_throughput`, we would not
    /// split it to an single group.
    pub min_table_split_write_throughput: u64,

    // If the compaction task does not report heartbeat beyond the
    // `compaction_task_max_heartbeat_interval_secs` interval, we will cancel the task
    #[serde(default = "default::meta::compaction_task_max_heartbeat_interval_secs")]
    pub compaction_task_max_heartbeat_interval_secs: u64,

    // If the compaction task does not change in progress beyond the
    // `compaction_task_max_heartbeat_interval_secs` interval, we will cancel the task
    #[serde(default = "default::meta::compaction_task_max_progress_interval_secs")]
    pub compaction_task_max_progress_interval_secs: u64,

    #[serde(default)]
    #[config_doc(nested)]
    pub compaction_config: CompactionConfig,

    #[serde(default = "default::meta::hybird_partition_vnode_count")]
    pub hybird_partition_vnode_count: u32,
    #[serde(default = "default::meta::event_log_enabled")]
    pub event_log_enabled: bool,
    /// Keeps the latest N events per channel.
    #[serde(default = "default::meta::event_log_channel_max_size")]
    pub event_log_channel_max_size: u32,

    #[serde(default, with = "meta_prefix")]
    #[config_doc(omitted)]
    pub developer: MetaDeveloperConfig,
    /// Whether compactor should rewrite row to remove dropped column.
    #[serde(default = "default::meta::enable_dropped_column_reclaim")]
    pub enable_dropped_column_reclaim: bool,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum DefaultParallelism {
    #[default]
    Full,
    Default(NonZeroUsize),
}

impl Serialize for DefaultParallelism {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        match self {
            DefaultParallelism::Full => Parallelism::Str("Full".to_string()).serialize(serializer),
            DefaultParallelism::Default(val) => {
                Parallelism::Int(val.get() as _).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for DefaultParallelism {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        let p = Parallelism::deserialize(deserializer)?;
        match p {
            Parallelism::Str(s) => {
                if s.trim().eq_ignore_ascii_case("full") {
                    Ok(DefaultParallelism::Full)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid default parallelism: {}",
                        s
                    )))
                }
            }
            Parallelism::Int(i) => Ok(DefaultParallelism::Default(if i > VirtualNode::COUNT {
                Err(serde::de::Error::custom(format!(
                    "default parallelism should be not great than {}",
                    VirtualNode::COUNT
                )))?
            } else {
                NonZeroUsize::new(i).ok_or_else(|| {
                    serde::de::Error::custom("default parallelism should be greater than 0")
                })?
            })),
        }
    }
}

/// The subsections `[meta.developer]`.
///
/// It is put at [`MetaConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaDeveloperConfig {
    /// The number of traces to be cached in-memory by the tracing collector
    /// embedded in the meta node.
    #[serde(default = "default::developer::meta_cached_traces_num")]
    pub cached_traces_num: u32,

    /// The maximum memory usage in bytes for the tracing collector embedded
    /// in the meta node.
    #[serde(default = "default::developer::meta_cached_traces_memory_limit_bytes")]
    pub cached_traces_memory_limit_bytes: usize,

    /// Compaction picker config
    #[serde(default = "default::developer::enable_trivial_move")]
    pub enable_trivial_move: bool,
    #[serde(default = "default::developer::enable_check_task_level_overlap")]
    pub enable_check_task_level_overlap: bool,
    #[serde(default = "default::developer::max_trivial_move_task_count_per_loop")]
    pub max_trivial_move_task_count_per_loop: usize,

    #[serde(default = "default::developer::max_get_task_probe_times")]
    pub max_get_task_probe_times: usize,
}

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::server::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    #[serde(default = "default::server::connection_pool_size")]
    pub connection_pool_size: u16,

    /// Used for control the metrics level, similar to log level.
    #[serde(default = "default::server::metrics_level")]
    pub metrics_level: MetricLevel,

    #[serde(default = "default::server::telemetry_enabled")]
    pub telemetry_enabled: bool,

    /// Enable heap profile dump when memory usage is high.
    #[serde(default)]
    pub heap_profiling: HeapProfilingConfig,

    // Number of max pending reset stream for grpc server.
    #[serde(default = "default::server::grpc_max_reset_stream_size")]
    pub grpc_max_reset_stream: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// The section `[batch]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default, with = "batch_prefix")]
    #[config_doc(omitted)]
    pub developer: BatchDeveloperConfig,

    /// This is the max number of queries per sql session.
    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    /// This is the max number of batch queries per frontend node.
    #[serde(default)]
    pub max_batch_queries_per_frontend_node: Option<u64>,

    #[serde(default = "default::batch::enable_barrier_read")]
    pub enable_barrier_read: bool,

    /// Timeout for a batch query in seconds.
    #[serde(default = "default::batch::statement_timeout_in_sec")]
    pub statement_timeout_in_sec: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    #[serde(default = "default::batch::frontend_compute_runtime_worker_threads")]
    /// frontend compute runtime worker threads
    pub frontend_compute_runtime_worker_threads: usize,

    /// This is the secs used to mask a worker unavailable temporarily.
    #[serde(default = "default::batch::mask_worker_temporary_secs")]
    pub mask_worker_temporary_secs: usize,
}

/// The section `[streaming]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingConfig {
    /// The maximum number of barriers in-flight in the compute nodes.
    #[serde(default = "default::streaming::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[serde(default = "default::streaming::async_stack_trace")]
    pub async_stack_trace: AsyncStackTraceOption,

    #[serde(default, with = "streaming_prefix")]
    #[config_doc(omitted)]
    pub developer: StreamingDeveloperConfig,

    /// Max unique user stream errors per actor
    #[serde(default = "default::streaming::unique_user_stream_errors")]
    pub unique_user_stream_errors: usize,

    /// Control the strictness of stream consistency.
    #[serde(default = "default::streaming::unsafe_enable_strict_consistency")]
    pub unsafe_enable_strict_consistency: bool,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

pub use risingwave_common_metrics::MetricLevel;

/// the section `[storage.cache]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheConfig {
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    #[serde(default)]
    pub block_cache_shard_num: Option<usize>,

    #[serde(default)]
    pub block_cache_eviction: CacheEvictionConfig,

    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    #[serde(default)]
    pub meta_cache_shard_num: Option<usize>,

    #[serde(default)]
    pub meta_cache_eviction: CacheEvictionConfig,
}

/// the section `[storage.cache.eviction]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "algorithm")]
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
    S3Fifo {
        small_queue_capacity_ratio_in_percent: Option<usize>,
    },
}

impl Default for CacheEvictionConfig {
    fn default() -> Self {
        Self::Lru {
            high_priority_ratio_in_percent: None,
        }
    }
}

/// The section `[storage]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StorageConfig {
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    #[serde(default = "default::storage::share_buffers_sync_parallelism")]
    pub share_buffers_sync_parallelism: u32,

    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    #[serde(default = "default::storage::share_buffer_compaction_worker_threads_number")]
    pub share_buffer_compaction_worker_threads_number: u32,

    /// Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
    /// is enough space.
    #[serde(default)]
    pub shared_buffer_capacity_mb: Option<usize>,

    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    #[serde(default = "default::storage::shared_buffer_flush_ratio")]
    pub shared_buffer_flush_ratio: f32,

    /// The threshold for the number of immutable memtables to merge to a new imm.
    #[serde(default = "default::storage::imm_merge_threshold")]
    pub imm_merge_threshold: usize,

    /// Whether to enable write conflict detection
    #[serde(default = "default::storage::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    #[serde(default)]
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

    #[serde(default)]
    pub data_file_cache: FileCacheConfig,

    #[serde(default)]
    pub meta_file_cache: FileCacheConfig,

    #[serde(default)]
    pub cache_refill: CacheRefillConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::storage::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    /// Max sub compaction task numbers
    #[serde(default = "default::storage::max_sub_compaction")]
    pub max_sub_compaction: u32,

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

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// The spill threshold for mem table.
    #[serde(default = "default::storage::mem_table_spill_threshold")]
    pub mem_table_spill_threshold: usize,

    #[serde(default)]
    pub object_store: ObjectStoreConfig,
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

/// The subsection `[storage.data_file_cache]` and `[storage.meta_file_cache]` in `risingwave.toml`.
///
/// It's put at [`StorageConfig::data_file_cache`] and  [`StorageConfig::meta_file_cache`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache::dir")]
    pub dir: String,

    #[serde(default = "default::file_cache::capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache::file_capacity_mb")]
    pub file_capacity_mb: usize,

    #[serde(default = "default::file_cache::device_align")]
    pub device_align: usize,

    #[serde(default = "default::file_cache::device_io_size")]
    pub device_io_size: usize,

    #[serde(default = "default::file_cache::flushers")]
    pub flushers: usize,

    #[serde(default = "default::file_cache::reclaimers")]
    pub reclaimers: usize,

    #[serde(default = "default::file_cache::recover_concurrency")]
    pub recover_concurrency: usize,

    #[serde(default = "default::file_cache::lfu_window_to_cache_size_ratio")]
    pub lfu_window_to_cache_size_ratio: usize,

    #[serde(default = "default::file_cache::lfu_tiny_lru_capacity_ratio")]
    pub lfu_tiny_lru_capacity_ratio: f64,

    #[serde(default = "default::file_cache::insert_rate_limit_mb")]
    pub insert_rate_limit_mb: usize,

    #[serde(default = "default::file_cache::catalog_bits")]
    pub catalog_bits: usize,

    #[serde(default = "default::file_cache::compression")]
    pub compression: String,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Debug, Default, Clone, Copy, ValueEnum, Serialize, Deserialize)]
pub enum AsyncStackTraceOption {
    /// Disabled.
    Off,
    /// Enabled with basic instruments.
    On,
    /// Enabled with extra verbose instruments in release build.
    /// Behaves the same as `on` in debug build due to performance concern.
    #[default]
    #[clap(alias = "verbose")]
    ReleaseVerbose,
}

impl AsyncStackTraceOption {
    pub fn is_verbose(self) -> Option<bool> {
        match self {
            Self::Off => None,
            Self::On => Some(false),
            Self::ReleaseVerbose => Some(!cfg!(debug_assertions)),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub enum CompactorMode {
    #[default]
    #[clap(alias = "dedicated")]
    Dedicated,

    #[clap(alias = "shared")]
    Shared,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct HeapProfilingConfig {
    /// Enable to auto dump heap profile when memory usage is high
    #[serde(default = "default::heap_profiling::enable_auto")]
    pub enable_auto: bool,

    /// The proportion (number between 0 and 1) of memory usage to trigger heap profile dump
    #[serde(default = "default::heap_profiling::threshold_auto")]
    pub threshold_auto: f32,

    /// The directory to dump heap profile. If empty, the prefix in `MALLOC_CONF` will be used
    #[serde(default = "default::heap_profiling::dir")]
    pub dir: String,
}

/// The subsections `[streaming.developer]`.
///
/// It is put at [`StreamingConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingDeveloperConfig {
    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    #[serde(default = "default::developer::stream_enable_executor_row_count")]
    pub enable_executor_row_count: bool,

    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// Limit number of the cached entries in an extreme aggregation call.
    #[serde(default = "default::developer::unsafe_stream_extreme_cache_size")]
    pub unsafe_extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    #[serde(default = "default::developer::stream_chunk_size")]
    pub chunk_size: usize,

    /// The initial permits that a channel holds, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_exchange_initial_permits")]
    pub exchange_initial_permits: usize,

    /// The permits that are batched to add back, for reducing the backward `AddPermits` messages
    /// in remote exchange.
    #[serde(default = "default::developer::stream_exchange_batched_permits")]
    pub exchange_batched_permits: usize,

    /// The maximum number of concurrent barriers in an exchange channel.
    #[serde(default = "default::developer::stream_exchange_concurrent_barriers")]
    pub exchange_concurrent_barriers: usize,

    /// The concurrency for dispatching messages to different downstream jobs.
    ///
    /// - `1` means no concurrency, i.e., dispatch messages to downstream jobs one by one.
    /// - `0` means unlimited concurrency.
    #[serde(default = "default::developer::stream_exchange_concurrent_dispatchers")]
    pub exchange_concurrent_dispatchers: usize,

    /// The initial permits for a dml channel, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_dml_channel_initial_permits")]
    pub dml_channel_initial_permits: usize,

    /// The max heap size of dirty groups of `HashAggExecutor`.
    #[serde(default = "default::developer::stream_hash_agg_max_dirty_groups_heap_size")]
    pub hash_agg_max_dirty_groups_heap_size: usize,

    #[serde(default = "default::developer::memory_controller_threshold_aggressive")]
    pub memory_controller_threshold_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_threshold_graceful")]
    pub memory_controller_threshold_graceful: f64,

    #[serde(default = "default::developer::memory_controller_threshold_stable")]
    pub memory_controller_threshold_stable: f64,

    #[serde(default = "default::developer::stream_enable_arrangement_backfill")]
    /// Enable arrangement backfill
    /// If true, the arrangement backfill will be disabled,
    /// even if session variable set.
    pub enable_arrangement_backfill: bool,
}

/// The subsections `[batch.developer]`.
///
/// It is put at [`BatchConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchDeveloperConfig {
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// The size of the channel used for output to exchange/shuffle.
    #[serde(default = "default::developer::batch_output_channel_size")]
    pub output_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    #[serde(default = "default::developer::batch_chunk_size")]
    pub chunk_size: usize,
}

macro_rules! define_system_config {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        paste::paste!(
            /// The section `[system]` in `risingwave.toml`. All these fields are used to initialize the system
            /// parameters persisted in Meta store. Most fields are for testing purpose only and should not be
            /// documented.
            #[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
            pub struct SystemConfig {
                $(
                    #[doc = $doc]
                    #[serde(default = "default::system::" $field "_opt")]
                    pub $field: Option<$type>,
                )*
            }
        );
    };
}

for_all_params!(define_system_config);

/// The subsections `[storage.object_store]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreConfig {
    #[serde(default = "default::object_store_config::object_store_set_atomic_write_dir")]
    pub object_store_set_atomic_write_dir: bool,

    #[serde(default)]
    pub retry: ObjectStoreRetryConfig,

    #[serde(default)]
    pub s3: S3ObjectStoreConfig,
}

impl ObjectStoreConfig {
    pub fn set_atomic_write_dir(&mut self) {
        self.object_store_set_atomic_write_dir = true;
    }
}

/// The subsections `[storage.object_store.s3]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreConfig {
    #[serde(default = "default::object_store_config::s3::object_store_keepalive_ms")]
    pub object_store_keepalive_ms: Option<u64>,
    #[serde(default = "default::object_store_config::s3::object_store_recv_buffer_size")]
    pub object_store_recv_buffer_size: Option<usize>,
    #[serde(default = "default::object_store_config::s3::object_store_send_buffer_size")]
    pub object_store_send_buffer_size: Option<usize>,
    #[serde(default = "default::object_store_config::s3::object_store_nodelay")]
    pub object_store_nodelay: Option<bool>,
    /// For backwards compatibility, users should use `S3ObjectStoreDeveloperConfig` instead.
    #[serde(
        default = "default::object_store_config::s3::developer::object_store_retry_unknown_service_error"
    )]
    pub retry_unknown_service_error: bool,
    #[serde(default = "default::object_store_config::s3::identity_resolution_timeout_s")]
    pub identity_resolution_timeout_s: u64,
    #[serde(default)]
    pub developer: S3ObjectStoreDeveloperConfig,
}

/// The subsections `[storage.object_store.s3.developer]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreDeveloperConfig {
    /// Whether to retry s3 sdk error from which no error metadata is provided.
    #[serde(
        default = "default::object_store_config::s3::developer::object_store_retry_unknown_service_error"
    )]
    pub object_store_retry_unknown_service_error: bool,
    /// An array of error codes that should be retried.
    /// e.g. `["SlowDown", "TooManyRequests"]`
    #[serde(
        default = "default::object_store_config::s3::developer::object_store_retryable_service_error_codes"
    )]
    pub object_store_retryable_service_error_codes: Vec<String>,

    #[serde(default = "default::object_store_config::s3::developer::use_opendal")]
    pub use_opendal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreRetryConfig {
    #[serde(default = "default::object_store_config::object_store_req_backoff_interval_ms")]
    pub req_backoff_interval_ms: u64,
    #[serde(default = "default::object_store_config::object_store_req_backoff_max_delay_ms")]
    pub req_backoff_max_delay_ms: u64,
    #[serde(default = "default::object_store_config::object_store_req_backoff_factor")]
    pub req_backoff_factor: u64,

    // upload
    #[serde(default = "default::object_store_config::object_store_upload_attempt_timeout_ms")]
    pub upload_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_upload_retry_attempts")]
    pub upload_retry_attempts: usize,

    // streaming_upload_init + streaming_upload
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_attempt_timeout_ms"
    )]
    pub streaming_upload_attempt_timeout_ms: u64,
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_retry_attempts"
    )]
    pub streaming_upload_retry_attempts: usize,

    // read
    #[serde(default = "default::object_store_config::object_store_read_attempt_timeout_ms")]
    pub read_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_read_retry_attempts")]
    pub read_retry_attempts: usize,

    // streaming_read_init + streaming_read
    #[serde(
        default = "default::object_store_config::object_store_streaming_read_attempt_timeout_ms"
    )]
    pub streaming_read_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_streaming_read_retry_attempts")]
    pub streaming_read_retry_attempts: usize,

    // metadata
    #[serde(default = "default::object_store_config::object_store_metadata_attempt_timeout_ms")]
    pub metadata_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_metadata_retry_attempts")]
    pub metadata_retry_attempts: usize,

    // delete
    #[serde(default = "default::object_store_config::object_store_delete_attempt_timeout_ms")]
    pub delete_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_delete_retry_attempts")]
    pub delete_retry_attempts: usize,

    // delete_object
    #[serde(
        default = "default::object_store_config::object_store_delete_objects_attempt_timeout_ms"
    )]
    pub delete_objects_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_delete_objects_retry_attempts")]
    pub delete_objects_retry_attempts: usize,

    // list
    #[serde(default = "default::object_store_config::object_store_list_attempt_timeout_ms")]
    pub list_attempt_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_list_retry_attempts")]
    pub list_retry_attempts: usize,
}

impl SystemConfig {
    #![allow(deprecated)]
    pub fn into_init_system_params(self) -> SystemParams {
        macro_rules! fields {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                SystemParams {
                    $($field: self.$field,)*
                    ..Default::default() // deprecated fields
                }
            };
        }

        let mut system_params = for_all_params!(fields);

        // Initialize backup_storage_url and backup_storage_directory if not set.
        if let Some(state_store) = &system_params.state_store
            && let Some(data_directory) = &system_params.data_directory
        {
            if system_params.backup_storage_url.is_none() {
                if let Some(hummock_state_store) = state_store.strip_prefix("hummock+") {
                    system_params.backup_storage_url = Some(hummock_state_store.to_owned());
                } else {
                    system_params.backup_storage_url = Some("memory".to_string());
                }
                tracing::info!("initialize backup_storage_url based on state_store");
            }
            if system_params.backup_storage_directory.is_none() {
                system_params.backup_storage_directory = Some(format!("{data_directory}/backup"));
                tracing::info!("initialize backup_storage_directory based on data_directory");
            }
        }
        system_params
    }
}

pub mod default {
    pub mod meta {
        use crate::config::{DefaultParallelism, MetaBackend};

        pub fn min_sst_retention_time_sec() -> u64 {
            86400
        }

        pub fn full_gc_interval_sec() -> u64 {
            86400
        }

        pub fn collect_gc_watermark_spin_interval_sec() -> u64 {
            5
        }

        pub fn periodic_compaction_interval_sec() -> u64 {
            60
        }

        pub fn vacuum_interval_sec() -> u64 {
            30
        }

        pub fn vacuum_spin_interval_ms() -> u64 {
            10
        }

        pub fn hummock_version_checkpoint_interval_sec() -> u64 {
            30
        }

        pub fn enable_hummock_data_archive() -> bool {
            false
        }

        pub fn min_delta_log_num_for_hummock_version_checkpoint() -> u64 {
            10
        }

        pub fn max_heartbeat_interval_sec() -> u32 {
            60
        }

        pub fn meta_leader_lease_secs() -> u64 {
            30
        }

        pub fn default_parallelism() -> DefaultParallelism {
            DefaultParallelism::Full
        }

        pub fn node_num_monitor_interval_sec() -> u64 {
            10
        }

        pub fn backend() -> MetaBackend {
            MetaBackend::Mem
        }

        pub fn periodic_space_reclaim_compaction_interval_sec() -> u64 {
            3600 // 60min
        }

        pub fn periodic_ttl_reclaim_compaction_interval_sec() -> u64 {
            1800 // 30mi
        }

        pub fn periodic_split_compact_group_interval_sec() -> u64 {
            10 // 10s
        }

        pub fn periodic_tombstone_reclaim_compaction_interval_sec() -> u64 {
            600
        }

        pub fn move_table_size_limit() -> u64 {
            10 * 1024 * 1024 * 1024 // 10GB
        }

        pub fn split_group_size_limit() -> u64 {
            64 * 1024 * 1024 * 1024 // 64GB
        }

        pub fn partition_vnode_count() -> u32 {
            16
        }

        pub fn table_write_throughput_threshold() -> u64 {
            16 * 1024 * 1024 // 16MB
        }

        pub fn min_table_split_write_throughput() -> u64 {
            4 * 1024 * 1024 // 4MB
        }

        pub fn compaction_task_max_heartbeat_interval_secs() -> u64 {
            30 // 30s
        }

        pub fn compaction_task_max_progress_interval_secs() -> u64 {
            60 * 10 // 10min
        }

        pub fn cut_table_size_limit() -> u64 {
            1024 * 1024 * 1024 // 1GB
        }

        pub fn hybird_partition_vnode_count() -> u32 {
            4
        }

        pub fn event_log_enabled() -> bool {
            true
        }

        pub fn event_log_channel_max_size() -> u32 {
            10
        }

        pub fn parallelism_control_batch_size() -> usize {
            10
        }

        pub fn parallelism_control_trigger_period_sec() -> u64 {
            10
        }

        pub fn parallelism_control_trigger_first_delay_sec() -> u64 {
            30
        }
        pub fn enable_dropped_column_reclaim() -> bool {
            false
        }
    }

    pub mod server {
        use crate::config::MetricLevel;

        pub fn heartbeat_interval_ms() -> u32 {
            1000
        }

        pub fn connection_pool_size() -> u16 {
            16
        }

        pub fn metrics_level() -> MetricLevel {
            MetricLevel::Info
        }

        pub fn telemetry_enabled() -> bool {
            true
        }

        pub fn grpc_max_reset_stream_size() -> u32 {
            200
        }
    }

    pub mod storage {
        pub fn share_buffers_sync_parallelism() -> u32 {
            1
        }

        pub fn share_buffer_compaction_worker_threads_number() -> u32 {
            4
        }

        pub fn shared_buffer_capacity_mb() -> usize {
            1024
        }

        pub fn shared_buffer_flush_ratio() -> f32 {
            0.8
        }

        pub fn imm_merge_threshold() -> usize {
            0 // disable
        }

        pub fn write_conflict_detection_enabled() -> bool {
            cfg!(debug_assertions)
        }

        pub fn block_cache_capacity_mb() -> usize {
            512
        }

        pub fn high_priority_ratio_in_percent() -> usize {
            70
        }

        pub fn window_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn protected_capacity_ratio_in_percent() -> usize {
            80
        }

        pub fn cmsketch_eps() -> f64 {
            0.002
        }

        pub fn cmsketch_confidence() -> f64 {
            0.95
        }

        pub fn small_queue_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn meta_cache_capacity_mb() -> usize {
            128
        }

        pub fn disable_remote_compactor() -> bool {
            false
        }

        pub fn share_buffer_upload_concurrency() -> usize {
            8
        }

        pub fn compactor_memory_limit_mb() -> usize {
            512
        }

        pub fn compactor_max_task_multiplier() -> f32 {
            3.0000
        }

        pub fn compactor_memory_available_proportion() -> f64 {
            0.8
        }

        pub fn sstable_id_remote_fetch_number() -> u32 {
            10
        }

        pub fn min_sst_size_for_streaming_upload() -> u64 {
            // 32MB
            32 * 1024 * 1024
        }

        pub fn max_sub_compaction() -> u32 {
            4
        }

        pub fn max_concurrent_compaction_task_number() -> u64 {
            16
        }

        pub fn max_preload_wait_time_mill() -> u64 {
            0
        }

        pub fn max_version_pinning_duration_sec() -> u64 {
            3 * 3600
        }

        pub fn compactor_max_sst_key_count() -> u64 {
            2 * 1024 * 1024 // 200w
        }

        pub fn compact_iter_recreate_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn compactor_iter_max_io_retry_times() -> usize {
            8
        }

        pub fn compactor_max_sst_size() -> u64 {
            512 * 1024 * 1024 // 512m
        }

        pub fn enable_fast_compaction() -> bool {
            true
        }

        pub fn check_compaction_result() -> bool {
            false
        }

        pub fn max_preload_io_retry_times() -> usize {
            3
        }
        pub fn mem_table_spill_threshold() -> usize {
            4 << 20
        }

        pub fn compactor_fast_max_compact_delete_ratio() -> u32 {
            40
        }

        pub fn compactor_fast_max_compact_task_size() -> u64 {
            2 * 1024 * 1024 * 1024 // 2g
        }

        pub fn max_prefetch_block_number() -> usize {
            16
        }
    }

    pub mod streaming {
        use crate::config::AsyncStackTraceOption;

        pub fn in_flight_barrier_nums() -> usize {
            // quick fix
            // TODO: remove this limitation from code
            10000
        }

        pub fn async_stack_trace() -> AsyncStackTraceOption {
            AsyncStackTraceOption::default()
        }

        pub fn unique_user_stream_errors() -> usize {
            10
        }

        pub fn unsafe_enable_strict_consistency() -> bool {
            true
        }
    }

    pub mod file_cache {

        pub fn dir() -> String {
            "".to_string()
        }

        pub fn capacity_mb() -> usize {
            1024
        }

        pub fn file_capacity_mb() -> usize {
            64
        }

        pub fn device_align() -> usize {
            4096
        }

        pub fn device_io_size() -> usize {
            16 * 1024
        }

        pub fn flushers() -> usize {
            4
        }

        pub fn reclaimers() -> usize {
            4
        }

        pub fn recover_concurrency() -> usize {
            8
        }

        pub fn lfu_window_to_cache_size_ratio() -> usize {
            1
        }

        pub fn lfu_tiny_lru_capacity_ratio() -> f64 {
            0.01
        }

        pub fn insert_rate_limit_mb() -> usize {
            0
        }

        pub fn catalog_bits() -> usize {
            6
        }

        pub fn compression() -> String {
            "none".to_string()
        }
    }

    pub mod cache_refill {
        pub fn data_refill_levels() -> Vec<u32> {
            vec![]
        }

        pub fn timeout_ms() -> u64 {
            6000
        }

        pub fn concurrency() -> usize {
            10
        }

        pub fn unit() -> usize {
            64
        }

        pub fn threshold() -> f64 {
            0.5
        }

        pub fn recent_filter_layers() -> usize {
            6
        }

        pub fn recent_filter_rotate_interval_ms() -> usize {
            10000
        }
    }

    pub mod heap_profiling {
        pub fn enable_auto() -> bool {
            true
        }

        pub fn threshold_auto() -> f32 {
            0.9
        }

        pub fn dir() -> String {
            "./".to_string()
        }
    }

    pub mod developer {
        pub fn meta_cached_traces_num() -> u32 {
            256
        }

        pub fn meta_cached_traces_memory_limit_bytes() -> usize {
            1 << 27 // 128 MiB
        }

        pub fn batch_output_channel_size() -> usize {
            64
        }

        pub fn batch_chunk_size() -> usize {
            1024
        }

        pub fn stream_enable_executor_row_count() -> bool {
            false
        }

        pub fn connector_message_buffer_size() -> usize {
            16
        }

        pub fn unsafe_stream_extreme_cache_size() -> usize {
            10
        }

        pub fn stream_chunk_size() -> usize {
            256
        }

        pub fn stream_exchange_initial_permits() -> usize {
            2048
        }

        pub fn stream_exchange_batched_permits() -> usize {
            256
        }

        pub fn stream_exchange_concurrent_barriers() -> usize {
            1
        }

        pub fn stream_exchange_concurrent_dispatchers() -> usize {
            0
        }

        pub fn stream_dml_channel_initial_permits() -> usize {
            32768
        }

        pub fn stream_hash_agg_max_dirty_groups_heap_size() -> usize {
            64 << 20 // 64MB
        }

        pub fn enable_trivial_move() -> bool {
            true
        }

        pub fn enable_check_task_level_overlap() -> bool {
            false
        }

        pub fn max_trivial_move_task_count_per_loop() -> usize {
            256
        }

        pub fn max_get_task_probe_times() -> usize {
            5
        }

        pub fn memory_controller_threshold_aggressive() -> f64 {
            0.9
        }
        pub fn memory_controller_threshold_graceful() -> f64 {
            0.8
        }
        pub fn memory_controller_threshold_stable() -> f64 {
            0.7
        }
        pub fn stream_enable_arrangement_backfill() -> bool {
            true
        }
    }

    pub use crate::system_param::default as system;

    pub mod batch {
        pub fn enable_barrier_read() -> bool {
            false
        }

        pub fn statement_timeout_in_sec() -> u32 {
            // 1 hour
            60 * 60
        }

        pub fn frontend_compute_runtime_worker_threads() -> usize {
            4
        }

        pub fn mask_worker_temporary_secs() -> usize {
            30
        }
    }

    pub mod compaction_config {
        const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB
        const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * 1024 * 1024; // 128MB
        const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * 1024 * 1024; // 512MB

        // decrease this configure when the generation of checkpoint barrier is not frequent.
        const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 12;
        const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 32 * 1024 * 1024; // 32MB
        const DEFAULT_MAX_SUB_COMPACTION: u32 = 4;
        const DEFAULT_LEVEL_MULTIPLIER: u64 = 5;
        const DEFAULT_MAX_SPACE_RECLAIM_BYTES: u64 = 512 * 1024 * 1024; // 512MB;
        const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER: u64 = 300;
        const DEFAULT_MAX_COMPACTION_FILE_COUNT: u64 = 100;
        const DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 3;
        const DEFAULT_MIN_OVERLAPPING_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 12;
        const DEFAULT_TOMBSTONE_RATIO_PERCENT: u32 = 40;
        const DEFAULT_EMERGENCY_PICKER: bool = true;

        use crate::catalog::hummock::CompactionFilterFlag;

        pub fn max_bytes_for_level_base() -> u64 {
            DEFAULT_MAX_BYTES_FOR_LEVEL_BASE
        }
        pub fn max_bytes_for_level_multiplier() -> u64 {
            DEFAULT_LEVEL_MULTIPLIER
        }
        pub fn max_compaction_bytes() -> u64 {
            DEFAULT_MAX_COMPACTION_BYTES
        }
        pub fn sub_level_max_compaction_bytes() -> u64 {
            DEFAULT_MIN_COMPACTION_BYTES
        }
        pub fn level0_tier_compact_file_number() -> u64 {
            DEFAULT_TIER_COMPACT_TRIGGER_NUMBER
        }
        pub fn target_file_size_base() -> u64 {
            DEFAULT_TARGET_FILE_SIZE_BASE
        }
        pub fn compaction_filter_mask() -> u32 {
            (CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL).into()
        }
        pub fn max_sub_compaction() -> u32 {
            DEFAULT_MAX_SUB_COMPACTION
        }
        pub fn level0_stop_write_threshold_sub_level_number() -> u64 {
            DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER
        }
        pub fn level0_sub_level_compact_level_count() -> u32 {
            DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT
        }
        pub fn level0_overlapping_sub_level_compact_level_count() -> u32 {
            DEFAULT_MIN_OVERLAPPING_SUB_LEVEL_COMPACT_LEVEL_COUNT
        }
        pub fn max_space_reclaim_bytes() -> u64 {
            DEFAULT_MAX_SPACE_RECLAIM_BYTES
        }
        pub fn level0_max_compact_file_number() -> u64 {
            DEFAULT_MAX_COMPACTION_FILE_COUNT
        }
        pub fn tombstone_reclaim_ratio() -> u32 {
            DEFAULT_TOMBSTONE_RATIO_PERCENT
        }

        pub fn enable_emergency_picker() -> bool {
            DEFAULT_EMERGENCY_PICKER
        }
    }

    pub mod object_store_config {
        const DEFAULT_REQ_BACKOFF_INTERVAL_MS: u64 = 1000; // 1s
        const DEFAULT_REQ_BACKOFF_MAX_DELAY_MS: u64 = 10 * 1000; // 10s
        const DEFAULT_REQ_MAX_RETRY_ATTEMPTS: usize = 3;

        pub fn object_store_set_atomic_write_dir() -> bool {
            false
        }

        pub fn object_store_req_backoff_interval_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_INTERVAL_MS
        }

        pub fn object_store_req_backoff_max_delay_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_MAX_DELAY_MS // 10s
        }

        pub fn object_store_req_backoff_factor() -> u64 {
            2
        }

        pub fn object_store_upload_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // init + upload_part + finish
        pub fn object_store_streaming_upload_attempt_timeout_ms() -> u64 {
            5 * 1000 // 5s
        }

        pub fn object_store_streaming_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on block_size
        pub fn object_store_read_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_streaming_read_attempt_timeout_ms() -> u64 {
            3 * 1000 // 3s
        }

        pub fn object_store_streaming_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_metadata_attempt_timeout_ms() -> u64 {
            60 * 1000 // 1min
        }

        pub fn object_store_metadata_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_delete_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on batch size
        pub fn object_store_delete_objects_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_objects_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_list_attempt_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn object_store_list_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub mod s3 {
            const DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S: u64 = 5;

            const DEFAULT_KEEPALIVE_MS: u64 = 600 * 1000; // 10min

            pub fn object_store_keepalive_ms() -> Option<u64> {
                Some(DEFAULT_KEEPALIVE_MS) // 10min
            }

            pub fn object_store_recv_buffer_size() -> Option<usize> {
                Some(1 << 21) // 2m
            }

            pub fn object_store_send_buffer_size() -> Option<usize> {
                None
            }

            pub fn object_store_nodelay() -> Option<bool> {
                Some(true)
            }

            pub fn identity_resolution_timeout_s() -> u64 {
                DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S
            }

            pub mod developer {
                use crate::util::env_var::env_var_is_true_or;
                const RW_USE_OPENDAL_FOR_S3: &str = "RW_USE_OPENDAL_FOR_S3";

                pub fn object_store_retry_unknown_service_error() -> bool {
                    false
                }

                pub fn object_store_retryable_service_error_codes() -> Vec<String> {
                    vec!["SlowDown".into(), "TooManyRequests".into()]
                }

                pub fn use_opendal() -> bool {
                    // TODO: deprecate this config when we are completely switch from aws sdk to opendal.
                    // The reason why we use !env_var_is_false_or(RW_USE_OPENDAL_FOR_S3, false) here is
                    // 1. Maintain compatibility so that there is no behavior change in cluster with RW_USE_OPENDAL_FOR_S3 set.
                    // 2. Change the default behavior to use opendal for s3 if RW_USE_OPENDAL_FOR_S3 is not set.
                    env_var_is_true_or(RW_USE_OPENDAL_FOR_S3, false)
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum EvictionConfig {
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

impl EvictionConfig {
    pub fn for_test() -> Self {
        Self::Lru(LruConfig {
            high_priority_pool_ratio: 0.0,
        })
    }
}

pub struct StorageMemoryConfig {
    pub block_cache_capacity_mb: usize,
    pub block_cache_shard_num: usize,
    pub meta_cache_capacity_mb: usize,
    pub meta_cache_shard_num: usize,
    pub shared_buffer_capacity_mb: usize,
    pub compactor_memory_limit_mb: usize,
    pub prefetch_buffer_capacity_mb: usize,
    pub block_cache_eviction_config: EvictionConfig,
    pub meta_cache_eviction_config: EvictionConfig,
}

pub const MAX_META_CACHE_SHARD_BITS: usize = 4;
pub const MIN_BUFFER_SIZE_PER_SHARD: usize = 256;
pub const MAX_BLOCK_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.

pub fn extract_storage_memory_config(s: &RwConfig) -> StorageMemoryConfig {
    let block_cache_capacity_mb = s.storage.cache.block_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::block_cache_capacity_mb()),
    );
    let meta_cache_capacity_mb = s.storage.cache.meta_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::meta_cache_capacity_mb()),
    );
    let shared_buffer_capacity_mb = s
        .storage
        .shared_buffer_capacity_mb
        .unwrap_or(default::storage::shared_buffer_capacity_mb());
    let meta_cache_shard_num = s.storage.cache.meta_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        shard_bits
    });
    let block_cache_shard_num = s.storage.cache.block_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_BLOCK_CACHE_SHARD_BITS;
        while (block_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0
        {
            shard_bits -= 1;
        }
        shard_bits
    });
    let compactor_memory_limit_mb = s
        .storage
        .compactor_memory_limit_mb
        .unwrap_or(default::storage::compactor_memory_limit_mb());

    let get_eviction_config = |c: &CacheEvictionConfig| {
        match c {
            CacheEvictionConfig::Lru {
                high_priority_ratio_in_percent,
            } => EvictionConfig::Lru(LruConfig {
                high_priority_pool_ratio: high_priority_ratio_in_percent.unwrap_or(
                    // adapt to old version
                    s.storage
                        .high_priority_ratio_in_percent
                        .unwrap_or(default::storage::high_priority_ratio_in_percent()),
                ) as f64
                    / 100.0,
            }),
            CacheEvictionConfig::Lfu {
                window_capacity_ratio_in_percent,
                protected_capacity_ratio_in_percent,
                cmsketch_eps,
                cmsketch_confidence,
            } => EvictionConfig::Lfu(LfuConfig {
                window_capacity_ratio: window_capacity_ratio_in_percent
                    .unwrap_or(default::storage::window_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                protected_capacity_ratio: protected_capacity_ratio_in_percent
                    .unwrap_or(default::storage::protected_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }),
            CacheEvictionConfig::S3Fifo {
                small_queue_capacity_ratio_in_percent,
            } => EvictionConfig::S3Fifo(S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio_in_percent
                    .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
            }),
        }
    };

    let block_cache_eviction_config = get_eviction_config(&s.storage.cache.block_cache_eviction);
    let meta_cache_eviction_config = get_eviction_config(&s.storage.cache.meta_cache_eviction);

    let prefetch_buffer_capacity_mb =
        s.storage
            .shared_buffer_capacity_mb
            .unwrap_or(match &block_cache_eviction_config {
                EvictionConfig::Lru(lru) => {
                    ((1.0 - lru.high_priority_pool_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::Lfu(lfu) => {
                    ((1.0 - lfu.protected_capacity_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::S3Fifo(s3fifo) => {
                    (s3fifo.small_queue_capacity_ratio * block_cache_capacity_mb as f64) as usize
                }
            });

    StorageMemoryConfig {
        block_cache_capacity_mb,
        block_cache_shard_num,
        meta_cache_capacity_mb,
        meta_cache_shard_num,
        shared_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        block_cache_eviction_config,
        meta_cache_eviction_config,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CompactionConfig {
    #[serde(default = "default::compaction_config::max_bytes_for_level_base")]
    pub max_bytes_for_level_base: u64,
    #[serde(default = "default::compaction_config::max_bytes_for_level_multiplier")]
    pub max_bytes_for_level_multiplier: u64,
    #[serde(default = "default::compaction_config::max_compaction_bytes")]
    pub max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::sub_level_max_compaction_bytes")]
    pub sub_level_max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::level0_tier_compact_file_number")]
    pub level0_tier_compact_file_number: u64,
    #[serde(default = "default::compaction_config::target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default::compaction_config::compaction_filter_mask")]
    pub compaction_filter_mask: u32,
    #[serde(default = "default::compaction_config::max_sub_compaction")]
    pub max_sub_compaction: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_sub_level_number")]
    pub level0_stop_write_threshold_sub_level_number: u64,
    #[serde(default = "default::compaction_config::level0_sub_level_compact_level_count")]
    pub level0_sub_level_compact_level_count: u32,
    #[serde(
        default = "default::compaction_config::level0_overlapping_sub_level_compact_level_count"
    )]
    pub level0_overlapping_sub_level_compact_level_count: u32,
    #[serde(default = "default::compaction_config::max_space_reclaim_bytes")]
    pub max_space_reclaim_bytes: u64,
    #[serde(default = "default::compaction_config::level0_max_compact_file_number")]
    pub level0_max_compact_file_number: u64,
    #[serde(default = "default::compaction_config::tombstone_reclaim_ratio")]
    pub tombstone_reclaim_ratio: u32,
    #[serde(default = "default::compaction_config::enable_emergency_picker")]
    pub enable_emergency_picker: bool,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// This test ensures that `config/example.toml` is up-to-date with the default values specified
    /// in this file. Developer should run `./risedev generate-example-config` to update it if this
    /// test fails.
    #[test]
    fn test_example_up_to_date() {
        const HEADER: &str = "# This file is generated by ./risedev generate-example-config
# Check detailed comments in src/common/src/config.rs";

        let actual = expect_test::expect_file!["../../config/example.toml"];
        let default = toml::to_string(&RwConfig::default()).expect("failed to serialize");

        let expected = format!("{HEADER}\n\n{default}");
        actual.assert_eq(&expected);

        let expected = rw_config_to_markdown();
        let actual = expect_test::expect_file!["../../config/docs.md"];
        actual.assert_eq(&expected);
    }

    #[derive(Debug)]
    struct ConfigItemDoc {
        desc: String,
        default: String,
    }

    fn rw_config_to_markdown() -> String {
        let mut config_rustdocs = BTreeMap::<String, Vec<(String, String)>>::new();
        RwConfig::config_docs("".to_string(), &mut config_rustdocs);

        // Section -> Config Name -> ConfigItemDoc
        let mut configs: BTreeMap<String, BTreeMap<String, ConfigItemDoc>> = config_rustdocs
            .into_iter()
            .map(|(k, v)| {
                let docs: BTreeMap<String, ConfigItemDoc> = v
                    .into_iter()
                    .map(|(name, desc)| {
                        (
                            name,
                            ConfigItemDoc {
                                desc,
                                default: "".to_string(), // unset
                            },
                        )
                    })
                    .collect();
                (k, docs)
            })
            .collect();

        let toml_doc: BTreeMap<String, toml::Value> =
            toml::from_str(&toml::to_string(&RwConfig::default()).unwrap()).unwrap();
        toml_doc.into_iter().for_each(|(name, value)| {
            set_default_values("".to_string(), name, value, &mut configs);
        });

        let mut markdown = "# RisingWave System Configurations\n\n".to_string()
            + "This page is automatically generated by `./risedev generate-example-config`\n";
        for (section, configs) in configs {
            if configs.is_empty() {
                continue;
            }
            markdown.push_str(&format!("\n## {}\n\n", section));
            markdown.push_str("| Config | Description | Default |\n");
            markdown.push_str("|--------|-------------|---------|\n");
            for (config, doc) in configs {
                markdown.push_str(&format!(
                    "| {} | {} | {} |\n",
                    config, doc.desc, doc.default
                ));
            }
        }
        markdown
    }

    fn set_default_values(
        section: String,
        name: String,
        value: toml::Value,
        configs: &mut BTreeMap<String, BTreeMap<String, ConfigItemDoc>>,
    ) {
        // Set the default value if it's a config name-value pair, otherwise it's a sub-section (Table) that should be recursively processed.
        if let toml::Value::Table(table) = value {
            let section_configs: BTreeMap<String, toml::Value> =
                table.clone().into_iter().collect();
            let sub_section = if section.is_empty() {
                name
            } else {
                format!("{}.{}", section, name)
            };
            section_configs
                .into_iter()
                .for_each(|(k, v)| set_default_values(sub_section.clone(), k, v, configs))
        } else if let Some(t) = configs.get_mut(&section) {
            if let Some(item_doc) = t.get_mut(&name) {
                item_doc.default = format!("{}", value);
            }
        }
    }
}
