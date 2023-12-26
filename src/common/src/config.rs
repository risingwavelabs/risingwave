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

//! This module defines the structure of the configuration file `risingwave.toml`.
//!
//! [`RwConfig`] corresponds to the whole config file and each other config struct corresponds to a
//! section in `risingwave.toml`.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;

use anyhow::Context;
use clap::ValueEnum;
use educe::Educe;
pub use risingwave_common_proc_macro::OverrideConfig;
use risingwave_pb::meta::SystemParams;
use serde::{Deserialize, Serialize, Serializer};
use serde_default::DefaultFromSerde;
use serde_json::Value;

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
#[derive(Educe, Clone, Serialize, Deserialize, Default)]
#[educe(Debug)]
pub struct RwConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub meta: MetaConfig,

    #[serde(default)]
    pub batch: BatchConfig,

    #[serde(default)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    #[educe(Debug(ignore))]
    pub system: SystemConfig,

    #[serde(flatten)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    #[default]
    Mem,
    Etcd,
}

/// The section `[meta]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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

    /// Whether to enable scale-in when recovery.
    #[serde(default)]
    pub enable_scale_in_when_recovery: bool,

    /// Whether to enable auto-scaling feature.
    #[serde(default)]
    pub enable_automatic_parallelism_control: bool,

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

    /// Schedule space_reclaim compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_space_reclaim_compaction_interval_sec")]
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// Schedule ttl_reclaim compaction for all compaction groups with this interval.
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

    #[serde(default = "default::meta::compaction_task_max_heartbeat_interval_secs")]
    // If the compaction task does not change in progress beyond the
    // `compaction_task_max_heartbeat_interval_secs` interval, we will cancel the task
    pub compaction_task_max_heartbeat_interval_secs: u64,

    #[serde(default)]
    pub compaction_config: CompactionConfig,

    #[serde(default = "default::meta::hybird_partition_vnode_count")]
    pub hybird_partition_vnode_count: u32,
    #[serde(default = "default::meta::event_log_enabled")]
    pub event_log_enabled: bool,
    /// Keeps the latest N events per channel.
    #[serde(default = "default::meta::event_log_channel_max_size")]
    pub event_log_channel_max_size: u32,
}

#[derive(Clone, Debug, Default)]
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
                NonZeroUsize::new(i)
                    .context("default parallelism should be greater than 0")
                    .map_err(|e| serde::de::Error::custom(e.to_string()))?
            })),
        }
    }
}

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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
    pub unrecognized: Unrecognized<Self>,
}

/// The section `[batch]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default, with = "batch_prefix")]
    pub developer: BatchDeveloperConfig,

    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    #[serde(default = "default::batch::enable_barrier_read")]
    pub enable_barrier_read: bool,

    /// Timeout for a batch query in seconds.
    #[serde(default = "default::batch::statement_timeout_in_sec")]
    pub statement_timeout_in_sec: u32,

    #[serde(default, flatten)]
    pub unrecognized: Unrecognized<Self>,
}

/// The section `[streaming]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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
    pub developer: StreamingDeveloperConfig,

    /// Max unique user stream errors per actor
    #[serde(default = "default::streaming::unique_user_stream_errors")]
    pub unique_user_stream_errors: usize,

    #[serde(default, flatten)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
pub enum MetricLevel {
    #[default]
    Disabled = 0,
    Critical = 1,
    Info = 2,
    Debug = 3,
}

impl clap::ValueEnum for MetricLevel {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Disabled, Self::Critical, Self::Info, Self::Debug]
    }

    fn to_possible_value<'a>(&self) -> ::std::option::Option<clap::builder::PossibleValue> {
        match self {
            Self::Disabled => Some(clap::builder::PossibleValue::new("disabled").alias("0")),
            Self::Critical => Some(clap::builder::PossibleValue::new("critical")),
            Self::Info => Some(clap::builder::PossibleValue::new("info").alias("1")),
            Self::Debug => Some(clap::builder::PossibleValue::new("debug")),
        }
    }
}

impl PartialEq<Self> for MetricLevel {
    fn eq(&self, other: &Self) -> bool {
        (*self as u8).eq(&(*other as u8))
    }
}

impl PartialOrd for MetricLevel {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (*self as u8).partial_cmp(&(*other as u8))
    }
}

/// The section `[storage]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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

    /// Capacity of sstable block cache.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    #[serde(default)]
    pub high_priority_ratio_in_percent: Option<usize>,

    /// Capacity of sstable meta cache.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// max memory usage for large query
    #[serde(default)]
    pub prefetch_buffer_capacity_mb: Option<usize>,

    #[serde(default = "default::storage::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::storage::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    #[serde(default)]
    pub compactor_memory_limit_mb: Option<usize>,

    /// Compactor calculates the maximum number of tasks that can be executed on the node based on
    /// worker_num and compactor_max_task_multiplier.
    /// max_pull_task_count = worker_num * compactor_max_task_multiplier
    #[serde(default = "default::storage::compactor_max_task_multiplier")]
    pub compactor_max_task_multiplier: f32,

    /// The percentage of memory available when compactor is deployed separately.
    /// non_reserved_memory_bytes = system_memory_available_bytes * compactor_memory_available_proportion
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
    #[serde(default = "default::storage::compact_iter_recreate_timeout_ms")]
    pub compact_iter_recreate_timeout_ms: u64,
    #[serde(default = "default::storage::compactor_max_sst_size")]
    pub compactor_max_sst_size: u64,
    #[serde(default = "default::storage::enable_fast_compaction")]
    pub enable_fast_compaction: bool,
    #[serde(default = "default::storage::max_preload_io_retry_times")]
    pub max_preload_io_retry_times: usize,

    #[serde(default = "default::storage::compactor_fast_max_compact_delete_ratio")]
    pub compactor_fast_max_compact_delete_ratio: u32,

    #[serde(default = "default::storage::compactor_fast_max_compact_task_size")]
    pub compactor_fast_max_compact_task_size: u64,

    #[serde(default, flatten)]
    pub unrecognized: Unrecognized<Self>,

    /// The spill threshold for mem table.
    #[serde(default = "default::storage::mem_table_spill_threshold")]
    pub mem_table_spill_threshold: usize,

    #[serde(default)]
    pub object_store: ObjectStoreConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct CacheRefillConfig {
    /// SSTable levels to refill.
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
    pub unrecognized: Unrecognized<Self>,
}

/// The subsection `[storage.data_file_cache]` and `[storage.meta_file_cache]` in `risingwave.toml`.
///
/// It's put at [`StorageConfig::data_file_cache`] and  [`StorageConfig::meta_file_cache`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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

    #[serde(default = "default::file_cache::ring_buffer_capacity_mb")]
    pub ring_buffer_capacity_mb: usize,

    #[serde(default = "default::file_cache::catalog_bits")]
    pub catalog_bits: usize,

    #[serde(default = "default::file_cache::compression")]
    pub compression: String,

    #[serde(default, flatten)]
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

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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

serde_with::with_prefix!(streaming_prefix "stream_");
serde_with::with_prefix!(batch_prefix "batch_");

/// The subsections `[streaming.developer]`.
///
/// It is put at [`StreamingConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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
}

/// The subsections `[batch.developer]`.
///
/// It is put at [`BatchConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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
/// The section `[system]` in `risingwave.toml`. All these fields are used to initialize the system
/// parameters persisted in Meta store. Most fields are for testing purpose only and should not be
/// documented.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct SystemConfig {
    /// The interval of periodic barrier.
    #[serde(default = "default::system::barrier_interval_ms")]
    pub barrier_interval_ms: Option<u32>,

    /// There will be a checkpoint for every n barriers
    #[serde(default = "default::system::checkpoint_frequency")]
    pub checkpoint_frequency: Option<u64>,

    /// Target size of the Sstable.
    #[serde(default = "default::system::sstable_size_mb")]
    pub sstable_size_mb: Option<u32>,

    #[serde(default = "default::system::parallel_compact_size_mb")]
    pub parallel_compact_size_mb: Option<u32>,

    /// Size of each block in bytes in SST.
    #[serde(default = "default::system::block_size_kb")]
    pub block_size_kb: Option<u32>,

    /// False positive probability of bloom filter.
    #[serde(default = "default::system::bloom_false_positive")]
    pub bloom_false_positive: Option<f64>,

    #[serde(default = "default::system::state_store")]
    pub state_store: Option<String>,

    /// Remote directory for storing data and metadata objects.
    #[serde(default = "default::system::data_directory")]
    pub data_directory: Option<String>,

    /// Remote storage url for storing snapshots.
    #[serde(default = "default::system::backup_storage_url")]
    pub backup_storage_url: Option<String>,

    /// Remote directory for storing snapshots.
    #[serde(default = "default::system::backup_storage_directory")]
    pub backup_storage_directory: Option<String>,

    /// Max number of concurrent creating streaming jobs.
    #[serde(default = "default::system::max_concurrent_creating_streaming_jobs")]
    pub max_concurrent_creating_streaming_jobs: Option<u32>,

    /// Whether to pause all data sources on next bootstrap.
    #[serde(default = "default::system::pause_on_next_bootstrap")]
    pub pause_on_next_bootstrap: Option<bool>,
}

/// The subsections `[storage.object_store]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreConfig {
    #[serde(default = "default::object_store_config::object_store_streaming_read_timeout_ms")]
    pub object_store_streaming_read_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_streaming_upload_timeout_ms")]
    pub object_store_streaming_upload_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_upload_timeout_ms")]
    pub object_store_upload_timeout_ms: u64,
    #[serde(default = "default::object_store_config::object_store_read_timeout_ms")]
    pub object_store_read_timeout_ms: u64,

    #[serde(default)]
    pub s3: S3ObjectStoreConfig,
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
    #[serde(default = "default::object_store_config::s3::object_store_req_retry_interval_ms")]
    pub object_store_req_retry_interval_ms: u64,
    #[serde(default = "default::object_store_config::s3::object_store_req_retry_max_delay_ms")]
    pub object_store_req_retry_max_delay_ms: u64,
    #[serde(default = "default::object_store_config::s3::object_store_req_retry_max_attempts")]
    pub object_store_req_retry_max_attempts: usize,
}

impl SystemConfig {
    #![allow(deprecated)]
    pub fn into_init_system_params(self) -> SystemParams {
        SystemParams {
            barrier_interval_ms: self.barrier_interval_ms,
            checkpoint_frequency: self.checkpoint_frequency,
            sstable_size_mb: self.sstable_size_mb,
            parallel_compact_size_mb: self.parallel_compact_size_mb,
            block_size_kb: self.block_size_kb,
            bloom_false_positive: self.bloom_false_positive,
            state_store: self.state_store,
            data_directory: self.data_directory,
            backup_storage_url: self.backup_storage_url,
            backup_storage_directory: self.backup_storage_directory,
            max_concurrent_creating_streaming_jobs: self.max_concurrent_creating_streaming_jobs,
            pause_on_next_bootstrap: self.pause_on_next_bootstrap,
            telemetry_enabled: None, // deprecated
        }
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

        pub fn min_delta_log_num_for_hummock_version_checkpoint() -> u64 {
            10
        }

        pub fn max_heartbeat_interval_sec() -> u32 {
            300
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
            64
        }

        pub fn table_write_throughput_threshold() -> u64 {
            16 * 1024 * 1024 // 16MB
        }

        pub fn min_table_split_write_throughput() -> u64 {
            4 * 1024 * 1024 // 4MB
        }

        pub fn compaction_task_max_heartbeat_interval_secs() -> u64 {
            60 // 1min
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
            4
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
            1.5000
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

        pub fn compactor_max_sst_size() -> u64 {
            512 * 1024 * 1024 // 512m
        }

        pub fn enable_fast_compaction() -> bool {
            true
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

        pub fn ring_buffer_capacity_mb() -> usize {
            256
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
    }

    pub mod system {
        pub use crate::system_param::default::*;
    }

    pub mod batch {
        pub fn enable_barrier_read() -> bool {
            false
        }

        pub fn statement_timeout_in_sec() -> u32 {
            // 1 hour
            60 * 60
        }
    }

    pub mod compaction_config {
        const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB
        const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * 1024 * 1024; // 128MB
        const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * 1024 * 1024; // 512MB

        // decrease this configure when the generation of checkpoint barrier is not frequent.
        const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 6;
        const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 32 * 1024 * 1024; // 32MB
        const DEFAULT_MAX_SUB_COMPACTION: u32 = 4;
        const DEFAULT_LEVEL_MULTIPLIER: u64 = 5;
        const DEFAULT_MAX_SPACE_RECLAIM_BYTES: u64 = 512 * 1024 * 1024; // 512MB;
        const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER: u64 = 300;
        const DEFAULT_MAX_COMPACTION_FILE_COUNT: u64 = 96;
        const DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 3;
        const DEFAULT_MIN_OVERLAPPING_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 6;
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
        pub fn object_store_streaming_read_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn object_store_streaming_upload_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn object_store_upload_timeout_ms() -> u64 {
            60 * 60 * 1000
        }

        pub fn object_store_read_timeout_ms() -> u64 {
            60 * 60 * 1000
        }

        pub mod s3 {
            /// Retry config for compute node http timeout error.
            const DEFAULT_RETRY_INTERVAL_MS: u64 = 20;
            const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 10 * 1000;
            const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 8;

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

            pub fn object_store_req_retry_interval_ms() -> u64 {
                DEFAULT_RETRY_INTERVAL_MS
            }

            pub fn object_store_req_retry_max_delay_ms() -> u64 {
                DEFAULT_RETRY_MAX_DELAY_MS // 10s
            }

            pub fn object_store_req_retry_max_attempts() -> usize {
                DEFAULT_RETRY_MAX_ATTEMPTS
            }
        }
    }
}

pub struct StorageMemoryConfig {
    pub block_cache_capacity_mb: usize,
    pub meta_cache_capacity_mb: usize,
    pub shared_buffer_capacity_mb: usize,
    pub data_file_cache_ring_buffer_capacity_mb: usize,
    pub meta_file_cache_ring_buffer_capacity_mb: usize,
    pub compactor_memory_limit_mb: usize,
    pub prefetch_buffer_capacity_mb: usize,
    pub high_priority_ratio_in_percent: usize,
}

pub fn extract_storage_memory_config(s: &RwConfig) -> StorageMemoryConfig {
    let block_cache_capacity_mb = s
        .storage
        .block_cache_capacity_mb
        .unwrap_or(default::storage::block_cache_capacity_mb());
    let meta_cache_capacity_mb = s
        .storage
        .meta_cache_capacity_mb
        .unwrap_or(default::storage::meta_cache_capacity_mb());
    let shared_buffer_capacity_mb = s
        .storage
        .shared_buffer_capacity_mb
        .unwrap_or(default::storage::shared_buffer_capacity_mb());
    let data_file_cache_ring_buffer_capacity_mb = s.storage.data_file_cache.ring_buffer_capacity_mb;
    let meta_file_cache_ring_buffer_capacity_mb = s.storage.meta_file_cache.ring_buffer_capacity_mb;
    let compactor_memory_limit_mb = s
        .storage
        .compactor_memory_limit_mb
        .unwrap_or(default::storage::compactor_memory_limit_mb());
    let high_priority_ratio_in_percent = s
        .storage
        .high_priority_ratio_in_percent
        .unwrap_or(default::storage::high_priority_ratio_in_percent());
    let prefetch_buffer_capacity_mb = s
        .storage
        .shared_buffer_capacity_mb
        .unwrap_or((100 - high_priority_ratio_in_percent) * block_cache_capacity_mb / 100);

    StorageMemoryConfig {
        block_cache_capacity_mb,
        meta_cache_capacity_mb,
        shared_buffer_capacity_mb,
        data_file_cache_ring_buffer_capacity_mb,
        meta_file_cache_ring_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        high_priority_ratio_in_percent,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
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
    }
}
