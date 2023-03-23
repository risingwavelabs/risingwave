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

use std::collections::HashMap;
use std::fs;

use clap::ValueEnum;
use risingwave_pb::meta::SystemParams;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB
/// For non-user-facing components where the CLI arguments do not override the config file.
pub const NO_OVERRIDE: Option<NoOverride> = None;

macro_rules! for_all_config_sections {
    ($macro:ident) => {
        $macro! {
            { server },
            { meta },
            { batch },
            { streaming },
            { storage },
            { storage.file_cache },
        }
    };
}

macro_rules! impl_warn_unrecognized_fields {
    ($({ $($field_path:ident).+ },)*) => {
        fn warn_unrecognized_fields(config: &RwConfig) {
            if !config.unrecognized.is_empty() {
                tracing::warn!("unrecognized fields in config: {:?}", config.unrecognized.keys());
            }
            $(
                if !config.$($field_path).+.unrecognized.is_empty() {
                    tracing::warn!("unrecognized fields in config section [{}]: {:?}", stringify!($($field_path).+), config.$($field_path).+.unrecognized.keys());
                }
            )*
        }
    };
}

for_all_config_sections!(impl_warn_unrecognized_fields);

pub fn load_config(path: &str, cli_override: Option<impl OverrideConfig>) -> RwConfig
where
{
    let mut config = if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        RwConfig::default()
    } else {
        let config_str = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to open config file '{}': {}", path, e));
        toml::from_str(config_str.as_str()).unwrap_or_else(|e| panic!("parse error {}", e))
    };
    if let Some(cli_override) = cli_override {
        cli_override.r#override(&mut config);
    }
    warn_unrecognized_fields(&config);
    config
}

pub trait OverrideConfig {
    fn r#override(self, config: &mut RwConfig);
}

/// A dummy struct for `NO_OVERRIDE`. Do NOT use it directly.
#[derive(Clone, Copy)]
pub struct NoOverride {}

impl OverrideConfig for NoOverride {
    fn r#override(self, _config: &mut RwConfig) {}
}

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
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
    pub system: SystemConfig,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    #[default]
    Mem,
    Etcd,
}

/// The section `[meta]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaConfig {
    /// Threshold used by worker node to filter out new SSTs when scanning object store, during
    /// full SST GC.
    #[serde(default = "default::meta::min_sst_retention_time_sec")]
    pub min_sst_retention_time_sec: u64,

    /// The spin interval when collecting global GC watermark in hummock
    #[serde(default = "default::meta::collect_gc_watermark_spin_interval_sec")]
    pub collect_gc_watermark_spin_interval_sec: u64,

    /// Schedule compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_compaction_interval_sec")]
    pub periodic_compaction_interval_sec: u64,

    /// Interval of GC metadata in meta store and stale SSTs in object store.
    #[serde(default = "default::meta::vacuum_interval_sec")]
    pub vacuum_interval_sec: u64,

    /// Maximum allowed heartbeat interval in seconds.
    #[serde(default = "default::meta::max_heartbeat_interval_sec")]
    pub max_heartbeat_interval_secs: u32,

    /// Whether to enable fail-on-recovery. Should only be used in e2e tests.
    #[serde(default)]
    pub disable_recovery: bool,

    #[serde(default = "default::meta::meta_leader_lease_secs")]
    pub meta_leader_lease_secs: u64,

    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// It is mainly useful for playgrounds.
    pub dangerous_max_idle_secs: Option<u64>,

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

    #[serde(default = "default::meta::periodic_split_compact_group_interval_sec")]
    pub periodic_split_compact_group_interval_sec: u64,

    /// Compute compactor_task_limit for machines with different hardware.Currently cpu is used as
    /// the main consideration,and is adjusted by max_compactor_task_multiplier, calculated as
    /// compactor_task_limit = core_num * max_compactor_task_multiplier;
    #[serde(default = "default::meta::max_compactor_task_multiplier")]
    pub max_compactor_task_multiplier: u32,

    #[serde(default = "default::meta::split_check_size_limit")]
    pub split_check_size_limit: u64,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for MetaConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::server::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    /// The maximum allowed heartbeat interval for workers.
    #[serde(default = "default::server::max_heartbeat_interval_secs")]
    pub max_heartbeat_interval_secs: u32,

    #[serde(default = "default::server::connection_pool_size")]
    pub connection_pool_size: u16,

    #[serde(default = "default::server::metrics_level")]
    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    pub metrics_level: u32,

    #[serde(default = "default::server::telemetry_enabled")]
    pub telemetry_enabled: bool,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[batch]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default)]
    pub developer: DeveloperConfig,

    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[streaming]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// The maximum number of barriers in-flight in the compute nodes.
    #[serde(default = "default::streaming::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    /// Enable reporting tracing information to jaeger.
    #[serde(default = "default::streaming::enable_jaegar_tracing")]
    pub enable_jaeger_tracing: bool,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[serde(default = "default::streaming::async_stack_trace")]
    pub async_stack_trace: AsyncStackTraceOption,

    #[serde(default)]
    pub developer: DeveloperConfig,

    /// Max unique user stream errors per actor
    #[serde(default = "default::streaming::unique_user_stream_errors")]
    pub unique_user_stream_errors: usize,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[storage]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    #[serde(default = "default::storage::shared_buffer_capacity_mb")]
    pub shared_buffer_capacity_mb: usize,

    /// Whether to enable write conflict detection
    #[serde(default = "default::storage::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    /// Capacity of sstable block cache.
    #[serde(default = "default::storage::block_cache_capacity_mb")]
    pub block_cache_capacity_mb: usize,

    /// Capacity of sstable meta cache.
    #[serde(default = "default::storage::meta_cache_capacity_mb")]
    pub meta_cache_capacity_mb: usize,

    #[serde(default = "default::storage::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    #[serde(default = "default::storage::enable_local_spill")]
    pub enable_local_spill: bool,

    /// Local object store root. We should call `get_local_object_store` to get the object store.
    #[serde(default = "default::storage::local_object_store")]
    pub local_object_store: String,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::storage::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    /// Capacity of sstable meta cache.
    #[serde(default = "default::storage::compactor_memory_limit_mb")]
    pub compactor_memory_limit_mb: usize,

    /// Number of SST ids fetched from meta per RPC
    #[serde(default = "default::storage::sstable_id_remote_fetch_number")]
    pub sstable_id_remote_fetch_number: u32,

    #[serde(default)]
    pub file_cache: FileCacheConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::storage::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    /// Max sub compaction task numbers
    #[serde(default = "default::storage::max_sub_compaction")]
    pub max_sub_compaction: u32,

    #[serde(default = "default::storage::max_concurrent_compaction_task_number")]
    pub max_concurrent_compaction_task_number: u64,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The subsection `[storage.file_cache]` in `risingwave.toml`.
///
/// It's put at [`StorageConfig::file_cache`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache::dir")]
    pub dir: String,

    #[serde(default = "default::file_cache::capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache::total_buffer_capacity_mb")]
    pub total_buffer_capacity_mb: usize,

    #[serde(default = "default::file_cache::cache_file_fallocate_unit_mb")]
    pub cache_file_fallocate_unit_mb: usize,

    #[serde(default = "default::file_cache::cache_meta_fallocate_unit_mb")]
    pub cache_meta_fallocate_unit_mb: usize,

    #[serde(default = "default::file_cache::cache_file_max_write_size_mb")]
    pub cache_file_max_write_size_mb: usize,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Debug, Default, Clone, ValueEnum, Serialize, Deserialize)]
pub enum AsyncStackTraceOption {
    Off,
    #[default]
    On,
    Verbose,
}

/// The subsections `[batch.developer]` and `[streaming.developer]`.
///
/// It is put at [`BatchConfig::developer`] and [`StreamingConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeveloperConfig {
    /// The size of the channel used for output to exchange/shuffle.
    #[serde(default = "default::developer::batch_output_channel_size")]
    pub batch_output_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    #[serde(default = "default::developer::batch_chunk_size")]
    pub batch_chunk_size: usize,

    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    #[serde(default = "default::developer::stream_enable_executor_row_count")]
    pub stream_enable_executor_row_count: bool,

    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::stream_connector_message_buffer_size")]
    pub stream_connector_message_buffer_size: usize,

    /// Limit number of the cached entries in an extreme aggregation call.
    #[serde(default = "default::developer::unsafe_stream_extreme_cache_size")]
    pub unsafe_stream_extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    #[serde(default = "default::developer::stream_chunk_size")]
    pub stream_chunk_size: usize,

    /// The initial permits that a channel holds, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_exchange_initial_permits")]
    pub stream_exchange_initial_permits: usize,

    /// The permits that are batched to add back, for reducing the backward `AddPermits` messages
    /// in remote exchange.
    #[serde(default = "default::developer::stream_exchange_batched_permits")]
    pub stream_exchange_batched_permits: usize,

    #[serde(flatten)]
    pub unrecognized: HashMap<String, Value>,
}

impl Default for DeveloperConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[system]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SystemConfig {
    /// The interval of periodic barrier.
    #[serde(default = "default::system::barrier_interval_ms")]
    pub barrier_interval_ms: u32,

    /// There will be a checkpoint for every n barriers
    #[serde(default = "default::system::checkpoint_frequency")]
    pub checkpoint_frequency: u64,

    /// Target size of the Sstable.
    #[serde(default = "default::system::sstable_size_mb")]
    pub sstable_size_mb: u32,

    /// Size of each block in bytes in SST.
    #[serde(default = "default::system::block_size_kb")]
    pub block_size_kb: u32,

    /// False positive probability of bloom filter.
    #[serde(default = "default::system::bloom_false_positive")]
    pub bloom_false_positive: f64,

    #[serde(default = "default::system::state_store")]
    pub state_store: String,

    /// Remote directory for storing data and metadata objects.
    #[serde(default = "default::system::data_directory")]
    pub data_directory: String,

    /// Remote storage url for storing snapshots.
    #[serde(default = "default::system::backup_storage_url")]
    pub backup_storage_url: String,

    /// Remote directory for storing snapshots.
    #[serde(default = "default::system::backup_storage_directory")]
    pub backup_storage_directory: String,

    #[serde(default = "default::system::telemetry_enabled")]
    pub telemetry_enabled: bool,
}

impl Default for SystemConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

impl SystemConfig {
    pub fn into_init_system_params(self) -> SystemParams {
        SystemParams {
            barrier_interval_ms: Some(self.barrier_interval_ms),
            checkpoint_frequency: Some(self.checkpoint_frequency),
            sstable_size_mb: Some(self.sstable_size_mb),
            block_size_kb: Some(self.block_size_kb),
            bloom_false_positive: Some(self.bloom_false_positive),
            state_store: Some(self.state_store),
            data_directory: Some(self.data_directory),
            backup_storage_url: Some(self.backup_storage_url),
            backup_storage_directory: Some(self.backup_storage_directory),
            telemetry_enabled: Some(self.telemetry_enabled),
        }
    }
}

mod default {
    pub mod meta {
        use crate::config::MetaBackend;

        pub fn min_sst_retention_time_sec() -> u64 {
            604800
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

        pub fn max_heartbeat_interval_sec() -> u32 {
            300
        }

        pub fn meta_leader_lease_secs() -> u64 {
            10
        }

        pub fn node_num_monitor_interval_sec() -> u64 {
            10
        }

        pub fn backend() -> MetaBackend {
            MetaBackend::Mem
        }

        pub fn periodic_space_reclaim_compaction_interval_sec() -> u64 {
            300 // 5min
        }

        pub fn periodic_ttl_reclaim_compaction_interval_sec() -> u64 {
            1800 // 30mi
        }

        pub fn periodic_split_compact_group_interval_sec() -> u64 {
            1800 // 30mi
        }

        pub fn max_compactor_task_multiplier() -> u32 {
            2
        }

        pub fn split_check_size_limit() -> u64 {
            64 * 1024 * 1024 * 1024 // 64GB
        }
    }

    pub mod server {

        pub fn heartbeat_interval_ms() -> u32 {
            1000
        }

        pub fn max_heartbeat_interval_secs() -> u32 {
            600
        }

        pub fn connection_pool_size() -> u16 {
            16
        }

        pub fn metrics_level() -> u32 {
            0
        }

        pub fn telemetry_enabled() -> bool {
            true
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

        pub fn write_conflict_detection_enabled() -> bool {
            cfg!(debug_assertions)
        }

        pub fn block_cache_capacity_mb() -> usize {
            512
        }

        pub fn meta_cache_capacity_mb() -> usize {
            128
        }

        pub fn disable_remote_compactor() -> bool {
            false
        }

        pub fn enable_local_spill() -> bool {
            true
        }

        pub fn local_object_store() -> String {
            "tempdisk".to_string()
        }

        pub fn share_buffer_upload_concurrency() -> usize {
            8
        }

        pub fn compactor_memory_limit_mb() -> usize {
            512
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
    }

    pub mod streaming {
        use crate::config::AsyncStackTraceOption;

        pub fn in_flight_barrier_nums() -> usize {
            // quick fix
            // TODO: remove this limitation from code
            10000
        }

        pub fn enable_jaegar_tracing() -> bool {
            false
        }

        pub fn async_stack_trace() -> AsyncStackTraceOption {
            AsyncStackTraceOption::On
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

        pub fn total_buffer_capacity_mb() -> usize {
            128
        }

        pub fn cache_file_fallocate_unit_mb() -> usize {
            512
        }

        pub fn cache_meta_fallocate_unit_mb() -> usize {
            16
        }

        pub fn cache_file_max_write_size_mb() -> usize {
            4
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

        pub fn stream_connector_message_buffer_size() -> usize {
            16
        }

        pub fn unsafe_stream_extreme_cache_size() -> usize {
            1 << 10
        }

        pub fn stream_chunk_size() -> usize {
            1024
        }

        pub fn stream_exchange_initial_permits() -> usize {
            8192
        }

        pub fn stream_exchange_batched_permits() -> usize {
            1024
        }
    }

    pub mod system {
        use crate::system_param;

        pub fn barrier_interval_ms() -> u32 {
            system_param::default::barrier_interval_ms()
        }

        pub fn checkpoint_frequency() -> u64 {
            system_param::default::checkpoint_frequency()
        }

        pub fn sstable_size_mb() -> u32 {
            system_param::default::sstable_size_mb()
        }

        pub fn block_size_kb() -> u32 {
            system_param::default::block_size_kb()
        }

        pub fn bloom_false_positive() -> f64 {
            system_param::default::bloom_false_positive()
        }

        pub fn state_store() -> String {
            system_param::default::state_store()
        }

        pub fn data_directory() -> String {
            system_param::default::data_directory()
        }

        pub fn backup_storage_url() -> String {
            system_param::default::backup_storage_url()
        }

        pub fn backup_storage_directory() -> String {
            system_param::default::backup_storage_directory()
        }

        pub fn telemetry_enabled() -> bool {
            system_param::default::telemetry_enabled()
        }
    }
}
