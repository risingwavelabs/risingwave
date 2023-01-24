// Copyright 2023 Singularity Data
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

use std::fs;

use clap::ArgEnum;
use serde::{Deserialize, Serialize};

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB
/// For non-user-facing components where the CLI arguments do not override the config file.
pub const NO_OVERWRITE: Option<NoOverwrite> = None;

pub fn load_config(path: &str, cli_overwrite: Option<impl OverwriteConfig>) -> RwConfig
where
{
    if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        return RwConfig::default();
    }
    let config_str = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to open config file '{}': {}", path, e));
    let mut config =
        toml::from_str(config_str.as_str()).unwrap_or_else(|e| panic!("parse error {}", e));
    if let Some(cli_overwrite) = cli_overwrite {
        cli_overwrite.overwrite(&mut config);
    }
    config
}

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RwConfig {
    #[serde(default)]
    pub server: ServerConfig,

    #[serde(default)]
    pub batch: BatchConfig,

    #[serde(default)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub backup: BackupConfig,

    #[serde(default)]
    pub meta: MetaConfig,

    #[serde(default)]
    pub frontend: FrontendConfig,

    #[serde(default)]
    pub compute_node: ComputeNodeConfig,

    #[serde(default)]
    pub compactor: CompactorConfig,
}

pub trait OverwriteConfig {
    fn overwrite(self, config: &mut RwConfig);
}

/// A dummy struct for `NO_OVERWRITE`. Do NOT use it directly.
#[derive(Clone, Copy)]
pub struct NoOverwrite {}

impl OverwriteConfig for NoOverwrite {
    fn overwrite(self, _config: &mut RwConfig) {}
}

#[derive(Copy, Clone, Debug, ArgEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    Mem,
    Etcd,
}

/// The section `[meta]` in `risingwave.toml`. This section only applies to the meta node.
/// A subset of the configs can be overwritten by CLI arguments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaConfig {
    // Below configs are CLI configurable.
    #[serde(default = "default::meta::listen_addr")]
    pub listen_addr: String,

    pub host: Option<String>,

    pub endpoint: Option<String>,

    pub dashboard_host: Option<String>,

    pub prometheus_host: Option<String>,

    #[serde(default = "default::meta::backend")]
    pub backend: MetaBackend,

    #[serde(default = "default::meta::etcd_endpoints")]
    pub etcd_endpoints: String,

    /// Whether to enable authentication with etcd. By default disabled.
    #[serde(default)]
    pub etcd_auth: bool,

    /// Username of etcd, required when --etcd-auth is enabled.
    #[serde(default = "default::meta::etcd_username")]
    pub etcd_username: String,

    /// Password of etcd, required when --etcd-auth is enabled.
    // TODO: it may be unsafe to put password in a file
    #[serde(default = "default::meta::etcd_password")]
    pub etcd_password: String,

    pub dashboard_ui_path: Option<String>,

    /// For dashboard service to fetch cluster info.
    pub prometheus_endpoint: Option<String>,

    /// Endpoint of the connector node, there will be a sidecar connector node
    /// colocated with Meta node in the cloud environment
    pub connector_rpc_endpoint: Option<String>,

    // Below configs are NOT CLI configurable.
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
}

impl Default for MetaConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Copy, Clone, Debug, ArgEnum, Serialize, Deserialize)]
pub enum AsyncStackTraceOption {
    Off,
    On,
    Verbose,
}

/// The section `[frontend]` in `risingwave.toml`. This section only applies to the compactor.
/// A subset of the configs can be overwritten by CLI arguments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrontendConfig {
    // Below configs are CLI configurable.
    #[serde(default = "default::frontend::listen_addr")]
    pub listen_addr: String,

    pub client_address: Option<String>,

    #[serde(default = "default::frontend::meta_address")]
    pub meta_address: String,

    #[serde(default = "default::frontend::prometheus_listen_addr")]
    pub prometheus_listener_addr: String,

    #[serde(default = "default::frontend::health_check_listener_addr")]
    pub health_check_listener_addr: String,

    #[serde(default = "default::frontend::metrics_level")]
    pub metrics_level: u32,
}

impl Default for FrontendConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
/// The section `[coompute_node]` in `risingwave.toml`. This section only applies to the compactor.
/// A subset of the configs can be overwritten by CLI arguments.
pub struct ComputeNodeConfig {
    #[serde(default = "default::compute_node::listen_addr")]
    pub listen_addr: String,

    pub client_address: Option<String>,

    #[serde(default = "default::compute_node::state_store")]
    pub state_store: String,

    #[serde(default = "default::compute_node::prometheus_listen_addr")]
    pub prometheus_listener_addr: String,

    #[serde(default = "default::compute_node::metrics_level")]
    pub metrics_level: u32,

    #[serde(default = "default::compute_node::meta_address")]
    pub meta_address: String,

    /// Enable reporting tracing information to jaeger.
    #[serde(default = "default::compute_node::enable_jaeger_tracing")]
    pub enable_jaeger_tracing: bool,

    /// Enable async stack tracing for risectl.
    #[serde(default = "default::compute_node::async_stack_trace")]
    pub async_stack_trace: AsyncStackTraceOption,

    /// Path to file cache data directory.
    /// Left empty to disable file cache.
    #[serde(default = "default::compute_node::file_cache_dir")]
    pub file_cache_dir: String,

    /// Endpoint of the connector node,
    pub connector_rpc_endpoint: Option<String>,

    /// Total available memory in bytes, used by LRU Manager
    #[serde(default = "default::compute_node::total_memory_bytes")]
    pub total_memory_bytes: usize,

    /// The parallelism that the compute node will register to the scheduler of the meta service.
    #[serde(default = "default::compute_node::parallelism")]
    pub parallelism: usize,
}

impl Default for ComputeNodeConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[compactor]` in `risingwave.toml`. This section only applies to the compactor.
/// A subset of the configs can be overwritten by CLI arguments.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactorConfig {
    // Below configs are CLI configurable.
    #[serde(default = "default::compactor::listen_addr")]
    pub listen_addr: String,

    pub client_address: Option<String>,

    #[serde(default = "default::compactor::state_store")]
    pub state_store: String,

    #[serde(default = "default::compactor::prometheus_listen_addr")]
    pub prometheus_listener_addr: String,

    #[serde(default = "default::compactor::metrics_level")]
    pub metrics_level: u32,

    #[serde(default = "default::compactor::meta_address")]
    pub meta_address: String,

    #[serde(default = "default::compactor::max_concurrent_task_number")]
    pub max_concurrent_task_number: u64,

    pub compaction_worker_threads_number: Option<usize>,
}

impl Default for CompactorConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::server::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    /// The maximum allowed heartbeat interval for workers.
    #[serde(default = "default::server::max_heartbeat_interval_secs")]
    pub max_heartbeat_interval_secs: u32,

    #[serde(default = "default::server::connection_pool_size")]
    pub connection_pool_size: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[batch]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default)]
    pub developer: DeveloperConfig,
}

impl Default for BatchConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[streaming]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamingConfig {
    /// The interval of periodic barrier.
    #[serde(default = "default::streaming::barrier_interval_ms")]
    pub barrier_interval_ms: u32,

    /// The maximum number of barriers in-flight in the compute nodes.
    #[serde(default = "default::streaming::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// There will be a checkpoint for every n barriers
    #[serde(default = "default::streaming::checkpoint_frequency")]
    pub checkpoint_frequency: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    #[serde(default)]
    pub developer: DeveloperConfig,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The section `[storage]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// Target size of the Sstable.
    #[serde(default = "default::storage::sst_size_mb")]
    pub sstable_size_mb: u32,

    /// Size of each block in bytes in SST.
    #[serde(default = "default::storage::block_size_kb")]
    pub block_size_kb: u32,

    /// False positive probability of bloom filter.
    #[serde(default = "default::storage::bloom_false_positive")]
    pub bloom_false_positive: f64,

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
    pub shared_buffer_capacity_mb: u32,

    /// Remote directory for storing data and metadata objects.
    #[serde(default = "default::storage::data_directory")]
    pub data_directory: String,

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

    #[serde(default = "default::storage::object_store_use_batch_delete")]
    pub object_store_use_batch_delete: bool,

    /// Whether to enable state_store_v1 for hummock
    #[serde(default = "default::storage::enable_state_store_v1")]
    pub enable_state_store_v1: bool,
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
#[serde(deny_unknown_fields)]
pub struct FileCacheConfig {
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
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// The subsections `[batch.developer]` and `[streaming.developer]`.
///
/// It is put at [`BatchConfig::developer`] and [`StreamingConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
}

impl Default for DeveloperConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

/// Configs for meta node backup
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    /// Remote storage url for storing snapshots.
    #[serde(default = "default::backup::storage_url")]
    pub storage_url: String,
    /// Remote directory for storing snapshots.
    #[serde(default = "default::backup::storage_directory")]
    pub storage_directory: String,
}

impl Default for BackupConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

mod default {
    pub mod meta {
        use crate::config::MetaBackend;

        pub fn listen_addr() -> String {
            "127.0.9.1:5690".to_string()
        }

        pub fn backend() -> MetaBackend {
            MetaBackend::Mem
        }

        pub fn etcd_endpoints() -> String {
            "".to_string()
        }

        pub fn etcd_username() -> String {
            "".to_string()
        }

        pub fn etcd_password() -> String {
            "".to_string()
        }

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
    }

    pub mod frontend {

        pub fn listen_addr() -> String {
            "127.0.0.1:4566".to_string()
        }

        pub fn meta_address() -> String {
            "http://127.0.0.1:5690".to_string()
        }

        pub fn prometheus_listen_addr() -> String {
            "127.0.0.1:2222".to_string()
        }

        pub fn health_check_listener_addr() -> String {
            "127.0.0.1:6786".to_string()
        }

        pub fn metrics_level() -> u32 {
            0
        }
    }

    pub mod compute_node {
        use crate::config::AsyncStackTraceOption;
        use crate::util::resource_util::cpu::total_cpu_available;
        use crate::util::resource_util::memory::total_memory_available_bytes;

        pub fn listen_addr() -> String {
            "127.0.0.1:5688".to_string()
        }

        pub fn state_store() -> String {
            "hummock+memory".to_string()
        }

        pub fn prometheus_listen_addr() -> String {
            "127.0.0.1:1222".to_string()
        }

        pub fn metrics_level() -> u32 {
            0
        }

        pub fn meta_address() -> String {
            "http://127.0.0.1:5690".to_string()
        }

        pub fn enable_jaeger_tracing() -> bool {
            false
        }

        pub fn async_stack_trace() -> AsyncStackTraceOption {
            AsyncStackTraceOption::On
        }

        pub fn file_cache_dir() -> String {
            "".to_string()
        }

        pub fn total_memory_bytes() -> usize {
            total_memory_available_bytes()
        }

        pub fn parallelism() -> usize {
            total_cpu_available().ceil() as usize
        }
    }

    pub mod compactor {

        pub fn listen_addr() -> String {
            "127.0.0.1:6660".to_string()
        }

        pub fn state_store() -> String {
            "".to_string()
        }

        pub fn prometheus_listen_addr() -> String {
            "127.0.0.1:1260".to_string()
        }

        pub fn metrics_level() -> u32 {
            0
        }

        pub fn meta_address() -> String {
            "http://127.0.0.1:5690".to_string()
        }

        pub fn max_concurrent_task_number() -> u64 {
            16
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
    }

    pub mod storage {

        pub fn sst_size_mb() -> u32 {
            256
        }

        pub fn block_size_kb() -> u32 {
            64
        }

        pub fn bloom_false_positive() -> f64 {
            0.001
        }

        pub fn share_buffers_sync_parallelism() -> u32 {
            1
        }

        pub fn share_buffer_compaction_worker_threads_number() -> u32 {
            4
        }

        pub fn shared_buffer_capacity_mb() -> u32 {
            1024
        }

        pub fn data_directory() -> String {
            "hummock_001".to_string()
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

        pub fn object_store_use_batch_delete() -> bool {
            true
        }
        pub fn enable_state_store_v1() -> bool {
            false
        }
    }

    pub mod streaming {
        pub fn barrier_interval_ms() -> u32 {
            1000
        }

        pub fn in_flight_barrier_nums() -> usize {
            // quick fix
            // TODO: remove this limitation from code
            10000
        }

        pub fn checkpoint_frequency() -> usize {
            10
        }
    }

    pub mod file_cache {

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

    pub mod backup {
        pub fn storage_url() -> String {
            "memory".to_string()
        }

        pub fn storage_directory() -> String {
            "backup".to_string()
        }
    }
}
