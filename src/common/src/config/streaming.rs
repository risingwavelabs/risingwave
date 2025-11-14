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

use super::*;

mod async_stack_trace;
mod join_encoding_type;

pub use async_stack_trace::*;
pub use join_encoding_type::*;

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

serde_with::with_prefix!(streaming_prefix "stream_");

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

    /// Minimum cache size for TopN cache per group key.
    #[serde(default = "default::developer::stream_topn_cache_min_capacity")]
    pub topn_cache_min_capacity: usize,

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

    #[serde(default = "default::developer::memory_controller_eviction_factor_aggressive")]
    pub memory_controller_eviction_factor_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_graceful")]
    pub memory_controller_eviction_factor_graceful: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_stable")]
    pub memory_controller_eviction_factor_stable: f64,

    #[serde(default = "default::developer::memory_controller_update_interval_ms")]
    pub memory_controller_update_interval_ms: usize,

    #[serde(default = "default::developer::memory_controller_sequence_tls_step")]
    pub memory_controller_sequence_tls_step: u64,

    #[serde(default = "default::developer::memory_controller_sequence_tls_lag")]
    pub memory_controller_sequence_tls_lag: u64,

    #[serde(default = "default::developer::stream_enable_arrangement_backfill")]
    /// Enable arrangement backfill
    /// If false, the arrangement backfill will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_arrangement_backfill` (default true)
    pub enable_arrangement_backfill: bool,

    #[serde(default = "default::developer::stream_high_join_amplification_threshold")]
    /// If number of hash join matches exceeds this threshold number,
    /// it will be logged.
    pub high_join_amplification_threshold: usize,

    /// Actor tokio metrics is enabled if `enable_actor_tokio_metrics` is set or metrics level >= Debug.
    #[serde(default = "default::developer::enable_actor_tokio_metrics")]
    pub enable_actor_tokio_metrics: bool,

    /// The number of the connections for streaming remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::stream_exchange_connection_pool_size")]
    pub(super) exchange_connection_pool_size: Option<u16>,

    /// A flag to allow disabling the auto schema change handling
    #[serde(default = "default::developer::stream_enable_auto_schema_change")]
    pub enable_auto_schema_change: bool,

    #[serde(default = "default::developer::enable_shared_source")]
    /// Enable shared source
    /// If false, the shared source will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_shared_source` (default true)
    pub enable_shared_source: bool,

    #[serde(default = "default::developer::switch_jdbc_pg_to_native")]
    /// When true, all jdbc sinks with connector='jdbc' and jdbc.url="jdbc:postgresql://..."
    /// will be switched from jdbc postgresql sinks to rust native (connector='postgres') sinks.
    pub switch_jdbc_pg_to_native: bool,

    /// The maximum number of consecutive barriers allowed in a message when sent between actors.
    #[serde(default = "default::developer::stream_max_barrier_batch_size")]
    pub max_barrier_batch_size: u32,

    /// Configure the system-wide cache row cardinality of hash join.
    /// For example, if this is set to 1000, it means we can have at most 1000 rows in cache.
    #[serde(default = "default::developer::streaming_hash_join_entry_state_max_rows")]
    pub hash_join_entry_state_max_rows: usize,

    #[serde(default = "default::developer::streaming_now_progress_ratio")]
    pub now_progress_ratio: Option<f32>,

    /// Enable / Disable profiling stats used by `EXPLAIN ANALYZE`
    #[serde(default = "default::developer::enable_explain_analyze_stats")]
    pub enable_explain_analyze_stats: bool,

    #[serde(default)]
    pub compute_client_config: RpcClientConfig,

    /// `IcebergListExecutor`: The interval in seconds for Iceberg source to list new files.
    #[serde(default = "default::developer::iceberg_list_interval_sec")]
    pub iceberg_list_interval_sec: u64,

    /// `IcebergFetchExecutor`: The number of files the executor will fetch concurrently in a batch.
    #[serde(default = "default::developer::iceberg_fetch_batch_size")]
    pub iceberg_fetch_batch_size: u64,

    /// `IcebergSink`: The size of the cache for positional delete in the sink.
    #[serde(default = "default::developer::iceberg_sink_positional_delete_cache_size")]
    pub iceberg_sink_positional_delete_cache_size: usize,

    /// `IcebergSink`: The maximum number of rows in a row group when writing Parquet files.
    #[serde(default = "default::developer::iceberg_sink_write_parquet_max_row_group_rows")]
    pub iceberg_sink_write_parquet_max_row_group_rows: usize,

    /// Whether by default enable preloading all rows in memory for state table.
    /// If true, all capable state tables will preload its state to memory
    #[serde(default = "default::streaming::default_enable_mem_preload_state_table")]
    pub default_enable_mem_preload_state_table: bool,

    /// The list of state table ids to *enable* preloading all rows in memory for state table.
    /// Only takes effect when `default_enable_mem_preload_state_table` is false.
    #[serde(default)]
    pub mem_preload_state_table_ids_whitelist: Vec<u32>,

    /// The list of state table ids to *disable* preloading all rows in memory for state table.
    /// Only takes effect when `default_enable_mem_preload_state_table` is true.
    #[serde(default)]
    pub mem_preload_state_table_ids_blacklist: Vec<u32>,

    /// Eliminate unnecessary updates aggressively, even if it impacts performance. Enable this
    /// only if it's confirmed that no-op updates are causing significant streaming amplification.
    #[serde(default)]
    pub aggressive_noop_update_elimination: bool,

    /// The interval in seconds for the refresh scheduler to check and trigger scheduled refreshes.
    #[serde(default = "default::developer::refresh_scheduler_interval_sec")]
    pub refresh_scheduler_interval_sec: u64,

    /// Determine which encoding will be used to encode join rows in operator cache.
    #[serde(default)]
    pub join_encoding_type: JoinEncodingType,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

pub mod default {
    pub use crate::config::default::developer;

    pub mod streaming {
        use tracing::info;

        use crate::config::AsyncStackTraceOption;
        use crate::util::env_var::env_var_is_true;

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

        pub fn default_enable_mem_preload_state_table() -> bool {
            if env_var_is_true("DEFAULT_ENABLE_MEM_PRELOAD_STATE_TABLE") {
                info!("enabled mem_preload_state_table globally by env var");
                true
            } else {
                false
            }
        }
    }
}
