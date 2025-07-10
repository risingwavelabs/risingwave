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

use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;

use super::compaction::CompactionConfig;
use super::types::{DefaultParallelism, MetaBackend, RpcClientConfig, Unrecognized};

serde_with::with_prefix!(meta_prefix "meta_");

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

    /// Max number of object per full GC job can fetch.
    #[serde(default = "default::meta::full_gc_object_limit")]
    pub full_gc_object_limit: u64,

    /// Duration in seconds to retain garbage collection history data.
    #[serde(default = "default::meta::gc_history_retention_time_sec")]
    pub gc_history_retention_time_sec: u64,

    /// Max number of inflight time travel query.
    #[serde(default = "default::meta::max_inflight_time_travel_query")]
    pub max_inflight_time_travel_query: u64,

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

    /// The interval at which a Hummock version snapshot is taken for time travel.
    ///
    /// Larger value indicates less storage overhead but worse query performance.
    #[serde(default = "default::meta::hummock_time_travel_snapshot_interval")]
    pub hummock_time_travel_snapshot_interval: u64,

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

    #[serde(default = "default::meta::move_table_size_limit")]
    #[deprecated]
    pub move_table_size_limit: u64,

    #[serde(default = "default::meta::split_group_size_limit")]
    #[deprecated]
    pub split_group_size_limit: u64,

    #[serde(default = "default::meta::cut_table_size_limit")]
    #[deprecated]
    pub cut_table_size_limit: u64,

    /// Whether to protect dropping a table with incoming sink.
    #[serde(default = "default::meta::protect_drop_table_with_incoming_sink")]
    pub protect_drop_table_with_incoming_sink: bool,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// Whether config object storage bucket lifecycle to purge stale data.
    #[serde(default)]
    pub do_not_config_object_storage_lifecycle: bool,

    /// Count of partition in split group. Meta will assign this value to every new group when it splits from default-group by automatically.
    /// Each partition contains aligned data of `vnode_count / partition_vnode_count` consecutive virtual-nodes of one state table.
    #[serde(default = "default::meta::partition_vnode_count")]
    pub partition_vnode_count: u32,

    /// The threshold of write throughput to trigger a group split.
    #[serde(
        default = "default::meta::table_high_write_throughput_threshold",
        alias = "table_write_throughput_threshold"
    )]
    pub table_high_write_throughput_threshold: u64,

    #[serde(
        default = "default::meta::table_low_write_throughput_threshold",
        alias = "min_table_split_write_throughput"
    )]
    /// The threshold of write throughput to trigger a group merge.
    pub table_low_write_throughput_threshold: u64,

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

    /// Count of partitions of tables in default group and materialized view group.
    /// The meta node will decide according to some strategy whether to cut the boundaries of the file according to the vnode alignment.
    /// Each partition contains aligned data of `vnode_count / hybrid_partition_vnode_count` consecutive virtual-nodes of one state table.
    /// Set it zero to disable this feature.
    #[serde(default = "default::meta::hybrid_partition_vnode_count")]
    pub hybrid_partition_vnode_count: u32,

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

    /// Whether to split the compaction group when the size of the group exceeds the `compaction_group_config.max_estimated_group_size() * split_group_size_ratio`.
    #[serde(default = "default::meta::split_group_size_ratio")]
    pub split_group_size_ratio: f64,

    // During group scheduling, the configured `*_throughput_ratio` is used to determine if the sample exceeds the threshold.
    // Use `table_stat_throuput_window_seconds_for_*` to check if the split and merge conditions are met.
    /// To split the compaction group when the high throughput statistics of the group exceeds the threshold.
    #[serde(default = "default::meta::table_stat_high_write_throughput_ratio_for_split")]
    pub table_stat_high_write_throughput_ratio_for_split: f64,

    /// To merge the compaction group when the low throughput statistics of the group exceeds the threshold.
    #[serde(default = "default::meta::table_stat_low_write_throughput_ratio_for_merge")]
    pub table_stat_low_write_throughput_ratio_for_merge: f64,

    // Hummock also control the size of samples to be judged during group scheduling by `table_stat_sample_size_for_split` and `table_stat_sample_size_for_merge`.
    // Will use max(table_stat_throuput_window_seconds_for_split /ckpt, table_stat_throuput_window_seconds_for_merge/ckpt) as the global sample size.
    // For example, if `table_stat_throuput_window_seconds_for_merge` = 240 and `table_stat_throuput_window_seconds_for_split` = 60, and `ckpt_sec = 1`,
    //  global sample size will be max(240/1, 60/1), then only the last 60 samples will be considered for split, and so on.
    /// The window seconds of table throughput statistic history for split compaction group.
    #[serde(default = "default::meta::table_stat_throuput_window_seconds_for_split")]
    pub table_stat_throuput_window_seconds_for_split: usize,

    /// The window seconds of table throughput statistic history for merge compaction group.
    #[serde(default = "default::meta::table_stat_throuput_window_seconds_for_merge")]
    pub table_stat_throuput_window_seconds_for_merge: usize,

    /// The threshold of table size in one compact task to decide whether to partition one table into `hybrid_partition_vnode_count` parts, which belongs to default group and materialized view group.
    /// Set it max value of 64-bit number to disable this feature.
    #[serde(default = "default::meta::compact_task_table_size_partition_threshold_low")]
    pub compact_task_table_size_partition_threshold_low: u64,

    /// The threshold of table size in one compact task to decide whether to partition one table into `partition_vnode_count` parts, which belongs to default group and materialized view group.
    /// Set it max value of 64-bit number to disable this feature.
    #[serde(default = "default::meta::compact_task_table_size_partition_threshold_high")]
    pub compact_task_table_size_partition_threshold_high: u64,

    /// The interval of the periodic scheduling compaction group split job.
    #[serde(
        default = "default::meta::periodic_scheduling_compaction_group_split_interval_sec",
        alias = "periodic_split_compact_group_interval_sec"
    )]
    pub periodic_scheduling_compaction_group_split_interval_sec: u64,

    /// The interval of the periodic scheduling compaction group merge job.
    #[serde(default = "default::meta::periodic_scheduling_compaction_group_merge_interval_sec")]
    pub periodic_scheduling_compaction_group_merge_interval_sec: u64,

    /// The threshold of each dimension of the compaction group after merging. When the dimension * `compaction_group_merge_dimension_threshold` >= limit, the merging job will be rejected.
    #[serde(default = "default::meta::compaction_group_merge_dimension_threshold")]
    pub compaction_group_merge_dimension_threshold: f64,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta_store_config: MetaStoreConfig,
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

    /// Max number of actor allowed per parallelism (default = 100).
    /// CREATE MV/Table will be noticed when the number of actors exceeds this limit.
    #[serde(default = "default::developer::actor_cnt_per_worker_parallelism_soft_limit")]
    pub actor_cnt_per_worker_parallelism_soft_limit: usize,

    /// Max number of actor allowed per parallelism (default = 400).
    /// CREATE MV/Table will be rejected when the number of actors exceeds this limit.
    #[serde(default = "default::developer::actor_cnt_per_worker_parallelism_hard_limit")]
    pub actor_cnt_per_worker_parallelism_hard_limit: usize,

    /// Max number of SSTs fetched from meta store per SELECT, during time travel Hummock version replay.
    #[serde(default = "default::developer::hummock_time_travel_sst_info_fetch_batch_size")]
    pub hummock_time_travel_sst_info_fetch_batch_size: usize,

    /// Max number of SSTs inserted into meta store per INSERT, during time travel metadata writing.
    #[serde(default = "default::developer::hummock_time_travel_sst_info_insert_batch_size")]
    pub hummock_time_travel_sst_info_insert_batch_size: usize,

    #[serde(default = "default::developer::time_travel_vacuum_interval_sec")]
    pub time_travel_vacuum_interval_sec: u64,

    /// Max number of epoch-to-version inserted into meta store per INSERT, during time travel metadata writing.
    #[serde(default = "default::developer::hummock_time_travel_epoch_version_insert_batch_size")]
    pub hummock_time_travel_epoch_version_insert_batch_size: usize,

    #[serde(default = "default::developer::hummock_gc_history_insert_batch_size")]
    pub hummock_gc_history_insert_batch_size: usize,

    #[serde(default = "default::developer::hummock_time_travel_filter_out_objects_batch_size")]
    pub hummock_time_travel_filter_out_objects_batch_size: usize,

    #[serde(default = "default::developer::hummock_time_travel_filter_out_objects_v1")]
    pub hummock_time_travel_filter_out_objects_v1: bool,

    #[serde(
        default = "default::developer::hummock_time_travel_filter_out_objects_list_version_batch_size"
    )]
    pub hummock_time_travel_filter_out_objects_list_version_batch_size: usize,

    #[serde(
        default = "default::developer::hummock_time_travel_filter_out_objects_list_delta_batch_size"
    )]
    pub hummock_time_travel_filter_out_objects_list_delta_batch_size: usize,

    #[serde(default)]
    pub compute_client_config: RpcClientConfig,

    #[serde(default)]
    pub stream_client_config: RpcClientConfig,

    #[serde(default)]
    pub frontend_client_config: RpcClientConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaStoreConfig {
    /// Maximum number of connections for the meta store connection pool.
    #[serde(default = "default::meta_store_config::max_connections")]
    pub max_connections: u32,
    /// Minimum number of connections for the meta store connection pool.
    #[serde(default = "default::meta_store_config::min_connections")]
    pub min_connections: u32,
    /// Connection timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::connection_timeout_sec")]
    pub connection_timeout_sec: u64,
    /// Idle timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::idle_timeout_sec")]
    pub idle_timeout_sec: u64,
    /// Acquire timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::acquire_timeout_sec")]
    pub acquire_timeout_sec: u64,
}

mod default {
    use super::*;

    pub mod meta {
        use super::*;

        pub fn min_sst_retention_time_sec() -> u64 { 3600 * 6 }
        pub fn full_gc_interval_sec() -> u64 { 3600 }
        pub fn full_gc_object_limit() -> u64 { 100_000 }
        pub fn gc_history_retention_time_sec() -> u64 { 3600 * 6 }
        pub fn max_inflight_time_travel_query() -> u64 { 1000 }
        pub fn periodic_compaction_interval_sec() -> u64 { 60 }
        pub fn vacuum_interval_sec() -> u64 { 30 }
        pub fn vacuum_spin_interval_ms() -> u64 { 50 }
        pub fn hummock_version_checkpoint_interval_sec() -> u64 { 30 }
        pub fn enable_hummock_data_archive() -> bool { false }
        pub fn hummock_time_travel_snapshot_interval() -> u64 { 100 }
        pub fn min_delta_log_num_for_hummock_version_checkpoint() -> u64 { 10 }
        pub fn max_heartbeat_interval_sec() -> u32 { 300 }
        pub fn meta_leader_lease_secs() -> u64 { 30 }
        pub fn default_parallelism() -> DefaultParallelism { DefaultParallelism::Full }
        pub fn node_num_monitor_interval_sec() -> u64 { 10 }
        pub fn backend() -> MetaBackend { MetaBackend::Mem }
        pub fn periodic_space_reclaim_compaction_interval_sec() -> u64 { 3600 }
        pub fn periodic_ttl_reclaim_compaction_interval_sec() -> u64 { 1800 }
        pub fn periodic_scheduling_compaction_group_split_interval_sec() -> u64 { 10 }
        pub fn periodic_tombstone_reclaim_compaction_interval_sec() -> u64 { 600 }
        #[deprecated] pub fn move_table_size_limit() -> u64 { 68719476736 }
        #[deprecated] pub fn split_group_size_limit() -> u64 { 68719476736 }
        pub fn protect_drop_table_with_incoming_sink() -> bool { true }
        pub fn partition_vnode_count() -> u32 { 32 }
        pub fn table_high_write_throughput_threshold() -> u64 { 16777216 }
        pub fn table_low_write_throughput_threshold() -> u64 { 4194304 }
        pub fn compaction_task_max_heartbeat_interval_secs() -> u64 { 120 }
        pub fn compaction_task_max_progress_interval_secs() -> u64 { 120 }
        #[deprecated] pub fn cut_table_size_limit() -> u64 { 1073741824 }
        pub fn hybrid_partition_vnode_count() -> u32 { 4 }
        pub fn compact_task_table_size_partition_threshold_low() -> u64 { 134217728 }
        pub fn compact_task_table_size_partition_threshold_high() -> u64 { 402653184 }
        pub fn event_log_enabled() -> bool { true }
        pub fn event_log_channel_max_size() -> u32 { 10 }
        pub fn parallelism_control_batch_size() -> usize { 10 }
        pub fn parallelism_control_trigger_period_sec() -> u64 { 10 }
        pub fn parallelism_control_trigger_first_delay_sec() -> u64 { 30 }
        pub fn enable_dropped_column_reclaim() -> bool { false }
        pub fn split_group_size_ratio() -> f64 { 2.0 }
        pub fn table_stat_high_write_throughput_ratio_for_split() -> f64 { 0.8 }
        pub fn table_stat_low_write_throughput_ratio_for_merge() -> f64 { 0.8 }
        pub fn table_stat_throuput_window_seconds_for_split() -> usize { 60 }
        pub fn table_stat_throuput_window_seconds_for_merge() -> usize { 240 }
        pub fn periodic_scheduling_compaction_group_merge_interval_sec() -> u64 { 30 }
        pub fn compaction_group_merge_dimension_threshold() -> f64 { 0.8 }
    }

    pub mod developer {
        pub fn meta_cached_traces_num() -> u32 { 256 }
        pub fn meta_cached_traces_memory_limit_bytes() -> usize { 64 * 1024 * 1024 }
        pub fn enable_trivial_move() -> bool { true }
        pub fn enable_check_task_level_overlap() -> bool { true }
        pub fn max_trivial_move_task_count_per_loop() -> usize { 256 }
        pub fn max_get_task_probe_times() -> usize { 5 }
        pub fn actor_cnt_per_worker_parallelism_soft_limit() -> usize { 100 }
        pub fn actor_cnt_per_worker_parallelism_hard_limit() -> usize { 400 }
        pub fn hummock_time_travel_sst_info_fetch_batch_size() -> usize { 1000 }
        pub fn hummock_time_travel_sst_info_insert_batch_size() -> usize { 1000 }
        pub fn time_travel_vacuum_interval_sec() -> u64 { 600 }
        pub fn hummock_time_travel_epoch_version_insert_batch_size() -> usize { 10000 }
        pub fn hummock_gc_history_insert_batch_size() -> usize { 10000 }
        pub fn hummock_time_travel_filter_out_objects_batch_size() -> usize { 1000 }
        pub fn hummock_time_travel_filter_out_objects_v1() -> bool { false }
        pub fn hummock_time_travel_filter_out_objects_list_version_batch_size() -> usize { 1000 }
        pub fn hummock_time_travel_filter_out_objects_list_delta_batch_size() -> usize { 500 }
        pub fn rpc_client_connect_timeout_secs() -> u64 { 5 }
    }

    pub mod meta_store_config {
        pub fn max_connections() -> u32 { 10 }
        pub fn min_connections() -> u32 { 1 }
        pub fn connection_timeout_sec() -> u64 { 30 }
        pub fn idle_timeout_sec() -> u64 { 600 }
        pub fn acquire_timeout_sec() -> u64 { 30 }
    }
}