
## meta.compaction_config

| Config | Desription |
|--------|------------|
| max_bytes_for_level_base | |
| max_bytes_for_level_multiplier | |
| max_compaction_bytes | |
| sub_level_max_compaction_bytes | |
| level0_tier_compact_file_number | |
| target_file_size_base | |
| compaction_filter_mask | |
| max_sub_compaction | |
| level0_stop_write_threshold_sub_level_number | |
| level0_sub_level_compact_level_count | |
| level0_overlapping_sub_level_compact_level_count | |
| max_space_reclaim_bytes | |
| level0_max_compact_file_number | |
| tombstone_reclaim_ratio | |
| enable_emergency_picker | |

## server

| Config | Desription |
|--------|------------|
| heartbeat_interval_ms |  The interval for periodic heartbeat from worker to the meta service.|
| connection_pool_size | |
| metrics_level |  Used for control the metrics level, similar to log level.|
| telemetry_enabled | |
| heap_profiling |  Enable heap profile dump when memory usage is high.|
| grpc_max_reset_stream | |
| unrecognized | |

## system

| Config | Desription |
|--------|------------|
| barrier_interval_ms | The interval of periodic barrier.|
| checkpoint_frequency | There will be a checkpoint for every n barriers.|
| sstable_size_mb | Target size of the Sstable.|
| parallel_compact_size_mb | |
| block_size_kb | Size of each block in bytes in SST.|
| bloom_false_positive | False positive probability of bloom filter.|
| state_store | |
| data_directory | Remote directory for storing data and metadata objects.|
| backup_storage_url | Remote storage url for storing snapshots.|
| backup_storage_directory | Remote directory for storing snapshots.|
| max_concurrent_creating_streaming_jobs | Max number of concurrent creating streaming jobs.|
| pause_on_next_bootstrap | Whether to pause all data sources on next bootstrap.|
| wasm_storage_url | |
| enable_tracing | Whether to enable distributed tracing.|

## meta.developer

| Config | Desription |
|--------|------------|
| cached_traces_num |  The number of traces to be cached in-memory by the tracing collector
 embedded in the meta node.|
| cached_traces_memory_limit_bytes |  The maximum memory usage in bytes for the tracing collector embedded
 in the meta node.|

## 

| Config | Desription |
|--------|------------|
| unrecognized | |

## meta

| Config | Desription |
|--------|------------|
| min_sst_retention_time_sec |  Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they
 are dangling.|
| full_gc_interval_sec |  Interval of automatic hummock full GC.|
| collect_gc_watermark_spin_interval_sec |  The spin interval when collecting global GC watermark in hummock.|
| periodic_compaction_interval_sec |  Schedule compaction for all compaction groups with this interval.|
| vacuum_interval_sec |  Interval of invoking a vacuum job, to remove stale metadata from meta store and objects
 from object store.|
| vacuum_spin_interval_ms |  The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of
 meta node.|
| hummock_version_checkpoint_interval_sec |  Interval of hummock version checkpoint.|
| min_delta_log_num_for_hummock_version_checkpoint |  The minimum delta log number a new checkpoint should compact, otherwise the checkpoint
 attempt is rejected.|
| max_heartbeat_interval_secs |  Maximum allowed heartbeat interval in seconds.|
| disable_recovery |  Whether to enable fail-on-recovery. Should only be used in e2e tests.|
| enable_scale_in_when_recovery |  Whether to enable scale-in when recovery.|
| enable_automatic_parallelism_control |  Whether to enable auto-scaling feature.|
| meta_leader_lease_secs | |
| dangerous_max_idle_secs |  After specified seconds of idle (no mview or flush), the process will be exited.
 It is mainly useful for playgrounds.|
| default_parallelism |  The default global parallelism for all streaming jobs, if user doesn't specify the
 parallelism, this value will be used. `FULL` means use all available parallelism units,
 otherwise it's a number.|
| enable_compaction_deterministic |  Whether to enable deterministic compaction scheduling, which
 will disable all auto scheduling of compaction tasks.
 Should only be used in e2e tests.|
| enable_committed_sst_sanity_check |  Enable sanity check when SSTs are committed.|
| node_num_monitor_interval_sec | |
| backend | |
| periodic_space_reclaim_compaction_interval_sec |  Schedule space_reclaim compaction for all compaction groups with this interval.|
| periodic_ttl_reclaim_compaction_interval_sec |  Schedule ttl_reclaim compaction for all compaction groups with this interval.|
| periodic_tombstone_reclaim_compaction_interval_sec | |
| periodic_split_compact_group_interval_sec | |
| move_table_size_limit | |
| split_group_size_limit | |
| cut_table_size_limit | |
| unrecognized | |
| do_not_config_object_storage_lifecycle |  Whether config object storage bucket lifecycle to purge stale data.|
| partition_vnode_count | |
| table_write_throughput_threshold | |
| min_table_split_write_throughput |  If the size of one table is smaller than `min_table_split_write_throughput`, we would not
 split it to an single group.|
| compaction_task_max_heartbeat_interval_secs | |
| compaction_task_max_progress_interval_secs | |
| hybird_partition_vnode_count | |
| event_log_enabled | |
| event_log_channel_max_size |  Keeps the latest N events per channel.|

## batch.developer

| Config | Desription |
|--------|------------|
| connector_message_buffer_size |  The capacity of the chunks in the channel that connects between `ConnectorSource` and
 `SourceExecutor`.|
| output_channel_size |  The size of the channel used for output to exchange/shuffle.|
| chunk_size |  The size of a chunk produced by `RowSeqScanExecutor`|

## storage

| Config | Desription |
|--------|------------|
| share_buffers_sync_parallelism |  parallelism while syncing share buffers into L0 SST. Should NOT be 0.|
| share_buffer_compaction_worker_threads_number |  Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
 tokio's default value (number of CPU core).|
| shared_buffer_capacity_mb |  Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
 is enough space.|
| shared_buffer_flush_ratio |  The shared buffer will start flushing data to object when the ratio of memory usage to the
 shared buffer capacity exceed such ratio.|
| imm_merge_threshold |  The threshold for the number of immutable memtables to merge to a new imm.|
| write_conflict_detection_enabled |  Whether to enable write conflict detection|
| block_cache_capacity_mb |  Capacity of sstable block cache.|
| high_priority_ratio_in_percent | |
| meta_cache_capacity_mb |  Capacity of sstable meta cache.|
| prefetch_buffer_capacity_mb |  max memory usage for large query|
| max_prefetch_block_number |  max prefetch block number|
| disable_remote_compactor | |
| share_buffer_upload_concurrency |  Number of tasks shared buffer can upload in parallel.|
| compactor_memory_limit_mb | |
| compactor_max_task_multiplier |  Compactor calculates the maximum number of tasks that can be executed on the node based on
 worker_num and compactor_max_task_multiplier.
 max_pull_task_count = worker_num * compactor_max_task_multiplier|
| compactor_memory_available_proportion |  The percentage of memory available when compactor is deployed separately.
 non_reserved_memory_bytes = system_memory_available_bytes * compactor_memory_available_proportion|
| sstable_id_remote_fetch_number |  Number of SST ids fetched from meta per RPC|
| data_file_cache | |
| meta_file_cache | |
| cache_refill | |
| min_sst_size_for_streaming_upload |  Whether to enable streaming upload for sstable.|
| max_sub_compaction |  Max sub compaction task numbers|
| max_concurrent_compaction_task_number | |
| max_preload_wait_time_mill | |
| max_version_pinning_duration_sec | |
| compactor_max_sst_key_count | |
| compact_iter_recreate_timeout_ms | |
| compactor_max_sst_size | |
| enable_fast_compaction | |
| check_compaction_result | |
| max_preload_io_retry_times | |
| compactor_fast_max_compact_delete_ratio | |
| compactor_fast_max_compact_task_size | |
| unrecognized | |
| mem_table_spill_threshold |  The spill threshold for mem table.|
| object_store | |

## batch

| Config | Desription |
|--------|------------|
| worker_threads_num |  The thread number of the batch task runtime in the compute node. The default value is
 decided by `tokio`.|
| distributed_query_limit | |
| enable_barrier_read | |
| statement_timeout_in_sec |  Timeout for a batch query in seconds.|
| unrecognized | |
| frontend_compute_runtime_worker_threads |  frontend compute runtime worker threads|

## streaming

| Config | Desription |
|--------|------------|
| in_flight_barrier_nums |  The maximum number of barriers in-flight in the compute nodes.|
| actor_runtime_worker_threads_num |  The thread number of the streaming actor runtime in the compute node. The default value is
 decided by `tokio`.|
| async_stack_trace |  Enable async stack tracing through `await-tree` for risectl.|
| developer | |
| unique_user_stream_errors |  Max unique user stream errors per actor|
| unrecognized | |
