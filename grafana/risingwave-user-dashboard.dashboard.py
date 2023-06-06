from grafanalib.core import (
    Dashboard,
    TimeSeries,
    Target,
    GridPos,
    RowPanel,
    Time,
    Templating,
)
import logging
import os
import sys

p = os.path.dirname(__file__)
sys.path.append(p)
from common import *
from jsonmerge import merge

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Fcy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}
datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


def section_actor_info(panels):
    excluded_cols = ["Time", "Value", "__name__", "job", "instance"]
    return [
        panels.row("Actor/Table Id Info"),
        panels.table_info(
            "Actor Id Info",
            "Mapping from actor id to fragment id",
            [panels.table_target(f"{metric('actor_info')}")],
            excluded_cols,
        ),
        panels.table_info(
            "Table Id Info",
            "Mapping from table id to actor id and table name",
            [panels.table_target(f"{metric('table_info')}")],
            excluded_cols,
        ),
    ]


def section_overview(panels):
    mv_filter = "executor_identity=~\".*MaterializeExecutor.*\""
    sink_filter = "executor_identity=~\".*SinkExecutor.*\""
    return [
        panels.row("Overview"),
        panels.timeseries_rowsps(
            "Aggregated Source Throughput(rows/s)",
            "The figure shows the number of rows read by each source per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_name)",
                    "{{source_name}}",
                ),
            ],
        ),
        panels.timeseries_bytesps(
            "Aggregated Source Throughput(MB/s)",
            "The figure shows the number of bytes read by each source per second.",
            [
                panels.target(
                    f"(sum by (source_id)(rate({metric('partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                    "source_id {{source_id}}",
                )
            ],
        ),
        panels.timeseries_rowsps(
            "Aggregated Sink Throughput(rows/s)",
            "The figure shows the number of rows output by each sink per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_executor_row_count', filter=sink_filter)}[$__rate_interval])) by (executor_identity)",
                    "{{executor_identity}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Aggregated Materialized View Throughput(rows/s)",
            "The figure shows the number of rows output by each materialized view per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_executor_row_count', filter=mv_filter)}[$__rate_interval])) by (executor_identity)",
                    "{{executor_identity}}",
                ),
            ],
        ),
        panels.timeseries_latency(
            "Barrier Latency",
            "The time that the data between two consecutive barriers gets fully processed, i.e. the computation "
            "results are made durable into materialized views or sink to external systems. This metric shows to users "
            "the freshness of materialized views.",
            quantile(
                lambda quantile, legend: panels.target(
                    f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                    f"barrier_latency_p{legend}",
                ),
                [50, 99],
            )
            + [
                panels.target(
                    f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval])",
                    "barrier_latency_avg",
                ),
            ],
        ),
        panels.timeseries_count(
            "Alerts",
            """Alerts in the system group by type:
            - Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck or under heavy load. Check 'Barrier Latency' panel.
            - Recovery Triggered: cluster recovery is triggered. Check 'Errors by Type' / 'Node Count' panels.
            - Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check 'Hummock Manager' section in dev dashboard.
            - Lagging Epoch: the pinned or safe epoch is lagging behind the current max committed epoch. Check 'Hummock Manager' section in dev dashboard.
            - Lagging Compaction: there are too many files in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
            - Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
            - Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
            """,
            [
                panels.target(
                    f"{metric('all_barrier_nums')} >= bool 200",
                    "Too Many Barriers",
                ),
                panels.target(
                    f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) > bool 0 + sum({metric('recovery_failure_cnt')}) > bool 0",
                    "Recovery Triggered",
                ),
                panels.target(
                    f"(({metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}) >= bool 100) + "
                    + f"(({metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}) >= bool 100)",
                    "Lagging Version",
                ),
                panels.target(
                    f"(({metric('storage_max_committed_epoch')} - {metric('storage_min_pinned_epoch')}) >= bool 6553600000 unless + {metric('storage_min_pinned_epoch')} == 0) + "
                    + f"(({metric('storage_max_committed_epoch')} - {metric('storage_safe_epoch')}) >= bool 6553600000 unless + {metric('storage_safe_epoch')} == 0)",
                    "Lagging Epoch",
                ),
                panels.target(
                    f"sum(label_replace({metric('storage_level_sst_num')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                    + f"{metric('storage_level_sst_num')}) by (L0) >= bool 200",
                    "Lagging Compaction",
                ),
                panels.target(
                    f"{metric('storage_stale_object_count')} >= bool 200",
                    "Lagging Vacuum",
                ),
                panels.target(
                    f"{metric('state_store_meta_cache_size')} >= bool 2*1024*1024*1024*1.1",
                    "Abnormal Meta Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_block_cache_size')} >= bool 512*1024*1024*1.1",
                    "Abnormal Block Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_uploader_uploading_task_size')} >= bool 1024*1024*1024*0.7",
                    "Abnormal Uploading Memory Usage",
                ),
            ],
            ["last"],
        ),
        panels.timeseries_count(
            "Errors",
            "Errors in the system group by type",
            [
                panels.target(
                    f"sum({metric('user_compute_error_count')}) by (error_type, error_msg, fragment_id, executor_name)",
                    "compute error {{error_type}}: {{error_msg}} ({{executor_name}}: fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"sum({metric('user_source_error_count')}) by (error_type, error_msg, fragment_id, table_id, executor_name)",
                    "parse error {{error_type}}: {{error_msg}} ({{executor_name}}: table_id={{table_id}}, fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"{metric('source_status_is_up')} == 0",
                    "source error: source_id={{source_id}}, source_name={{source_name}} @ {{instance}}",
                ),
                panels.target(
                    f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by (instance, job, type)",
                    "remote storage error {{type}}: {{job}} @ {{instance}}",
                ),
            ],
        ),
        panels.timeseries_query_per_sec(
            "Batch Query QPS",
            "",
            [
                panels.target(
                    f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                    "Local mode",
                ),
                panels.target(
                    f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                    "Distributed mode",
                ),
            ],
        ),
        panels.timeseries_count(
            "Node Count",
            "The number of each type of RisingWave components alive.",
            [
                panels.target(
                    f"sum({metric('worker_num')}) by (worker_type)", "{{worker_type}}"
                )
            ],
            ["last"],
        ),
    ]


def section_cpu(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "CPU",
            [
                panels.timeseries_cpu(
                    "Node CPU Usage",
                    "The CPU usage of each RisingWave component.",
                    [
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by (instance)",
                            "{{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Node CPU Core Number",
                    "Number of CPU cores per RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_cpu_core_num')}) by (instance)",
                            "{{instance}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_memory(outer_panels):
    panels = outer_panels.sub_panel()
    meta_miss_filter = "type='meta_miss'"
    return [
        outer_panels.row_collapsed(
            "Memory",
            [
                panels.timeseries_memory(
                    "Node Memory",
                    "The memory usage of each RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_resident_memory_bytes')}) by (job,instance)",
                            "{{job}} @ {{instance}}",
                        )
                    ],
                ),
                panels.timeseries_memory(
                    "Memory Usage (Total)",
                    "",
                    [
                        panels.target(
                            f"sum({metric('state_store_meta_cache_size')}) by (instance) + "
                            + f"sum({metric('state_store_block_cache_size')}) by (instance) + "
                            + f"sum({metric('state_store_limit_memory_size')}) by (instance)",
                            "storage @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Memory Usage (Detailed)",
                    "",
                    [
                        panels.target(
                            "rate(actor_memory_usage[$__rate_interval])",
                            "streaming actor - {{actor_id}}",
                        ),
                        panels.target(
                            f"sum({metric('state_store_meta_cache_size')}) by (job,instance)",
                            "storage meta cache - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum({metric('state_store_block_cache_size')}) by (job,instance)",
                            "storage block cache - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum({metric('state_store_limit_memory_size')}) by (job,instance)",
                            "storage write buffer - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Executor Cache",
                    "Executor cache statistics",
                    [
                        panels.target(
                            f"rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])",
                            "Join - cache miss - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_join_lookup_total_count')}[$__rate_interval])",
                            "Join - total lookups - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])",
                            "Agg - cache miss - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])",
                            "Distinct agg cache miss - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])",
                            "Group top n cache miss - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])",
                            "Group top n appendonly cache miss - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])",
                            "Lookup executor cache miss - table {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])",
                            "temporal join cache miss - table_id {{table_id}} actor {{actor_id}}",
                        ),

                        panels.target(
                            f"rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])",
                            "Agg - total lookups - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])",
                            "Materialize - cache hit count - table {{table_id}} - actor {{actor_id}}  {{instance}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])",
                            "Materialize - total cache count - table {{table_id}} - actor {{actor_id}}  {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, actor_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, actor_id))",
                            "join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Agg cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Distinct agg cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream group top n cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream lookup cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),

                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "Stream temporal join cache miss ratio - table {{table_id}} actor {{actor_id}} ",
                        ),
                        
                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, actor_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, actor_id))",
                            "materialize executor cache miss ratio - table {{table_id}} - actor {{actor_id}}  {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage Cache",
                    "Storage cache statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by (job, instance, table_id, type)",
                            "memory cache - {{table_id}} @ {{type}} @ {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by (job, type)",
                            "total_meta_miss_count - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage Bloom Filer",
                    "Storage bloom filter statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (job,instance,table_id)",
                            "bloom filter total - {{table_id}} @ {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (job,instance,table_id)",
                            "bloom filter false positive  - {{table_id}} @ {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage File Cache",
                    "Storage file cache statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('file_cache_latency_count')}[$__rate_interval])) by (op, instance)",
                            "file cache {{op}} @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('file_cache_miss')}[$__rate_interval])) by (instance)",
                            "file cache miss @ {{instance}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_network(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Network",
            [
                panels.timeseries_bytes_per_sec(
                    "Streming Remote Exchange (Bytes/s)",
                    "Send/Recv throughput per node for streaming exchange",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])) by (instance)",
                            "Send @ {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])) by (instance)",
                            "Recv @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput per node",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by (instance)",
                            "read - {{instance}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by (instance)",
                            "write - {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Batch Exchange Recv (Rows/s)",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_exchange_recv_row_number')}",
                            "{{query_id}} : {{source_stage_id}}.{{source_task_id}} -> {{target_stage_id}}.{{target_task_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_storage(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Storage",
            [
                panels.timeseries_bytes(
                    "Object Size",
                    """
                    Objects are classified into 3 groups:
                    - not referenced by versions: these object are being deleted from object store.
                    - referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
                    - referenced by current version: these objects are in the latest version.
                    """,
                    [
                        panels.target(
                            f"{metric('storage_stale_object_size')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_size')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_size')}",
                            "referenced by current version",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Object Total Number",
                    """
                    Objects are classified into 3 groups:
                    - not referenced by versions: these object are being deleted from object store.
                    - referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
                    - referenced by current version: these objects are in the latest version.
                    """,
                    [
                        panels.target(
                            f"{metric('storage_stale_object_count')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_count')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_count')}",
                            "referenced by current version",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Write Bytes",
                    "The number of bytes that have been written by compaction."
                    "Flush refers to the process of compacting Memtables to SSTables at Level 0."
                    "Compaction refers to the process of compacting SSTables at one level to another level.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) by (job) > 0",
                            "Compaction - {{job}}",
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by (job) > 0",
                            "Flush - {{job}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by (job)",
                            "read - {{job}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by (job)",
                            "write - {{job}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Size",
                    "Size statistics for checkpoint",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, job))",
                                f"p{legend}" + " - {{job}}",
                            ),
                            [50, 99],
                        ),
                        panels.target(
                            f"sum by(le, job) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, job) (rate({metric('state_store_sync_size_count')}[$__rate_interval]))",
                            "avg - {{job}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming",
            [
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"rate({metric('stream_source_output_rows_counts')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s)",
                    "The figure shows the number of bytes read by each source per second.",
                    [
                        panels.target(
                            f"(sum by (source_id)(rate({metric('partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                            "source={{source_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Throughput(rows)",
                    "Total number of rows that have been read from the backfill operator used by MV on MV",
                    [
                        panels.target(
                            f"rate({metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "Read Snapshot - table_id={{table_id}} actor={{actor_id}} @ {{instance}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "Upstream - table_id={{table_id}} actor={{actor_id}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Backpressure",
                    "We first record the total blocking duration(ns) of output buffer of each actor. It shows how "
                    "much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, "
                    "on average. Then we divide this duration by 1 second and show it as a percentage.",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval]) / 1000000000",
                            "{{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_batch(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Batch",
            [
                panels.timeseries_count(
                    "Running query in distributed execution mode",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_running_query_num')}",
                            "The number of running query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Rejected query in distributed execution mode",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_rejected_query_counter')}",
                            "The number of rejected query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Completed query in distributed execution mode",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Query Latency in Distributed Execution Mode",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p50 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p90 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p99 - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency in Local Execution Mode",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.5, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p50 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.9, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p90 - {{job}} @ {{instance}}",
                        ),
                        panels.target(
                            f"histogram_quantile(0.95, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, job, instance))",
                            "p99 - {{job}} @ {{instance}}",
                        ),
                    ],
                ),
            ],
        )
    ]

templating_list = []
if dynamic_source_enabled:
    templating_list.append(
        {
            "hide": 0,
            "includeAll": False,
            "multi": False,
            "name": f"{datasource_const}",
            "options": [],
            "query": "prometheus",
            "queryValue": "",
            "refresh": 2,
            "skipUrlSync": False,
            "type": "datasource"
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": "label_values(up{risingwave_name=~\".+\"}, namespace)",
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
        "name": "namespace",
        "options": [],
        "query": {
            "query": "label_values(up{risingwave_name=~\".+\"}, namespace)",
            "refId": "StandardVariableQuery"
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {
            "selected": False,
            "text": "risingwave",
            "value": "risingwave"
        },
        "definition": "label_values(up{namespace=\"$namespace\", risingwave_name=~\".+\"}, risingwave_name)",
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": "label_values(up{namespace=\"$namespace\", risingwave_name=~\".+\"}, risingwave_name)",
            "refId": "StandardVariableQuery"
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 6,
        "type": "query",
    }
    if dynamic_source_enabled:
        namespace_json = merge(namespace_json, {"datasource": datasource})
        name_json = merge(name_json, {"datasource": datasource})

    templating_list.append(namespace_json)
    templating_list.append(name_json)


node_json = {
    "current": {
        "selected": False,
        "text": "All",
        "value": "__all"
    },
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, instance)",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Node",
    "multi": True,
    "name": "node",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, instance)",
        "refId": "StandardVariableQuery"
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

job_json = {
    "current": {
        "selected": False,
        "text": "All",
        "value": "__all"
    },
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, job)",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Job",
    "multi": True,
    "name": "job",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, job)",
        "refId": "StandardVariableQuery"
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

if dynamic_source_enabled:
    node_json = merge(node_json, {"datasource": datasource})
    job_json = merge(job_json, {"datasource": datasource})

templating_list.append(node_json)
templating_list.append(job_json)
templating = Templating(templating_list)

dashboard = Dashboard(
    title="risingwave_dashboard",
    description="RisingWave Dashboard",
    tags=["risingwave"],
    timezone="browser",
    editable=True,
    uid=dashboard_uid,
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    templating=templating,
    version=dashboard_version,
    panels=[
        *section_actor_info(panels),
        *section_overview(panels),
        *section_cpu(panels),
        *section_memory(panels),
        *section_network(panels),
        *section_storage(panels),
        *section_streaming(panels),
        *section_batch(panels),
    ],
).auto_panel_ids()
