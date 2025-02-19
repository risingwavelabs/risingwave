import logging
import os
import sys

from grafanalib.core import (
    Dashboard,
    GridPos,
    RowPanel,
    Target,
    Templating,
    Time,
    TimeSeries,
)

p = os.path.dirname(__file__)
sys.path.append(p)

from jsonmerge import merge

from common import *

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Fcy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}
datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


def section_actor_info(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Actor/Table Id Info",
            [
                panels.table_info(
                    "Actor Info",
                    "Information about actors",
                    [panels.table_target(f"group({metric('actor_info')}) by (actor_id, fragment_id, compute_node)")],
                    ["actor_id", "fragment_id", "compute_node"],
                ),
                panels.table_info(
                    "State Table Info",
                    "Information about state tables. Column `materialized_view_id` is the id of the materialized view that this state table belongs to.",
                    [panels.table_target(f"group({metric('table_info')}) by (table_id, table_name, table_type, materialized_view_id, fragment_id, compaction_group_id)")],
                    ["table_id", "table_name", "table_type", "materialized_view_id", "fragment_id", "compaction_group_id"],
                ),
                panels.table_info(
                    "Actor Count (Group By Compute Node)",
                    "Actor count per compute node",
                    [panels.table_target(f"count({metric('actor_info')}) by (compute_node)")],
                    ["table_id", "table_name", "table_type", "materialized_view_id", "fragment_id", "compaction_group_id"],
                    dict.fromkeys(["Time"], True)
                )
            ],
        )
    ]


def section_overview(panels):
    mv_filter = 'executor_identity=~".*MaterializeExecutor.*"'
    sink_filter = 'executor_identity=~".*SinkExecutor.*"'
    return [
        panels.row("Overview"),
        panels.timeseries_rowsps(
            "Source Throughput(rows/s)",
            "The figure shows the number of rows read by each source per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                    "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                ),
            ],
        ),
        panels.timeseries_bytesps(
            "Source Throughput(MB/s)",
            "The figure shows the number of bytes read by each source per second.",
            [
                panels.target(
                    f"(sum by (source_id, source_name, fragment_id)(rate({metric('source_partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                    "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                )
            ],
        ),
        panels.timeseries_rowsps(
            "Sink Throughput(rows/s)",
            "The number of rows streamed into each sink per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)",
                    "sink {{sink_id}} {{sink_name}}",
                ),
            ],
        ),
        panels.timeseries_rowsps(
            "Materialized View Throughput(rows/s)",
            "The figure shows the number of rows written into each materialized view per second.",
            [
                panels.target(
                    f"sum(rate({metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                    "mview {{table_id}} {{table_name}}",
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
                    f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval]) > 0",
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
            - Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard, and take care of the type of 'Commit Flush Bytes' and 'Compaction Throughput', whether the throughput is too low.
            - Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
            - Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
            - Write Stall: Compaction cannot keep up. Stall foreground write, Check 'Compaction' section in dev dashboard.
            - Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check 'Hummock Manager' section in dev dashboard.
            - Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check 'Hummock Manager' and `Compaction` section in dev dashboard and take care of the type of 'Compaction Success Count', whether the number of trivial-move tasks spiking.
            - Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check 'Hummock Write' section in dev dashboard and take care of the 'Event handle latency', whether the time consumed exceeds the barrier latency.
            - Abnormal Object Storage Failure: the number of object storage failures is too large, exceeding the expected 50. Check 'Object Storage' section in dev dashboard and take care of the 'Object Storage Failure Rate', whether the rate is too high.
            """,
            [
                panels.target(
                    f"{metric('all_barrier_nums')} >= bool 200",
                    "Too Many Barriers {{database_id}}",
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
                    f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                    + f"{metric('storage_level_total_file_size')}) by (L0) >= bool 52428800",
                    "Lagging Compaction",
                ),
                panels.target(
                    f"{metric('storage_stale_object_count')} >= bool 200",
                    "Lagging Vacuum",
                ),
                panels.target(
                    f"{metric('state_store_meta_cache_usage_ratio')} >= bool 1.1",
                    "Abnormal Meta Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_block_cache_usage_ratio')} >= bool 1.1",
                    "Abnormal Block Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_uploading_memory_usage_ratio')} >= bool 0.7",
                    "Abnormal Uploading Memory Usage",
                ),
                panels.target(
                    f"{metric('storage_write_stop_compaction_groups')} > bool 0",
                    "Write Stall",
                ),
                panels.target(
                    f"{metric('storage_version_size')} >= bool 314572800",
                    "Abnormal Version Size",
                ),
                panels.target(
                    f"{metric('storage_delta_log_count')} >= bool 5000",
                    "Abnormal Delta Log Number",
                ),
                panels.target(
                    f"{metric('state_store_event_handler_pending_event')} >= bool 10000000",
                    "Abnormal Pending Event Number",
                ),
                panels.target(
                    f"{metric('object_store_failure_count')} >= bool 50",
                    "Abnormal Object Storage Failure",
                ),
            ],
            ["last"],
        ),
        panels.timeseries_count(
            "Errors",
            "Errors in the system group by type",
            [
                panels.target(
                    f"sum({metric('user_compute_error')}) by (error_type, executor_name, fragment_id)",
                    "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"sum({metric('user_source_error')}) by (error_type, source_id, source_name, fragment_id)",
                    "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"sum({metric('user_sink_error')}) by (error_type, sink_id, sink_name, fragment_id)",
                    "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"{metric('source_status_is_up')} == 0",
                    "source error: source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                    % NODE_LABEL,
                ),
                panels.target(
                    f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                    "remote storage error {{type}}: {{%s}} @ {{%s}}"
                    % (COMPONENT_LABEL, NODE_LABEL),
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
        panels.timeseries_count(
            "Active Sessions",
            "Number of active sessions in frontend nodes",
            [
                panels.target(
                    f"{metric('frontend_active_sessions')}",
                    "",
                ),
            ],
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
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Node CPU Core Number",
                    "Number of CPU cores per RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_cpu_core_num')}) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
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
                            f"avg({metric('process_resident_memory_bytes')}) by ({COMPONENT_LABEL},{NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_memory(
                    "Memory Usage (Total)",
                    "",
                    [
                        panels.target(
                            f"sum({metric('state_store_meta_cache_size')}) by ({NODE_LABEL}) + "
                            + f"sum({metric('state_store_block_cache_size')}) by ({NODE_LABEL}) + "
                            + f"sum({metric('uploading_memory_size')}) by ({NODE_LABEL})",
                            "storage @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Memory Usage (Detailed)",
                    "",
                    [
                        panels.target(
                            f"sum({metric('state_store_meta_cache_size')}) by ({COMPONENT_LABEL},{NODE_LABEL})",
                            "storage meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_block_cache_size')}) by ({COMPONENT_LABEL},{NODE_LABEL})",
                            "storage block cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL},{NODE_LABEL})",
                            "storage write buffer - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('stream_memory_usage')} * on(table_id) group_left(materialized_view_id) table_info) by (materialized_view_id)",
                            "materialized_view {{materialized_view_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id)) >= 0",
                            "Join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_state_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_state_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg state cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Distinct agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream lookup cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream temporal join cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Materialize executor cache miss ratio - table {{table_id}} fragment {{fragment_id}}"
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_left_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache left miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_right_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache right miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage Cache",
                    "Storage cache statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type)",
                            "memory cache - {{table_id}} @ {{type}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, type)",
                            "total_meta_miss_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage Bloom Filer",
                    "Storage bloom filter statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "bloom filter total - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "bloom filter false positive  - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage File Cache",
                    "Storage file cache statistics",
                    [
                        panels.target(
                            f"sum(rate({metric('file_cache_latency_count')}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "file cache {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('file_cache_miss')}[$__rate_interval])) by ({NODE_LABEL})",
                            "file cache miss @ {{%s}}" % NODE_LABEL,
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
                            f"sum(rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Send @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Recv @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput per node",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by ({NODE_LABEL})",
                            "read - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by ({NODE_LABEL})",
                            "write - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Batch Exchange Recv (Rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_exchange_recv_row_number')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Recv @ {{%s}}" % NODE_LABEL,
                        )
                    ],
                ),
            ],
        )
    ]


def section_storage(outer_panels):
    panels = outer_panels.sub_panel()
    mv_total_size_filter = "metric='materialized_view_total_size'"
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
                panels.timeseries_kilobytes(
                    "Materialized View Size",
                    "The storage size of each materialized view",
                    [
                        panels.target(
                            f"{metric('storage_materialized_view_stats', mv_total_size_filter)}/1024",
                            "{{metric}}, mv id - {{table_id}} ",
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
                            f"sum({metric('storage_level_compact_write')}) by ({COMPONENT_LABEL}) > 0",
                            "Compaction - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by ({COMPONENT_LABEL}) > 0",
                            "Flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL})",
                            "read - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Size",
                    "Size statistics for checkpoint",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"p{legend}" + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [50, 99],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}) (rate({metric('state_store_sync_size_count')}[$__rate_interval])) > 0",
                            "avg - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming(outer_panels):
    panels = outer_panels.sub_panel()
    mv_filter = 'executor_identity=~".*MaterializeExecutor.*"'
    table_type_filter = 'table_type=~"MATERIALIZED_VIEW"'
    mv_throughput_query = f'sum(rate({metric("stream_executor_row_count", filter=mv_filter)}[$__rate_interval]) * on(actor_id) group_left(materialized_view_id, table_name) (group({metric("table_info", filter=table_type_filter)}) by (actor_id, materialized_view_id, table_name))) by (materialized_view_id, table_name)'

    return [
        outer_panels.row_collapsed(
            "Streaming",
            [
                panels.timeseries_rowsps(
                    "Source Throughput (rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput (MB/s)",
                    "The figure shows the number of bytes read by each source per second.",
                    [
                        panels.target(
                            f"(sum by (source_id)(rate({metric('partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                            "source={{source_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Backfill Throughput (rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_backfill_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Materialized View Throughput (rows/s)",
                    "The figure shows the number of rows written into each materialized executor actor per second.",
                    [
                        panels.target(
                            mv_throughput_query,
                            "materialized view {{table_name}} table_id {{materialized_view_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Throughput (rows/s)",
                    "Total number of rows that have been read from the backfill operator used by MV on MV",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_snapshot_read_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "{{table_id}}: {{table_name}} snapshot-read",
                        ),
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "{{table_id}}: {{table_name}} upstream",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Output Blocking Time Ratio (Backpressure)",
                    "We first record the total blocking duration(ns) of output buffer of each actor. It shows how "
                    "much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, "
                    "on average. Then we divide this duration by 1 second and show it as a percentage.",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, downstream_fragment_id) / 1000000000",
                            "fragment {{fragment_id}}->{{downstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Latency of Materialize Views & Sinks",
                    "The current epoch that the Materialize Executors or Sink Executor are processing. If an MV/Sink's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled `current_epoch` makes sense.
                            f"max(timestamp({metric('stream_mview_current_epoch')}) - {epoch_to_unix_millis(metric('stream_mview_current_epoch'))}/1000) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "{{table_id}} {{table_name}}",
                        ),
                        panels.target(
                            f"max(timestamp({metric('log_store_latest_read_epoch')}) - {epoch_to_unix_millis(metric('log_store_latest_read_epoch'))}/1000) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} (output)",
                        ),
                        panels.target(
                            f"max(timestamp({metric('log_store_latest_write_epoch')}) - {epoch_to_unix_millis(metric('log_store_latest_write_epoch'))}/1000) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} (enqueue)",
                        ),
                    ]
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
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency in Local Execution Mode",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
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
            "type": "datasource",
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": 'label_values(up{risingwave_name=~".+"}, namespace)',
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
        "name": "namespace",
        "options": [],
        "query": {
            "query": 'label_values(up{risingwave_name=~".+"}, namespace)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {"selected": False, "text": "risingwave", "value": "risingwave"},
        "definition": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
            "refId": "StandardVariableQuery",
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
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{NODE_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{NODE_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

job_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{COMPONENT_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{COMPONENT_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
        "refId": "StandardVariableQuery",
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
    refresh="",
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
