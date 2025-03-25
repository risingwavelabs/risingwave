from ..common import *
from . import section


@section
def _(panels: Panels):
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
                    f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, database_id))",
                    f"barrier_latency_p{legend}" + " (database {{database_id}})",
                ),
                [50, 99],
            )
            + [
                panels.target(
                    f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval]) > 0",
                    "barrier_latency_avg (database {{database_id}})",
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
                    "Recovery Triggered {{recovery_type}}",
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
