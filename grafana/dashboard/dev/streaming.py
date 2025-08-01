from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming (Source/Sink/Materialized View/Barrier)",
            [
                panels.subheader("Source"),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                        panels.target(
                            f"sum(rate({metric('file_source_input_row_count')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_name}} source_id {{source_id}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(rows).",
                    [
                        panels.target(
                            f"rate({metric('source_partition_input_count')}[$__rate_interval])",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
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
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(MB/s).",
                    [
                        panels.target(
                            f"(rate({metric('source_partition_input_bytes')}[$__rate_interval]))/(1000*1000)",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Backfill Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_backfill_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Upstream Status",
                    "Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.",
                    [
                        panels.target(
                            f"{metric('source_status_is_up')}",
                            "source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Source Split Change Events frequency(events/s)",
                    "Source Split Change Events frequency by source_id and actor_id",
                    [
                        panels.target(
                            f"rate({metric('stream_source_split_change_event_count')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Kafka Consumer Lag Size",
                    "Kafka Consumer Lag Size by source_id, partition and actor_id",
                    [
                        panels.target(
                            f"clamp_min({metric('source_kafka_high_watermark')} - on(source_id, partition) group_right() {metric('source_latest_message_id')}, 0)",
                            "source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Sink"),
                # TODO: These 2 metrics should be deprecated because they are unaware of Log Store
                # Let's remove them when all sinks are migrated to Log Store
                panels.timeseries_rowsps(
                    "Sink Executor Throughput (rows/s)",
                    "The number of rows streamed into the SinkExecutor per second. For sinks with 'sink_decouple = true', please refer to the 'Sink Metrics' section",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)",
                            "sink {{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id, actor_id) * on(actor_id) group_left(sink_name) {metric('sink_info')}",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Sink Executor Throughput (MB/s)",
                    "The figure shows the number of bytes written SinkExecutor per second. For sinks with 'sink_decouple = true', please refer to the 'Sink Metrics' section",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_sink_input_bytes')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)) / (1000*1000)",
                            "sink {{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"(sum(rate({metric('stream_sink_input_bytes')}[$__rate_interval])) by (sink_id, actor_id) * on(actor_id) group_left(sink_name) {metric('sink_info')}) / (1000*1000)",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Materialized View"),
                panels.timeseries_rowsps(
                    "Materialized View Throughput (rows/s)",
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "mview {{table_id}} {{table_name}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_mview_input_row_count')}[$__rate_interval]) * on(fragment_id, table_id) group_left(table_name) {metric('table_info')}",
                            "mview {{table_id}} {{table_name}} - actor {{actor_id}} fragment_id {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Snapshot-Read Throughput (rows/s)",
                    "Rows/sec that we read from the backfill snapshot by materialized view",
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
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_snapshot_backfill_consume_snapshot_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} {{stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Upstream Throughput (rows/s)",
                    "Total number of rows that have been output from the backfill upstream",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Current Epoch of Materialize Views",
                    "The current epoch that the Materialize Executors are processing. If an MV's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled `current_epoch` makes sense.
                            f"min({metric('stream_mview_current_epoch')} != 0) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "{{table_id}} {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Snapshot Backfill Lag",
                    "",
                    [
                        panels.target(
                            f"{metric('meta_snapshot_backfill_upstream_lag')} / (2^16) / 1000",
                            "lag @ {{table_id}}",
                        ),
                    ],
                ),
                panels.subheader(
                    "Barrier",
                    "More fine-grained barrier metrics here. Core metrics like **Barrier Number** are in **Cluster Essential** section.",
                    height=2.2,
                ),
                panels.timeseries_latency(
                    "Barrier Send Latency",
                    "The duration between the time point when the scheduled barrier needs to be sent and the time point when "
                    "the barrier gets actually sent to all the compute nodes. Developers can thus detect any internal "
                    "congestion.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_send_duration_seconds_bucket')}[$__rate_interval])) by (le, database_id))",
                            f"barrier_send_latency_p{legend}" + " {{database_id}}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_send_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_send_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_send_latency_avg {{database_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier In-Flight Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_inflight_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_inflight_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"max(sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval]))) > 0",
                            "barrier_inflight_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Sync Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                            f"barrier_sync_latency_p{legend}"
                            + " - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval])) > 0",
                            "barrier_sync_latency_avg - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Wait Commit Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_wait_commit_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_wait_commit_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_wait_commit_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_wait_commit_avg",
                        ),
                    ]
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_barrier_wait_commit_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_upstream_wait_progress_latency_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_upstream_wait_progress_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Earliest In-Flight Barrier Progress",
                    "The number of actors that have processed the earliest in-flight barriers per second. "
                    "This metric helps users to detect potential congestion or stuck in the system.",
                    [
                        panels.target(
                            f"rate({metric('stream_barrier_manager_progress')}[$__rate_interval])",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]
