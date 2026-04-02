from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    mv_throughput_query = f"sum(rate({metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)"

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
                        panels.target(
                            f"sum(rate({metric('file_source_input_row_count')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_name}} source_id {{source_id}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput (MB/s)",
                    "The figure shows the number of bytes read by each source per second.",
                    [
                        panels.target(
                            f"(sum by (source_id, source_name, fragment_id)(rate({metric('source_partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
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
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            mv_throughput_query,
                            "materialized view {{table_name}} table_id {{table_id}}",
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
                                  rate({metric('stream_backfill_snapshot_read_row_count', node_filter_enabled=False)}[$__rate_interval])
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
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "{{table_id}}: {{table_name}} upstream",
                        ),
                        panels.target(
                            f"""
                                sum by (table_id) (
                                    rate({metric('stream_snapshot_backfill_consume_snapshot_row_count', node_filter_enabled=False)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "{{table_id}}: {{table_name}} consume snapshot"
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
                    ],
                ),
            ],
        )
    ]
