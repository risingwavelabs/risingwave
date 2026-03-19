from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Iceberg Metrics",
            [
                panels.subheader("Writer"),
                panels.timeseries_ops(
                    "Write QPS of Iceberg Writer",
                    "iceberg write qps",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_write_qps')}[$__rate_interval])) by (actor_id, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Latency of Iceberg Writer",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_write_latency_bucket')}[$__rate_interval])) by (le, sink_id, sink_name))",
                                f"p{legend}" + " @ {{sink_id}} {{sink_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, job, instance, sink_id, sink_name)(rate({metric('iceberg_write_latency_sum')}[$__rate_interval])) / sum by(le, type, job, instance, sink_id, sink_name) (rate({metric('iceberg_write_latency_count')}[$__rate_interval])) > 0",
                            "avg @ {{sink_id}} {{sink_name}}",
                        ),
                    ],
                ),
                panels.subheader("Writer State"),
                panels.timeseries_count(
                    "Iceberg Rolling Unflushed Data Files",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_rolling_unflushed_data_file')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg Position Delete Cache Count",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_position_delete_cache_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg Partition Count",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_partition_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Read/Write Size"),
                panels.timeseries_bytes(
                    "Iceberg Write Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('iceberg_write_bytes')}) by (sink_name)",
                            "write @ {{sink_name}}",
                        ),
                        panels.target(
                            f"sum({metric('iceberg_write_bytes')})",
                            "total write",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Iceberg Read Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('iceberg_read_bytes')}) by (table_name)",
                            "read @ {{table_name}}",
                        ),
                        panels.target(
                            f"sum({metric('iceberg_read_bytes')})",
                            "total read",
                        ),
                    ],
                ),
                panels.subheader("Snapshots"),
                panels.timeseries_count(
                    "Iceberg Snapshot Number",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_snapshot_num')}",
                            "{{sink_name}} @ {{catalog_name}} {{table_name}}",
                        ),
                    ],
                ),
                panels.subheader("Source Ingestion"),
                panels.timeseries_latency(
                    "Iceberg Source Snapshot Lag",
                    "Time difference between latest available snapshot and last ingested snapshot",
                    [
                        panels.target(
                            f"{metric('iceberg_source_snapshot_lag_seconds')}",
                            "{{source_name}} ({{source_id}}) @ {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Source Snapshots Discovered",
                    "Rate of new snapshots discovered via incremental scan",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_snapshots_discovered_total')}[$__rate_interval])) by (source_id, source_name, table_name)",
                            "{{source_name}} ({{source_id}}) @ {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Iceberg Source List Duration",
                    "Time spent planning files from a snapshot",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_source_list_duration_seconds_bucket')}[$__rate_interval])) by (le, source_id, source_name, table_name))",
                                f"p{legend}" + " @ {{source_name}} ({{source_id}}) {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Source Files Discovered",
                    "Rate of files discovered per scan by type",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_files_discovered_total')}[$__rate_interval])) by (source_id, source_name, table_name, file_type)",
                            "{{file_type}} @ {{source_name}} ({{source_id}}) {{table_name}}",
                        ),
                    ],
                ),
                panels.subheader("Source File Reading"),
                panels.timeseries_latency(
                    "Iceberg Source File Read Duration",
                    "Per-file read duration",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_source_file_read_duration_seconds_bucket')}[$__rate_interval])) by (le, table_name))",
                                f"p{legend}" + " @ {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Source Rows Read",
                    "Rate of rows read from Iceberg source",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_rows_read_total')}[$__rate_interval])) by (table_name)",
                            "{{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Source Files Read",
                    "Rate of files read from Iceberg source by type",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_files_read_total')}[$__rate_interval])) by (table_name, file_type)",
                            "{{file_type}} @ {{table_name}}",
                        ),
                    ],
                ),
                panels.subheader("Source Delete Handling"),
                panels.timeseries_ops(
                    "Iceberg Source Delete Rows Applied",
                    "Rate of rows removed by delete processing",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_delete_rows_applied_total')}[$__rate_interval])) by (table_name, delete_type)",
                            "{{delete_type}} @ {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg Source Delete Files Per Data File",
                    "Distribution of delete files attached per data file scan task",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_source_delete_files_per_data_file_bucket')}[$__rate_interval])) by (le, source_id, source_name, table_name))",
                                f"p{legend}" + " @ {{source_name}} ({{source_id}}) {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.subheader("Source Operational Health"),
                panels.timeseries_count(
                    "Iceberg Source Checkpoint File Count",
                    "Number of files tracked in checkpoint state table",
                    [
                        panels.target(
                            f"{metric('iceberg_source_checkpoint_file_count')}",
                            "{{source_name}} ({{source_id}}) @ {{table_name}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iceberg Source Scan Errors",
                    "Rate of scan errors categorized by type",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_source_scan_errors_total')}[$__rate_interval])) by (source_id, source_name, table_name, error_type)",
                            "{{error_type}} @ {{source_name}} ({{source_id}}) {{table_name}}",
                        ),
                    ],
                ),
            ],
        )
    ]
