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
            ],
        )
    ]
