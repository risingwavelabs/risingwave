from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Iceberg Metrics",
            [
                panels.timeseries_ops(
                    "Write Qps Of Iceberg Writer",
                    "iceberg write qps",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_write_qps')}[$__rate_interval])) by (actor_id, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Latency Of Iceberg Writer",
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
                panels.timeseries_count(
                    "Iceberg rolling unfushed data file",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_rolling_unflushed_data_file')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg position delete cache num",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_position_delete_cache_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg partition num",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_partition_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
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
            ],
        )
    ]
