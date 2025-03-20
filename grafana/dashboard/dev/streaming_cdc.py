from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming CDC",
            [
                panels.timeseries_rowsps(
                    "CDC Backfill Snapshot Read Throughput(rows)",
                    "Total number of rows that have been read from the cdc backfill snapshot",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "CDC Backfill Upstream Throughput(rows)",
                    "Total number of rows that have been output from the cdc backfill upstream",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "CDC Consume Lag Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('source_cdc_event_lag_duration_milliseconds_bucket')}[$__rate_interval])) by (le, table_name))",
                                f"lag p{legend}" + " - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "CDC Source Errors",
                    "",
                    [
                        panels.target(
                            f"sum({metric('cdc_source_error')}) by (connector_name, source_id, error_msg)",
                            "{{connector_name}}: {{error_msg}} ({{source_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Failure Count",
                    "Total number of failed auto schema change attempts of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_failure_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Success Count",
                    "Total number of succeeded auto schema change of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_success_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Auto Schema Change Latency (sec)",
                    "Latency of Auto Schema Change Process",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('auto_schema_change_latency_bucket')}[$__rate_interval])) by (le, table_id, table_name))",
                                f"lag p{legend}" + "{{table_id}} - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        ),
    ]
