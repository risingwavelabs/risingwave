from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "User Defined Function",
            [
                panels.timeseries_count(
                    "UDF Calls Count",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_success_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_success_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_failure_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_failure_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_retry_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_retry_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_success_count')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_success_count - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_failure_count')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_failure_count - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_retry_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_retry_count - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "UDF Input Chunk Rows",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('udf_input_chunk_rows_sum')}[$__rate_interval])) by (link, name, fragment_id) / sum(irate({metric('udf_input_chunk_rows_count')}[$__rate_interval])) by (link, name, fragment_id) > 0",
                            "udf_input_chunk_rows_avg - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "UDF Latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('udf_latency_sum')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / sum(irate({metric('udf_latency_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                            "udf_latency_avg - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, link, name, fragment_id))",
                            "udf_latency_p99_by_name - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(irate({metric('udf_latency_sum')}[$__rate_interval])) by (link, name, fragment_id) / sum(irate({metric('udf_latency_count')}[$__rate_interval])) by (link, name, fragment_id) > 0",
                            "udf_latency_avg_by_name - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "UDF Throughput (rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_input_rows')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_throughput_rows - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_input_rows')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_throughput_rows - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "UDF Throughput (bytes)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_input_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "udf_throughput_bytes - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_input_bytes')}[$__rate_interval])) by (link, name, fragment_id) / (1024*1024)",
                            "udf_throughput_bytes - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "UDF Memory Usage (bytes)",
                    "Currently only embedded JS UDF supports this. Others will always show 0.",
                    [
                        panels.target(
                            f"sum({metric('udf_memory_usage')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_memory_usage - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('udf_memory_usage')}) by (name, fragment_id)",
                            "udf_memory_usage - {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
