from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    operation_rate_blacklist = "type!~'streaming_read'"
    operation_duration_blacklist = "type!~'streaming_read'"
    put_op_types = ["upload", "streaming_upload"]
    post_op_types = [
        "streaming_upload_init",
        "streaming_upload_finish",
        "delete_objects",
    ]
    list_op_types = ["list"]
    delete_op_types = ["delete"]
    read_op_types = ["read", "streaming_read_init", "metadata"]
    write_op_filter = f"type=~'({'|'.join(put_op_types + post_op_types + list_op_types + delete_op_types)})'"
    read_op_filter = f"type=~'({'|'.join(read_op_types)})'"
    return [
        outer_panels.row_collapsed(
            "Object Storage",
            [
                panels.timeseries_bytes_per_sec(
                    "Operation Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "read - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "write - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Operation Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_latency_bucket', operation_duration_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                "{{type}}"
                                + f" p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('object_store_operation_latency_sum', operation_duration_blacklist)}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('object_store_operation_latency_count')}[$__rate_interval])) > 0",
                            "{{type}} avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', operation_rate_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', write_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-write - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', read_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-read - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Operation Size",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_bytes_bucket')}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "{{type}}"
                            + f" p{legend}"
                            + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        [50, 99, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Operation Failure Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Retry Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_request_retry_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Realtime)",
                    "There are two types of operations: 1. GET, SELECT, and DELETE, they cost 0.0004 USD per 1000 "
                    "requests. 2. PUT, COPY, POST, LIST, they cost 0.005 USD per 1000 requests."
                    "Reading from S3 across different regions impose extra cost. This metric assumes 0.01 USD per 1GB "
                    "data transfer. Please checkout AWS's pricing model for more accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('object_store_read_bytes')}) * 0.01 / 1000 / 1000 / 1000",
                            "(Cross Region) Data Transfer Cost",
                            True,
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', read_op_filter)}) * 0.0004 / 1000",
                            "GET, SELECT, and all other Requests Cost",
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', write_op_filter)}) * 0.005 / 1000",
                            "PUT, COPY, POST, LIST Requests Cost",
                        ),
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Monthly)",
                    "This metric uses the total size of data in S3 at this second to derive the cost of storing data "
                    "for a whole month. The price is 0.023 USD per GB. Please checkout AWS's pricing model for more "
                    "accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}) * 0.023 / 1000 / 1000",
                            "Monthly Storage Cost",
                        ),
                    ],
                ),
            ],
        )
    ]
