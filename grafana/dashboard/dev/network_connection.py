from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    s3_filter = 'connection_type="S3"'
    grpc_filter = 'connection_type=~"grpc.*"'
    return [
        outer_panels.row_collapsed(
            "Network connection",
            [
                panels.timeseries_bytesps(
                    "Network throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "S3 throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "gRPC throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "IO error rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_io_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} S3 {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} grpc {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} total {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Existing connection count",
                    "",
                    [
                        panels.target(
                            f"sum({metric('connection_count', filter=s3_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('connection_count', filter=grpc_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection err rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        )
    ]
