from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Network",
            [
                panels.timeseries_bytes_per_sec(
                    "Streming Remote Exchange (Bytes/s)",
                    "Send/Recv throughput per node for streaming exchange",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Send @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Recv @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Storage Remote I/O (Bytes/s)",
                    "The remote storage read/write throughput per node",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval])) by ({NODE_LABEL})",
                            "read - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval])) by ({NODE_LABEL})",
                            "write - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_row(
                    "Batch Exchange Recv (Rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_exchange_recv_row_number')}[$__rate_interval])) by ({NODE_LABEL})",
                            "Recv @ {{%s}}" % NODE_LABEL,
                        )
                    ],
                ),
            ],
        )
    ]
