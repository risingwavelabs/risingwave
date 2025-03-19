from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "CPU",
            [
                panels.timeseries_cpu(
                    "Node CPU Usage",
                    "The CPU usage of each RisingWave component.",
                    [
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Node CPU Core Number",
                    "Number of CPU cores per RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_cpu_core_num')}) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]
