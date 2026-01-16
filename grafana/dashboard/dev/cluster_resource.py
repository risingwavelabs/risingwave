from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row("[Resource] Cluster Resource"),
        *[
            panels.timeseries_memory(
                "Node Memory",
                "The memory usage of each RisingWave component.",
                [
                    panels.target(
                        f"avg({metric('process_resident_memory_bytes')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                        "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                    )
                ],
            ),
            panels.timeseries_cpu(
                "Node CPU",
                "The CPU usage of each RisingWave component.",
                [
                    panels.target(
                        f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                        "cpu usage (total) - {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                    panels.target(
                        f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                        "cpu usage (avg per core) - {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                ],
            ),
        ],
    ]
