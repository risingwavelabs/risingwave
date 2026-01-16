from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Memory manager",
            [
                panels.timeseries_count(
                    "LRU manager loop count per sec",
                    "",
                    [
                        panels.target(
                            f"rate({metric('lru_runtime_loop_count')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager eviction policy",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_eviction_policy')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager sequence",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_latest_sequence')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('lru_watermark_sequence')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The allocated memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The resident memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_resident_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The metadata memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_metadata_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The allocated memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ms(
                    "LRU manager diff between current watermark and evicted watermark time (ms) for actors",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_current_watermark_time_ms')} - on() group_right() {metric('lru_evicted_watermark_time_ms')}",
                            "table {{table_id}} actor {{actor_id}} desc: {{desc}}",
                        ),
                    ],
                ),
            ],
        ),
    ]
