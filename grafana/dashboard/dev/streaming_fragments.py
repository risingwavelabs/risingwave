from ..common import *
from . import section
from .streaming_common import (
    _actor_busy_rate_expr,
    _actor_busy_rate_target,
    _actor_busy_time_relative_target,
)

def _fragment_topk_percent_expr(expr: str) -> str:
    """Wrap an expression as top-k percent for fragment tables."""
    return f"topk(10, ({expr}) * 100)"

def _fragment_peak_rate_with_id_at_step(expr: str) -> str:
    """Compute the peak rate over the dashboard range and attach `id` from fragment_id."""
    return (
        f"label_replace("
        f"(max_over_time(({expr})[$__range:$__interval])), 'id', '$1', 'fragment_id', '(.+)')"
    )

def _fragment_peak_rate_per_actor_with_id_at_step(per_actor_expr: str) -> str:
    """Compute the peak per-actor rate (normalized to seconds) and attach `id`."""
    return (
        f"label_replace("
        f"(max_over_time((({per_actor_expr}) / 1000000000)[$__range:$__interval])), "
        f"'id', '$1', 'fragment_id', '(.+)')"
    )

@section
def _(outer_panels: Panels):
    # The actor_id can be masked due to metrics level settings.
    # We use this filter to suppress the actor-level panels if applicable.
    actor_level_filter = "actor_id!=''"
    panels = outer_panels.sub_panel()
    actor_count_expr = (
        f"clamp_min(sum({metric('stream_actor_count')}) by (fragment_id), 1)"
    )
    poll_duration_rate_expr = (
        f"sum(rate({metric('stream_actor_poll_duration')}[$__rate_interval])) by (fragment_id)"
    )
    poll_duration_expr = (
        f"{poll_duration_rate_expr} / on(fragment_id) {actor_count_expr}"
    )
    idle_duration_rate_expr = (
        f"sum(rate({metric('stream_actor_idle_duration')}[$__rate_interval])) by (fragment_id)"
    )
    idle_duration_expr = (
        f"{idle_duration_rate_expr} / on(fragment_id) {actor_count_expr}"
    )
    scheduled_duration_rate_expr = (
        f"sum(rate({metric('stream_actor_scheduled_duration')}[$__rate_interval])) by (fragment_id)"
    )
    scheduled_duration_expr = (
        f"{scheduled_duration_rate_expr} / on(fragment_id) {actor_count_expr}"
    )
    return [
        outer_panels.row_collapsed(
            "Streaming Fragments",
            [
                panels.subheader("Overview"),
                panels.table_info(
                    "Top Fragments by Busy Rate",
                    "Top 10 fragments with the highest peak busy rate (%).",
                    [
                        panels.table_target(
                            _fragment_topk_percent_expr(
                                _fragment_peak_rate_with_id_at_step(
                                    _actor_busy_rate_expr("$__rate_interval")
                                )
                            )
                        )
                    ],
                    ["id", "Value"],
                    dict.fromkeys(["Time", "fragment_id"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.table_info(
                    "Top Fragments by CPU Rate",
                    "Top 10 fragments with the highest peak CPU rate (%).",
                    [
                        panels.table_target(
                            _fragment_topk_percent_expr(
                                _fragment_peak_rate_per_actor_with_id_at_step(
                                    poll_duration_expr
                                )
                            )
                        )
                    ],
                    ["id", "Value"],
                    dict.fromkeys(["Time", "fragment_id"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.table_info(
                    "Top Fragments by Idle Rate",
                    "Top 10 fragments with the highest peak idle rate (%).",
                    [
                        panels.table_target(
                            _fragment_topk_percent_expr(
                                _fragment_peak_rate_per_actor_with_id_at_step(
                                    idle_duration_expr
                                )
                            )
                        )
                    ],
                    ["id", "Value"],
                    dict.fromkeys(["Time", "fragment_id"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.table_info(
                    "Top Fragments by Scheduling Delay Rate",
                    "Top 10 fragments with the highest peak scheduling delay rate (%).",
                    [
                        panels.table_target(
                            _fragment_topk_percent_expr(
                                _fragment_peak_rate_per_actor_with_id_at_step(
                                    scheduled_duration_expr
                                )
                            )
                        )
                    ],
                    ["id", "Value"],
                    dict.fromkeys(["Time", "fragment_id"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.subheader("Busy Rate (IO + CPU Usage) by Fragment"),
                panels.timeseries_percentage(
                    "Actor Busy Rate",
                    "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream.",
                    [
                        _actor_busy_rate_target(panels, "$__rate_interval"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Busy Rate (10m)",
                    "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 10 minutes.",
                    [
                        _actor_busy_rate_target(panels, "10m"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Busy Rate (5m)",
                    "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 5 minutes.",
                    [
                        _actor_busy_rate_target(panels, "5m"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Busy Rate (3m)",
                    "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 3 minutes.",
                    [
                        _actor_busy_rate_target(panels, "3m"),
                    ],
                ),
                panels.timeseries_latency_ns(
                    "Actor Busy Time (Relative)",
                    "The relative busy time of an actor, i.e. the time that an actor is not blocked by its downstream or upstream.",
                    [
                        _actor_busy_time_relative_target(panels),
                    ],
                ),
                panels.subheader("CPU, Idle Time, Scheduling Delay by Actor (Provided by Tokio)"),
                panels.timeseries_percentage(
                    "Tokio: CPU Usage (Actor Poll Rate Per Actor)",
                    "This can be used to estimate the CPU usage of an actor",
                    [
                        panels.target(
                            f"{poll_duration_expr} / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Tokio: Actor Poll Avg Rate Per Poll",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) / (rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0) / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Tokio: Actor Idle Rate Per Actor",
                    "Idle time could be due to no data to process, or waiting for async operations like IO",
                    [
                        panels.target(
                            f"{idle_duration_expr} / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Idle Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Tokio: Actor Idle Avg Rate Per Idle",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) / (rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0) / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Tokio: Actor Scheduling Delay Rate Per Actor",
                    "Scheduling delay could be due to poor scheduling priority, or a lack of CPU resources - for instance if there are long polling durations, can be mitigated by scaling up the number of worker threads, or improving the concurrency of the operator",
                    [
                        panels.target(
                            f"{scheduled_duration_expr} / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Scheduled Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Tokio: Actor Scheduled Avg Rate Per Scheduled",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) / (rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0) / 1000000000",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Throughput by Fragment"),
                panels.timeseries_rowsps(
                    "Actor Input Throughput (rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_in_record_cnt')}[$__rate_interval])) by (fragment_id, upstream_fragment_id)",
                            "fragment total {{fragment_id}}<-{{upstream_fragment_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_actor_in_record_cnt', actor_level_filter)}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Actor Output Throughput (rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_out_record_cnt')}[$__rate_interval])) by (fragment_id)",
                            "fragment total {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_actor_out_record_cnt', actor_level_filter)}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Backpressure by Fragment"),
                panels.timeseries_percentage(
                    "Actor Output Blocking Rate (Downstream Backpressure)",
                    "The rate that an actor is blocked by its downstream.",
                    [
                        # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                        # Thus to calculate the average, we need to manually divide the actor count.
                        #
                        # Note: actor_count is equal to the number of dispatchers for a given downstream fragment,
                        # this holds true as long as we don't support multiple edges between two fragments.
                        panels.target(
                            f"sum(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, downstream_fragment_id) \
                                / ignoring (downstream_fragment_id) group_left sum({metric('stream_actor_count')}) by (fragment_id) \
                                / 1000000000",
                            "fragment {{fragment_id}}->{{downstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Input Blocking Rate (Upstream Backpressure)",
                    "The rate that an actor is blocked by its upstream.",
                    [
                        # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                        # Thus to calculate the average, we need to manually divide the actor count.
                        panels.target(
                            f"sum(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, upstream_fragment_id) \
                                / ignoring (upstream_fragment_id) group_left sum({metric('stream_actor_count')}) by (fragment_id) \
                                / 1000000000",
                            "fragment {{fragment_id}}<-{{upstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Current Epoch of Actors"),
                panels.timeseries_epoch(
                    "Current Epoch of Actors",
                    "The current epoch that the actors are processing. If an actor's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled epochs makes sense.
                            f"min({metric('stream_actor_current_epoch')} != 0) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
