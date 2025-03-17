from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors (Tokio)",
            [
                panels.timeseries_actor_latency(
                    "Actor Execution Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_actor_execution_time')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Fast Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Slow Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Idle Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) / rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Scheduled Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) / rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
