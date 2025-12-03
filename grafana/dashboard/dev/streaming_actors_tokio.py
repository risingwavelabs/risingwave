from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors (Tokio)",
            [
                panels.timeseries_percentage(
                    "Tokio: Actor Poll Rate Per Actor",
                    "This can be used to estimate the CPU usage of an actor",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_poll_duration')}[$__rate_interval])) by (fragment_id)"
                            f"/ on(fragment_id) sum({metric('stream_actor_count')}) by (fragment_id)"
                            f" / 1000000000",
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
                            f"sum(rate({metric('stream_actor_idle_duration')}[$__rate_interval])) by (fragment_id)"
                            f"/ on(fragment_id) sum({metric('stream_actor_count')}) by (fragment_id)"
                            f" / 1000000000",
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
                            f"sum(rate({metric('stream_actor_scheduled_duration')}[$__rate_interval])) by (fragment_id)"
                            f"/ on(fragment_id) sum({metric('stream_actor_count')}) by (fragment_id)"
                            f" / 1000000000",
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
            ],
        )
    ]
