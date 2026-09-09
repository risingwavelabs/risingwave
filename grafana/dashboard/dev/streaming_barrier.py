from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Barrier",
            [
                panels.subheader(
                    "Barrier",
                    "More fine-grained barrier metrics here. Core metrics like **Barrier Number** and **Barrier Latency** are in **Cluster Essential** section.",
                    height=2.2,
                ),
                panels.timeseries_latency(
                    "Barrier In-Flight Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_inflight_duration_seconds_bucket')}[$__rate_interval])) by (le, partial_graph))",
                            f"barrier_inflight_latency_p{legend}"
                            + " {{partial_graph}}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"max by (partial_graph) (sum by (partial_graph, {NODE_LABEL}) (rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval])) / sum by (partial_graph, {NODE_LABEL}) (rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval]))) > 0",
                            "barrier_inflight_latency_avg {{partial_graph}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Sync Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le, partial_graph, {NODE_LABEL}))",
                            f"barrier_sync_latency_p{legend}"
                            + " {{partial_graph}} - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"sum by(partial_graph, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval])) / sum by(partial_graph, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval])) > 0",
                            "barrier_sync_latency_avg {{partial_graph}} - {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Wait Commit Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_wait_commit_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_wait_commit_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_wait_commit_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_wait_commit_avg",
                        ),
                    ]
                    # Retain this query for compatibility with pre-v3.0 clusters, where the metric
                    # is still produced.
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_barrier_wait_commit_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Earliest In-Flight Barrier Progress",
                    "The number of actors that have processed the earliest in-flight barriers per second. "
                    "This metric helps users to detect potential congestion or stuck in the system.",
                    [
                        panels.target(
                            f"rate({metric('stream_barrier_manager_progress')}[$__rate_interval])",
                            "{{partial_graph}} - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Barrier Interval",
                    "Barrier interval of each database in milliseconds",
                    [
                        panels.target(
                            f"{metric('meta_barrier_interval_by_database')}",
                            "barrier_interval {{database_id}}"
                        ),
                    ],
                ),
                panels.subheader(
                    "Temporal Filter NOW()",
                    "Observability for the per-fragment streaming `NOW()` clock exposed by "
                    "`NowExecutor`. When `streaming.developer.now_progress_ratio` is set, the "
                    "streaming clock advances at a bounded rate per barrier, which can trail "
                    "wall time if the barrier interval changes without a recovery. A steadily "
                    "growing **Wall-Clock Drift** panel means downstream temporal filters "
                    "(`col < NOW()`, `col <= NOW()`) will delay eligible rows.",
                    height=2.5,
                ),
                panels.timeseries_ms(
                    "Temporal Filter NOW() vs Wall Clock Drift",
                    "Milliseconds by which the streaming `NOW()` value lags the barrier's "
                    "wall-clock epoch. Zero or near-zero is healthy. A monotonically "
                    "increasing series indicates the streaming clock is falling behind and "
                    "the fragment likely needs a RECOVER after a `barrier_interval_ms` change.",
                    [
                        panels.target(
                            f"{metric('stream_now_wall_clock_drift_ms')}",
                            "drift ms - fragment {{fragment_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"max by (fragment_id) ({metric('stream_now_wall_clock_drift_ms')})",
                            "max drift ms - fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_ms(
                    "Temporal Filter NOW() Streaming Clock",
                    "The most recent streaming `NOW()` value emitted by each `NowExecutor`, "
                    "expressed as milliseconds since the Unix epoch. Compare against the "
                    "dashboard time to see when a fragment's clock stops advancing.",
                    [
                        panels.target(
                            f"{metric('stream_now_streaming_clock_ms')}",
                            "streaming NOW() ms - fragment {{fragment_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
