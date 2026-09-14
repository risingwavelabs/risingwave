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
                    "`NowExecutor`. When `streaming.developer.now_progress_ratio > 1`, clock "
                    "advancement is bounded using the barrier interval captured at startup. "
                    "Increasing the runtime interval beyond that bound can make `NOW()` lag "
                    "processed barrier timestamps and delay temporal-filter output. Inspect "
                    "both the streaming timestamp and barrier progress when diagnosing stalls.",
                    height=2.5,
                ),
                panels.timeseries_ms(
                    "Temporal Filter NOW() vs Wall Clock Drift",
                    "Milliseconds by which streaming `NOW()` lags the latest processed "
                    "barrier epoch. Zero means it has caught up to that barrier, not "
                    "necessarily to current wall time. This gauge retains its previous "
                    "value while watermark emission is paused or stalled. A growing series "
                    "alone does not establish that recovery is required. For drift caused "
                    "by a stale `barrier_interval_ms`, RECOVER reloads the interval but "
                    "restores the checkpointed clock; catch-up is gradual as barrier "
                    "progress permits, rather than an immediate reset to zero.",
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
