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
                    "More fine-grained barrier metrics here. Core metrics like **Barrier Number** are in **Cluster Essential** section.",
                    height=2.2,
                ),
                panels.timeseries_latency(
                    "Barrier Latency",
                    "The time that the data between two consecutive barriers gets fully processed, i.e. the computation "
                    "results are made durable into materialized views or sink to external systems. This metric shows to users "
                    "the freshness of materialized views.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, database_id))",
                            f"barrier_latency_p{legend} " + " (database {{database_id}})",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_latency_avg (database {{database_id}})",
                        ),
                    ]
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id, barrier_type))",
                            f"snapshot_backfill_barrier_latency_p{legend} table_id[{{{{table_id}}}}] {{{{barrier_type}}}}",
                        ),
                        [50, 90, 99, 999, "max"],
                    ),
                ),
                panels.timeseries_latency(
                    "Barrier Send Latency",
                    "The duration between the time point when the scheduled barrier needs to be sent and the time point when "
                    "the barrier gets actually sent to all the compute nodes. Developers can thus detect any internal "
                    "congestion.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_send_duration_seconds_bucket')}[$__rate_interval])) by (le, database_id))",
                            f"barrier_send_latency_p{legend}" + " {{database_id}}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_send_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_send_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_send_latency_avg {{database_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier In-Flight Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_inflight_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_inflight_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"max(sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval]))) > 0",
                            "barrier_inflight_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Sync Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                            f"barrier_sync_latency_p{legend}"
                            + " - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval])) > 0",
                            "barrier_sync_latency_avg - {{%s}}" % NODE_LABEL,
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
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_barrier_wait_commit_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_upstream_wait_progress_latency_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_upstream_wait_progress_latency_p{legend} table_id[{{{{table_id}}}}]",
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
                            "{{%s}}" % NODE_LABEL,
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
            ],
        )
    ]
