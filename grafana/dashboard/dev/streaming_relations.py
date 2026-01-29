from ..common import *
from . import section
from .streaming_common import _actor_busy_rate_expr

def _relation_busy_rate_expr_by_mv(rate_interval: str):
    """Return per-relation busy rate (by materialized_view_id), based on busiest actor."""
    actor_busy_rate_expr = _actor_busy_rate_expr(rate_interval)
    return (
        f"topk(1,"
        f"  ({actor_busy_rate_expr}) * on (fragment_id) group_right {metric('table_info')}"
        f") by (materialized_view_id)"
    )

def _relation_busy_rate_expr(rate_interval: str):
    """Return per-relation busy rate with relation metadata labels attached."""
    relation_busy_rate_expr = _relation_busy_rate_expr_by_mv(rate_interval)
    relation_busy_rate_with_metadata_expr = (
        f"label_replace(({relation_busy_rate_expr}), 'id', '$1', 'materialized_view_id', '(.+)')"
        f"* on (id) group_left (name, type) {metric('relation_info')}"
    )
    return relation_busy_rate_with_metadata_expr

def _relation_metric_with_id(expr: str) -> str:
    """Attach `id` label from materialized_view_id for relation-level tables."""
    return f"label_replace(({expr}), 'id', '$1', 'materialized_view_id', '(.+)')"

def _relation_metric_with_metadata(expr: str) -> str:
    """Join relation_info to add name/type labels after peak computation."""
    return f"({expr}) * on (id) group_left (name, type) {metric('relation_info')}"

def _relation_busy_peak_topk_expr(rate_interval: str) -> str:
    """Compute top-k peak busy rate for relations over the dashboard range."""
    per_mv_busy_expr = _relation_busy_rate_expr_by_mv(rate_interval)
    per_mv_busy_with_id_expr = _relation_metric_with_id(per_mv_busy_expr)
    with_metadata_expr = _relation_metric_with_metadata(per_mv_busy_with_id_expr)
    peak_expr = f"max_over_time(({with_metadata_expr})[$__range:$__interval])"
    return f"topk(10, (100 * ({peak_expr})))"

def _relation_cpu_peak_topk_expr(
    poll_duration_rate_expr: str, actor_count_expr: str
) -> str:
    """Compute top-k peak CPU rate for relations, normalized by actor count."""
    per_fragment_rate_expr = (
        f"({poll_duration_rate_expr}) / on(fragment_id) {actor_count_expr}"
    )
    per_mv_rate_expr = _sum_fragment_metric_by_mv(per_fragment_rate_expr)
    per_mv_rate_expr = f"({per_mv_rate_expr}) / 1000000000"
    per_mv_rate_with_id_expr = (
        f"label_replace(({per_mv_rate_expr}), "
        f"'id', '$1', 'materialized_view_id', '(.+)')"
    )
    with_metadata_expr = (
        f"({per_mv_rate_with_id_expr}) * on (id) group_left (name, type)"
        f" {metric('relation_info')}"
    )
    peak_expr = (
        f"max_over_time(({with_metadata_expr})[$__range:$__interval])"
    )
    return f"topk(10, (100 * ({peak_expr})))"

def _relation_busy_rate_target(panels: Panels, rate_interval: str):
    return panels.target(
        _relation_busy_rate_expr(rate_interval),
        "name {{name}} id {{id}} type {{type}} fragment {{fragment_id}}",
    )


def _sum_fragment_metric_by_mv(expr: str) -> str:
    return (
        f"sum(({expr})"
        f"* on(fragment_id) group_left(materialized_view_id)"
        f"max by (fragment_id, materialized_view_id) ({metric('table_info')}))"
        f"by (materialized_view_id)"
    )

@section
def _(outer_panels: Panels):
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
    return [
        outer_panels.row_collapsed(
            "Streaming Relation Metrics",
            [
                panels.subheader("Overview"),
                panels.table_info(
                    "Top Relations by Busy Rate",
                    "Top 10 relations with the highest peak busy rate (%).",
                    [
                        panels.table_target(
                            _relation_busy_peak_topk_expr("$__rate_interval")
                        )
                    ],
                    ["id", "name", "Value", "type"],
                    dict.fromkeys(["Time"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.table_info(
                    "Top Relations by CPU Rate",
                    "Top 10 relations with the highest peak CPU rate (%).",
                    [
                        panels.table_target(
                            _relation_cpu_peak_topk_expr(
                                poll_duration_rate_expr, actor_count_expr
                            )
                        )
                    ],
                    ["id", "name", "Value", "type"],
                    dict.fromkeys(["Time"], True),
                    {"Value": "rate"},
                    {"rate": "percent"},
                ),
                panels.subheader("CPU Usage By Relation"),
                panels.timeseries_percentage(
                    "CPU Usage Per Streaming Job",
                    "The figure shows the CPU usage of each streaming job",
                    [
                        panels.target(
                            f"label_replace("
                            f"({_sum_fragment_metric_by_mv(poll_duration_expr)}"
                            f"/ 1000000000), "
                            f"'id', '$1', 'materialized_view_id', '(.*)'"
                            f") * on(id) group_left(name, type) {metric('relation_info')}",
                            "{{type}} {{name}} id {{id}}",
                        )
                    ],
                ),
                panels.subheader("Busy Rate By Relation"),
                panels.timeseries_percentage(
                    "Relation Busy Rate",
                    "The rate that a relation is busy, i.e. the busy rate of its busiest actor.",
                    [
                        _relation_busy_rate_target(panels, "$__rate_interval"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Relation Busy Rate (10m)",
                    "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 10 minutes.",
                    [
                        _relation_busy_rate_target(panels, "10m"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Relation Busy Rate (5m)",
                    "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 5 minutes.",
                    [
                        _relation_busy_rate_target(panels, "5m"),
                    ],
                ),
                panels.timeseries_percentage(
                    "Relation Busy Rate (3m)",
                    "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 3 minutes.",
                    [
                        _relation_busy_rate_target(panels, "3m"),
                    ],
                ),
                panels.subheader("Latency By Relation"),
                # FIXME(kwannoel): We should use the max timestamp of the database, rather than cluster level.
                panels.timeseries_latency(
                    "Latency of Materialize Views & Sinks",
                    "The current epoch that the Materialize Executors or Sink Executor are processing. If an MV/Sink's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled `current_epoch` makes sense.
                            f"max(timestamp({metric('stream_mview_current_epoch')}) - {epoch_to_unix_millis(metric('stream_mview_current_epoch'))}/1000) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "{{table_id}} {{table_name}}",
                        ),
                        panels.target(
                            f"max(timestamp({metric('log_store_latest_read_epoch')}) - {epoch_to_unix_millis(metric('log_store_latest_read_epoch'))}/1000) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} (output)",
                        ),
                        panels.target(
                            f"max(timestamp({metric('log_store_latest_write_epoch')}) - {epoch_to_unix_millis(metric('log_store_latest_write_epoch'))}/1000) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} (enqueue)",
                        ),
                    ],
                ),
                panels.subheader("Epoch By Relation"),
                panels.timeseries_epoch(
                    "Current Epoch of Materialize Views",
                    "The current epoch that the Materialize Executors are processing. If an MV's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled `current_epoch` makes sense.
                            f"min({metric('stream_mview_current_epoch')} != 0) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "{{table_id}} {{table_name}}",
                        ),
                    ],
                ),
                panels.subheader("Throughput By Relation"),
                panels.timeseries_rowsps(
                    "Materialized View Throughput (rows/s)",
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "mview {{table_id}} {{table_name}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_mview_input_row_count')}[$__rate_interval]) * on(fragment_id, table_id) group_left(table_name) {metric('table_info')}",
                            "mview {{table_id}} {{table_name}} - actor {{actor_id}} fragment_id {{fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Memory Usage By Relation"),
                panels.timeseries_bytes(
                    "Total Memory Usage",
                    "Executor cache + shared buffer imm size aggregated by relation.",
                    [
                        panels.target(
                            _relation_metric_with_metadata(
                                "("
                                + relabel_materialized_view_id_as_id(
                                    f"sum({metric('stream_memory_usage')} * on(table_id) group_left(materialized_view_id) {metric('table_info')}) by (materialized_view_id)"
                                )
                                + " + "
                                + relabel_materialized_view_id_as_id(
                                    _sum_fragment_metric_by_mv(
                                        metric("state_store_per_fragment_imm_size")
                                    )
                                )
                                + ")"
                            ),
                            "relation {{name}} (id={{id}} type={{type}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage of Materialized Views",
                    "Memory usage aggregated by materialized views",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')} * on(table_id) group_left(materialized_view_id) {metric('table_info')}) by (materialized_view_id)",
                            "materialized view {{materialized_view_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Shared Buffer Memory Usage",
                    "Shared buffer imm size aggregated by relation.",
                    [
                        panels.target(
                            _relation_metric_with_metadata(
                                relabel_materialized_view_id_as_id(
                                    _sum_fragment_metric_by_mv(
                                        metric("state_store_per_fragment_imm_size")
                                    )
                                )
                            ),
                            "relation {{name}} (id={{id}} type={{type}})",
                        ),
                    ],
                ),
            ],
        )
    ]
