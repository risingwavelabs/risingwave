from ..common import *
from . import section

# NOTE(kwannoel):
# Relative busy time is computed as:
# 1) negate the idle time;
# 2) compute the max idle time as a baseline offset; and
# 3) add (2) to (1) to obtain a positive relative busy time.
#
# Limitations:
# - This does not consider database isolation; different databases may have different max idle times.
# - Idle time is cumulative (total to date), not a delta over a window. It is mainly useful for the
#   total busy time cost. To observe busy time over a specific period, use the busy time rate.
def _actor_busy_time_relative_target(panels: Panels):
    # NOTE(kwannoel): The output blocking duration is the duration that the output buffer is blocked.
    # We record this metric per edge, and we dispatch concurrently for each edge.
    output_blocking_per_edge_expr = (
        f"sum({metric('stream_actor_output_buffer_blocking_duration_ns')})"
        f"  by (fragment_id, downstream_fragment_id)"
        f"/ ignoring (downstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
    )

    # NOTE(kwannoel): Due to concurrent dispatching, we take the max of the blocking duration per edge,
    # per fragment, to obtain the blocking duration of the fragment.
    # We make sure to fill in zero if there are no dispatchers for a given fragment.
    # This ensures it can still match when joining with the input blocking duration.
    # We don't do the same for the input blocking duration; otherwise, when actors block on input for a long time,
    # we would still record their input blocking duration as zero.
    # Furthermore, the case where an actor has no input is typically in root nodes (e.g., source or table).
    # These are rarely the bottleneck; if they are, consult the input blocking duration panel.
    output_block_expr = (
        f"("
        f"  max("
        f"    {output_blocking_per_edge_expr}"
        f"  ) by (fragment_id)"
        f"  or (sum({metric('stream_actor_count')}) by (fragment_id) * 0)"
        f")"
    )

    # NOTE(kwannoel): The input blocking duration is the duration that the input buffer is blocked.
    # We record this metric per edge, and we merge inputs concurrently for each edge, since we can have multiple
    # merge executors for a given fragment (e.g. union).
    input_blocking_per_edge_expr = (
        f"sum({metric('stream_actor_input_buffer_blocking_duration_ns')})"
        f"  by (fragment_id, upstream_fragment_id)"
        f"/ ignoring (upstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
    )

    # NOTE(kwannoel): We take the max of the blocking duration per edge, per fragment,
    # to get the input blocking duration of the fragment.
    input_block_expr = (
        f"("
        f"  max("
        f"    {input_blocking_per_edge_expr}"
        f"  ) by (fragment_id)"
        f")"
    )
    # NOTE(kwannoel): Busy time is the sum of the output and input blocking durations.
    # It's critical for output_block_expr to be on the left-hand side (LHS), since it always exists.
    # If input_block_expr doesn't exist, it either means the input is blocked, or the actor has no input.
    # In the first case, we don't want to render the blocking duration; treating it as zero would skew the result.
    # In the second case, rendering is not important. As mentioned above, such actors are usually root nodes
    # (e.g., source or table) and are rarely the bottleneck.
    busy_expr = f"(({output_block_expr}) + ({input_block_expr})) or ({input_block_expr})"

    # NOTE(kwannoel): We ignore `fragment_id` to get a global max of busy time.
    baseline_busy_expr = f"ignoring (fragment_id) group_left() max({busy_expr})"

    return panels.target(
        f"-({busy_expr}) + {baseline_busy_expr}",
        "fragment {{fragment_id}}",
    )

# NOTE(kwannoel): Busy rate is analogous to busy time, but over an interval.
# - We divide by the rate interval to obtain a rate, and by 1e9 to convert ns to seconds
#   (rate is per second).
# - Busy rate equals: 1 - idle rate.
# - Idle time is approximated by max(output_blocking_duration, input_blocking_duration) per fragment,
#   because output and input blocking events can overlap across actors and edges. We therefore use the max as an estimate.
def _actor_busy_rate_expr(rate_interval: str):
    output_blocking_rate_per_edge_expr = (
        f"sum(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[{rate_interval}]))"
        f"  by (fragment_id, downstream_fragment_id)"
        f"/ ignoring (downstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
        f"/ 1000000000"
    )
    output_blocking_rate_expr = (
        f"max("
        f"  {output_blocking_rate_per_edge_expr}"
        f") by (fragment_id)"
        f"  or (sum({metric('stream_actor_count')}) by (fragment_id) * 0)"
    )
    input_blocking_rate_per_edge_expr = (
        f"sum(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[{rate_interval}]))"
        f"  by (fragment_id, upstream_fragment_id)"
        f"/ ignoring (upstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
        f"/ 1000000000"
    )
    input_blocking_rate_expr = (
        f"max("
        f"    {input_blocking_rate_per_edge_expr}"
        f") by (fragment_id)"
    )
    busy_rate_expr = (
        f"clamp_min("
        f"  1 - ("
        f"    ({output_blocking_rate_expr}) + ({input_blocking_rate_expr}) or"
        f"    ({input_blocking_rate_expr})"
        f"  ), 0"
        f")"
    )
    return busy_rate_expr

def _actor_busy_rate_target(panels: Panels, rate_interval: str):
    actor_busy_rate_expr = _actor_busy_rate_expr(rate_interval)
    return panels.target(
        f"({actor_busy_rate_expr})",
        "fragment {{fragment_id}}",
    )



@section
def _(outer_panels: Panels):
    # The actor_id can be masked due to metrics level settings.
    # We use this filter to suppress the actor-level panels if applicable.
    actor_level_filter = "actor_id!=''"
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "[Streaming] Streaming Actors",
            [
                panels.subheader("General"),
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
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage",
                    "The operator-level memory usage statistics collected by each LRU cache",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')}) by (table_id, desc)",
                            "table total {{table_id}}: {{desc}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_memory_usage', actor_level_filter)}",
                            "actor {{actor_id}} table {{table_id}}: {{desc}}",
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
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id)) >= 0",
                            "Join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_state_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_state_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg state cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Distinct agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream lookup cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream temporal join cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Materialize executor cache miss ratio - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_left_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache left miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_right_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache right miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Barrier Align Per Second (excludes merge node)",
                    "",
                    [
                        # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                        # Thus to calculate the average, we need to manually divide the actor count.
                        panels.target(
                            f"sum(rate({metric('stream_barrier_align_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id, wait_side, executor) \
                            / ignoring (wait_side, executor) group_left sum({metric('stream_actor_count')}) by (fragment_id)",
                            "fragment avg {{fragment_id}} {{wait_side}} {{executor}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_barrier_align_duration_ns', actor_level_filter)}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} fragment {{fragment_id}} {{wait_side}} {{executor}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_rowsps(
                    "Executor Throughput",
                    "When enabled, this metric shows the input throughput of each executor.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_executor_row_count')}[$__rate_interval])) by (executor_identity, fragment_id)",
                            "{{executor_identity}} fragment total {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_executor_row_count', actor_level_filter)}[$__rate_interval])",
                            "{{executor_identity}} actor {{actor_id}}",
                        ),
                    ],
                ),
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
                panels.subheader("Exchange"),
                panels.timeseries_percentage(
                    "Merger Barrier Align",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_merge_barrier_align_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id) \
                            / sum({metric('stream_actor_count')}) by (fragment_id)",
                            "avg - fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_merge_barrier_align_duration_ns', actor_level_filter)}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} - fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Send Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Recv Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Temporal Join"),
                panels.timeseries_actor_ops(
                    "Temporal Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (fragment_id)",
                            "temporal join cache miss, table_id {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])",
                            "temporal join cache miss, table_id {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Temporal Join Cache Keys",
                    "The number of keys cached in temporal join executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_temporal_join_cached_entry_count')}) by (table_id, fragment_id)",
                            "Temporal Join cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_temporal_join_cached_entry_count')}",
                            "Temporal Join cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Materialize"),
                panels.timeseries_actor_ops(
                    "Materialize Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_data_exist_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "data exist count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache hit count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval]) - rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "total cached count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_hit_count', actor_level_filter)}[$__rate_interval])",
                            "cache hit count - actor {{actor_id}} table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_total_count', actor_level_filter)}[$__rate_interval])",
                            "total cached count - actor {{actor_id}} table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Join"),
                panels.timeseries_percentage(
                    "Join Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Join Actor Match Duration Per Chunk/Barrier",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id,side)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Keys",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "number of join keys in the executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_join_cached_entry_count')}) by (fragment_id, side)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_join_cached_entry_count')}",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Executor Matched Rows",
                    "The number of matched rows on the opposite side",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target_hidden(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_join_matched_join_keys_bucket')}[$__rate_interval])) by (le, fragment_id, table_id, {COMPONENT_LABEL}))",
                                f"p{legend} - fragment {{{{fragment_id}}}} table_id {{{{table_id}}}} - {{{{{COMPONENT_LABEL}}}}}",
                            ),
                            [90, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, fragment_id, table_id) (rate({metric('stream_join_matched_join_keys_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, fragment_id, table_id) (rate({table_metric('stream_join_matched_join_keys_count')}[$__rate_interval])) >= 0",
                            "avg - fragment {{fragment_id}} table_id {{table_id}} - {{%s}}"
                            % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.subheader("Aggregation"),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each StreamChunk",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level cache miss - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level total lookups - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])",
                            "chunk-level cache miss  - table {{table_id}} actor {{actor_id}}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])",
                            "chunk-level total lookups  - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Cached Keys",
                    "The number of keys cached in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_agg_distinct_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg distinct cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_cached_entry_count')}",
                            "stream agg cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_distinct_cached_entry_count')}",
                            "stream agg distinct cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Dirty Groups Count",
                    "The number of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_count')}) by (table_id, fragment_id)",
                            "stream agg dirty groups count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_count')}",
                            "stream agg dirty groups count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Aggregation Dirty Groups Heap Size",
                    "The total heap size of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_heap_size')}) by (table_id, fragment_id)",
                            "stream agg dirty groups heap size | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_heap_size')}",
                            "stream agg dirty groups heap size | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("TopN"),
                panels.timeseries_count(
                    "TopN Cached Keys",
                    "The number of keys cached in each top_n executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_group_top_n_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_group_top_n_appendonly_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n appendonly cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_cached_entry_count')}",
                            "group top_n cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_appendonly_cached_entry_count')}",
                            "group top_n appendonly cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.subheader("Over Window"),
                panels.timeseries_actor_ops(
                    "Over Window Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache lookup count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])",
                            "cache lookup count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])",
                            "cache miss count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache lookup count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_left_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache left miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_right_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache right miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Over Window Executor State Computation",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_accessed_entry_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "accessed entry count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_compute_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "compute count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_same_output_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "same output count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Over Window Cached Keys",
                    "The number of keys cached in over window executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_over_window_cached_entry_count')}) by (table_id, fragment_id)",
                            "over window cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_over_window_cached_entry_count')}",
                            "over window cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_over_window_range_cache_entry_count')}) by (table_id, fragment_id)",
                            "over window partition range cache entry count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.subheader("Lookup"),
                panels.timeseries_count(
                    "Lookup Cached Keys",
                    "The number of keys cached in lookup executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_lookup_cached_entry_count')}) by (table_id, fragment_id)",
                            "lookup cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_lookup_cached_entry_count')}",
                            "lookup cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
