from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    # The actor_id can be masked due to metrics level settings.
    # We use this filter to suppress the actor-level panels if applicable.
    actor_level_filter = "actor_id!=''"
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Operator Metrics by Operator Type",
            [
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
