from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    meta_miss_filter = "type='file_meta_miss'"
    meta_total_filter = "type='file_meta_total'"
    data_miss_filter = "type='file_block_miss'"
    data_total_filter = "type='file_block_total'"
    hnsw_graph_miss_filter = "type='hnsw_graph_miss'"
    hnsw_graph_total_filter = "type='hnsw_graph_total'"

    return [
        outer_panels.row_collapsed(
            "Vector Search",
            [
                panels.timeseries_ops(
                    "Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_vector_object_request_counts')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type, mode)",
                            "{{table_id}} @ {{mode}} {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Cache Size",
                    "Vector search has 2 parts of memory usage: 1. Meta Cache 2. Block Cache"
                    "This metric shows the real memory usage of each of these 2 caches.",
                    [
                        panels.target(
                            f"avg({metric('state_store_vector_data_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "data cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_vector_meta_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Usage Ratio",
                    "Vector search has 2 parts of memory usage: 1. Meta Cache 2. Block Cache"
                    "",
                    [
                        panels.target(
                            f"avg({metric('state_store_vector_data_cache_usage_ratio')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "data cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                        panels.target(
                            f"avg({metric('state_store_vector_meta_cache_usage_ratio')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_vector_object_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) / (sum(rate({table_metric('state_store_vector_object_request_counts', meta_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) >= 0",
                            "meta cache miss ratio - {{table_id}} {{mode}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_vector_object_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) / (sum(rate({table_metric('state_store_vector_object_request_counts', data_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) >= 0",
                            "block cache miss ratio - {{table_id}} {{mode}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_vector_object_request_counts', hnsw_graph_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) / (sum(rate({table_metric('state_store_vector_object_request_counts', hnsw_graph_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,mode)) >= 0",
                            "hnsw graph miss ratio - {{table_id}} {{mode}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                    ],
                ),
                # todo: hitmap to measure block efficiency
                # panels.timeseries_percentage(
                #     "Block Cache Efficiency",
                #     "Histogram of the estimated hit ratio of a block while in the block cache.",
                #     [
                #         *quantile(
                #             lambda quantile, legend: panels.target(
                #                 f"clamp_max(histogram_quantile({quantile}, sum(rate({metric('block_efficiency_histogram_bucket')}[$__rate_interval])) by (le,{COMPONENT_LABEL},{NODE_LABEL})), 1)",
                #                 f"block cache efficienfy - p{legend}"
                #                 + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                #             ),
                #             [10, 25, 50, 75, 90, 100],
                #         ),
                #     ],
                # ),
                panels.timeseries_latency(
                    "Read Duration",
                    "Histogram of the latency of read operations that have been issued to the vector index.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_vector_nearest_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,top_n,ef_search))",
                                f"p{legend}"
                                + " - {{table_id}} [top_n={{top_n}}, ef_search={{ef_search}}] @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,top_n,ef_search)(rate({table_metric('state_store_vector_nearest_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,top_n,ef_search) (rate({table_metric('state_store_vector_nearest_duration_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} [top_n={{top_n}}, ef_search={{ef_search}}] {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Read Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_vector_nearest_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id,top_n,ef_search)",
                            "nearest - {{table_id}} [top_n={{top_n}}, ef_search={{ef_search}}] @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Stat",
                    "Histogram of stats of reading the vector index.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_vector_request_stats_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,type,mode,top_n,ef))",
                                f"p{legend}"
                                + " - {{table_id}} {{type}} {{mode}} [top_n={{top_n}}, ef={{ef}}] @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                                ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,type,mode,top_n,ef)(rate({table_metric('state_store_vector_request_stats_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id,type,mode,top_n,ef) (rate({table_metric('state_store_vector_request_stats_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} {{type}} {{mode}} [top_n={{top_n}}, ef={{ef}}] {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                    ],
                ),
                panels.timeseries_count(
                    "Vector Index Count Stat",
                    "count stats of vector index",
                    [
                        panels.target(
                            f"sum({metric('state_store_vector_hnsw_graph_level_node_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, level)",
                            "{{table_id}} vector count level {{level}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_vector_hnsw_graph_level_node_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id)",
                            "{{table_id}} vector count total - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                        panels.target(
                            f"sum({metric('state_store_vector_index_file_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id)",
                            "{{table_id}} vector file total - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Vector Index File Stat",
                    "file stats of vector index",
                    [
                        panels.target(
                            f"sum({metric('state_store_vector_index_file_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id,type)",
                            "{{table_id}} {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        )
    ]
