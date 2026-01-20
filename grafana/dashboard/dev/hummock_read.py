from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    meta_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    file_cache_get_filter = "op='get'"

    return [
        outer_panels.row_collapsed(
            "Hummock (Read)",
            [
                panels.timeseries_ops(
                    "Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type)",
                            "{{table_id}} @ {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, type)",
                            "total_meta_miss_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Cache Size",
                    "Hummock has three parts of memory usage: 1. Meta Cache 2. Block Cache"
                    "This metric shows the real memory usage of each of these three caches.",
                    [
                        panels.target(
                            f"avg({metric('state_store_meta_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_block_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "data cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_prefetch_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "prefetch cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) >= 0",
                            "meta cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) >= 0",
                            "block cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Block Cache Efficiency",
                    "Histogram of the estimated hit ratio of a block while in the block cache.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"clamp_max(histogram_quantile({quantile}, sum(rate({metric('block_efficiency_histogram_bucket')}[$__rate_interval])) by (le,{COMPONENT_LABEL},{NODE_LABEL})), 1)",
                                f"block cache efficienfy - p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [10, 25, 50, 75, 90, 100],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iter keys flow",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type, table_id)",
                            "iter keys flow - {{table_id}} @ {{type}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Merged SSTs",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_merge_sstable_counts_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, table_id, type))",
                                f"# merged ssts p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{type}}"
                                % COMPONENT_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_count')}[$__rate_interval])) > 0",
                            "# merged ssts avg  - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Read Duration - Get",
                    "Histogram of the latency of Get operations that have been issued to the state store.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_get_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Read Duration - Iter",
                    "Histogram of the time spent on iterator initialization."
                    "Histogram of the time spent on iterator scanning.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_init_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"create_iter_time p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_init_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, iter_type) (rate({metric('state_store_iter_init_duration_count')}[$__rate_interval])) > 0",
                            "create_iter_time avg - {{iter_type}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_scan_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"pure_scan_time p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_scan_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, iter_type) (rate({metric('state_store_iter_scan_duration_count')}[$__rate_interval])) > 0",
                            "pure_scan_time avg - {{iter_type}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Bloom Filter Ops",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter false positive count  - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter positive count - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter check count- {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter Positive Rate",
                    "Positive / Total",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)) >= 0",
                            "bloom filter positive rate - {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter False-Positive Rate",
                    "False-Positive / Total",
                    [
                        panels.target(
                            f"(((sum(rate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type))) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type))) >= 0",
                            "read req bloom filter false positive rate - {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Slow Fetch Meta Unhits",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_iter_slow_fetch_meta_cache_unhits')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Read Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "get - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_shared_buffer_hit_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "shared_buffer hit - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id, iter_type)",
                            "{{iter_type}} - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Get",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_key_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_value_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend} - {{{{table_id}}}} {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Materialized View Read Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_iter_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id) + sum((histogram_quantile({quantile}, sum(rate({metric("state_store_get_key_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({metric("state_store_get_value_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id)',
                                f"read p{legend} - materialized view {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Item Count - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_item_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"{metric('state_store_iter_in_progress_counts')}",
                            "Existing {{iter_type}} count @ {{table_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_iter_log_op_type_counts')}[$__rate_interval])) by (table_id, op_type)",
                            "iter_log op count @ {{table_id}} {{op_type}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Read Throughput - Get",
                    "The size of a single key-value pair when reading by operation Get."
                    "Operation Get gets a single key-value pair with respect to a caller-specified key. If the key does not "
                    "exist in the storage, the size of key is counted into this metric and the size of value is 0.",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_get_key_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL}) + sum(rate({metric('state_store_get_value_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Read Throughput - Iter",
                    "The size of all the key-value paris when reading by operation Iter."
                    "Operation Iter scans a range of key-value pairs.",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_iter_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Fetch Meta Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_fetch_meta_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"fetch_meta_duration p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_count')}[$__rate_interval])) > 0",
                            "fetch_meta_duration avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Fetch Meta Unhits",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_iter_fetch_meta_cache_unhits')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Safe Version Fetch Count",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_safe_version_hit')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('state_store_safe_version_miss')}",
                            "",
                        ),
                    ],
                ),
            ],
        )
    ]
