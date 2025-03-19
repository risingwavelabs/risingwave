from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Adhoc queries (Batch & Frontend)",
            [
                panels.subheader("Batch"),
                panels.timeseries_row(
                    "Exchange Recv Row Number",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_exchange_recv_row_number')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Mpp Task Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_task_num')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "Batch Mem Usage",
                    "All memory usage of batch executors in bytes",
                    [
                        panels.target(
                            f"{metric('compute_batch_total_mem')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('frontend_batch_total_mem')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Heartbeat Worker Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_heartbeat_worker_num')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Row SeqScan Next Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('batch_row_seq_scan_next_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"row_seq_scan next p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_count')}[$__rate_interval])) > 0",
                            "row_seq_scan next avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Batch Spill Throughput",
                    "Disk throughputs of spilling-out in the bacth query engine",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_spill_read_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "read - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('batch_spill_write_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "write - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.subheader("Frontend"),
                panels.timeseries_count(
                    "Active Sessions",
                    "Number of active sessions",
                    [
                        panels.target(
                            f"{metric('frontend_active_sessions')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "The Number of Running Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_running_query_num')}",
                            "The number of running query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Rejected queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_rejected_query_counter')}",
                            "The number of rejected query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Completed Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Subsription Cursor Nums",
                    "The number of valid and invalid subscription cursor",
                    [
                        panels.target(
                            f"{metric('subsription_cursor_nums')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('invalid_subsription_cursor_nums')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Subscription Cursor Error Count",
                    "The subscription error num of cursor",
                    [
                        panels.target(
                            f"{metric('subscription_cursor_error_count')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Query Duration(ms)",
                    "The amount of time a query exists inside the cursor",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_query_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Declare Duration(ms)",
                    "Subscription cursor duration of declare",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_declare_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Fetch Duration(ms)",
                    "Subscription cursor duration of fetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_fetch_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Last Fetch Duration(ms)",
                    "Since the last fetch, the time up to now",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_last_fetch_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency (Distributed Query Mode)",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency (Local Query Mode)",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]
