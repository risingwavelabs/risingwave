from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Batch",
            [
                panels.timeseries_count(
                    "Running query in distributed execution mode",
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
                    "Rejected query in distributed execution mode",
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
                    "Completed query in distributed execution mode",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Query Latency in Distributed Execution Mode",
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
                    "Query Latency in Local Execution Mode",
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
