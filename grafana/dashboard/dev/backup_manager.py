from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Backup Manager",
            [
                panels.timeseries_count(
                    "Job Count",
                    "Total backup job count since the Meta node starts",
                    [
                        panels.target(
                            f"{metric('backup_job_count')}",
                            "job count",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Job Process Time",
                    "Latency of backup jobs since the Meta node starts",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('backup_job_latency_bucket')}[$__rate_interval])) by (le, state))",
                                f"Job Process Time p{legend}" + " - {{state}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]
