from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Refresh Manager",
            [
                panels.timeseries_latency(
                    "Refresh Job Duration",
                    "Time taken to complete a refresh job",
                    [
                        panels.target(
                            f"{metric('meta_refresh_job_duration')}",
                            "{{table_id}} - {{status}}"
                        )
                    ]
                ),
                panels.timeseries_count(
                    "Refresh Job Finish Rate",
                    "Number of finished refresh jobs",
                    [
                        panels.target(
                            f"sum by(status) (rate({metric('meta_refresh_job_finish_cnt')}[$__rate_interval]))",
                            "{{status}}"
                        )
                    ]
                ),
                panels.timeseries_count(
                    "Refresh Cron Job Triggers",
                    "Number of cron refresh jobs triggered",
                    [
                        panels.target(
                            f"rate({metric('meta_refresh_cron_job_trigger_cnt')}[$__rate_interval])",
                            "{{table_id}}"
                        )
                    ]
                ),
                panels.timeseries_count(
                    "Refresh Cron Job Misses",
                    "Number of skipped cron refresh jobs",
                    [
                        panels.target(
                            f"rate({metric('meta_refresh_cron_job_miss_cnt')}[$__rate_interval])",
                            f"{{table_id}}"
                        )
                    ]
                ),
            ]
        )
    ]
