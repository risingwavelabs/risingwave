from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Iceberg Compaction Metrics",
            [
                panels.timeseries_count(
                    "Iceberg Compaction Commit Count",
                    "",
                    [
                        panels.target(
                            f"sum({metric('compaction_commit_counter')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Rewritten Bytes",
                    "",
                    [
                        panels.target(
                            f"sum({metric('compaction_rewritten_bytes')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Rewritten File counts",
                    "",
                    [
                        panels.target(
                            f"sum({metric('compaction_rewritten_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Added File counts",
                    "",
                    [
                        panels.target(
                            f"sum({metric('compaction_added_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Failed File counts",
                    "",
                    [
                        panels.target(
                            f"sum({metric('compaction_failed_data_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Commit Duration",
                    "iceberg compaction commit duration in milliseconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compaction_commit_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_latency(
                    "Iceberg Compaction Duration",
                    "iceberg compaction duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compaction_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]
