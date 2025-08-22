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
                    "iceberg compaction commit count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_commit_counter')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Input File counts",
                    "iceberg compaction input file counts",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_input_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Input Bytes",
                    "iceberg compaction input bytes",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_input_bytes_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Output File counts",
                    "iceberg compaction output file counts",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_output_files_count')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Output Bytes",
                    "iceberg compaction output bytes",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_output_bytes_total')}) by (catalog_name, table_ident)",
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
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_commit_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Duration",
                    "iceberg compaction duration in milliseconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Execution Error Count",
                    "iceberg compaction execution error count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_executor_error_counter')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Datafusion Record Processed Count",
                    "iceberg compaction datafusion record processed count",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_datafusion_records_processed_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Datafusion Bytes Processed",
                    "iceberg compaction datafusion bytes processed",
                    [
                        panels.target(
                            f"sum({metric('iceberg_compaction_datafusion_bytes_processed_total')}) by (catalog_name, table_ident)",
                            "{{catalog_name}}-{{table_ident}}",
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Datafusion Batch Fetch Duration",
                    "iceberg compaction datafusion batch fetch duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_fetch_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_latency_ms(
                    "Iceberg Compaction Datafusion Batch Write Duration",
                    "iceberg compaction datafusion batch write duration in seconds",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_write_duration_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Iceberg Compaction Datafusion Batch Row Count Distribution",
                    "iceberg compaction datafusion batch row count distribution",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_row_count_dist_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Iceberg Compaction Datafusion Batch Bytes Distribution",
                    "iceberg compaction datafusion batch bytes distribution",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_compaction_datafusion_batch_bytes_dist_bucket')}[$__rate_interval])) by (le, catalog_name, table_ident))",
                                f"p{legend}" + " @ {{catalog_name}} {{table_ident}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),

            ]
        )
    ]
