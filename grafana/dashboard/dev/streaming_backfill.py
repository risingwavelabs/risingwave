from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "[Streaming] Streaming Backfill",
            [
                # FIXME(kwannoel): We should add the job id as the label, rather than solely the upstream table id.
                panels.timeseries_rowsps(
                    "Backfill Snapshot-Read Throughput (rows/s)",
                    "Rows/sec that we read from the backfill snapshot by table",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_snapshot_read_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_snapshot_backfill_consume_snapshot_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} {{stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Upstream Throughput (rows/s)",
                    "Total number of rows that have been output from the backfill upstream by table",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Snapshot Backfill Lag",
                    "",
                    [
                        panels.target(
                            f"{metric('meta_snapshot_backfill_upstream_lag')} / (2^16) / 1000",
                            "lag @ {{table_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]
