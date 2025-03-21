from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Actor/Table Id Info",
            [
                panels.table_info(
                    "Actor Info",
                    "Information about actors",
                    [
                        panels.table_target(
                            f"group({metric('actor_info')}) by (actor_id, fragment_id, compute_node)"
                        )
                    ],
                    ["actor_id", "fragment_id", "compute_node"],
                ),
                panels.table_info(
                    "State Table Info",
                    "Information about state tables. Column `materialized_view_id` is the id of the materialized view that this state table belongs to.",
                    [
                        panels.table_target(
                            f"group({metric('table_info')}) by (table_id, table_name, table_type, materialized_view_id, fragment_id, compaction_group_id)"
                        )
                    ],
                    [
                        "table_id",
                        "table_name",
                        "table_type",
                        "materialized_view_id",
                        "fragment_id",
                        "compaction_group_id",
                    ],
                ),
                panels.table_info(
                    "Actor Count (Group By Compute Node)",
                    "Actor count per compute node",
                    [
                        panels.table_target(
                            f"count({metric('actor_info')}) by (compute_node)"
                        )
                    ],
                    [
                        "table_id",
                        "table_name",
                        "table_type",
                        "materialized_view_id",
                        "fragment_id",
                        "compaction_group_id",
                    ],
                    dict.fromkeys(["Time"], True),
                ),
            ],
        )
    ]
