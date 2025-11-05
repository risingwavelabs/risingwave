from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "System Parameters",
            [
                panels.table_info(
                    "System Parameters",
                    "Current system parameters from meta",
                    [
                        panels.table_target(
                            f"group({metric('system_param_info')}) by (name, value)"
                        )
                    ],
                    ["name", "value"],
                ),
            ],
        )
    ]

