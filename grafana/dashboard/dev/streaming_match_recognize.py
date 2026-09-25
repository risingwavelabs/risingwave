from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    # The actor_id can be masked due to metrics level settings.
    # We use this filter to suppress the actor-level panels if applicable.
    actor_level_filter = "actor_id!=''"
    panels = outer_panels.sub_panel()

    def per_second(name):
        return [
            panels.target(
                f"sum(rate({metric(name)}[$__rate_interval])) by (table_id, fragment_id)",
                "table {{table_id}} fragment {{fragment_id}}",
            ),
            panels.target_hidden(
                f"rate({metric(name, actor_level_filter)}[$__rate_interval])",
                "actor {{actor_id}} table {{table_id}}",
            ),
        ]

    return [
        outer_panels.row_collapsed(
            "Streaming MATCH_RECOGNIZE",
            [
                panels.timeseries_row(
                    "Match Recognize Retained Rows",
                    "Rows held in the matcher's memory, summed over the actor's partitions: "
                    "exactly the rows some live partial match or held match still references. "
                    "Bounded only by match liveness and WITHIN; sustained growth is a pattern "
                    "whose closer never arrives.",
                    [
                        panels.target(
                            f"sum({metric('stream_match_recognize_retained_rows')}) by (table_id, fragment_id)",
                            "table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_match_recognize_retained_rows', actor_level_filter)}",
                            "actor {{actor_id}} table {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Match Recognize Matches Emitted",
                    "Matches emitted per second.",
                    per_second("stream_match_recognize_matches_emitted_count"),
                ),
                panels.timeseries_rowsps(
                    "Match Recognize Evicted Rows",
                    "Rows leaving the matcher per second, whether consumed by an emitted match "
                    "or pruned as a dead prefix.",
                    per_second("stream_match_recognize_evicted_rows_count"),
                ),
                panels.timeseries_ops(
                    "Match Recognize Scan Budget Exhausted",
                    "Budget exhaustions per second, on an arrival's advance or on a watermark "
                    "visit: a backtracking pattern over the buffered rows. The warning. "
                    "Exhaustion is self-healing while each visit still makes progress; see Stuck "
                    "Visits for the watermark visits that do not.",
                    per_second("stream_match_recognize_scan_budget_exhausted_count"),
                ),
                panels.timeseries_ops(
                    "Match Recognize Stuck Visits",
                    "Watermark visits per second that spent the scan budget and moved nothing: "
                    "no scan or freeze cursor advanced, nothing emitted, nothing evicted, no gate "
                    "verdict cached. The alert: such a visit repeats the same work next time until "
                    "new rows change the partition. Sustained, the partition will not decide "
                    "unless rows or the query change (simplify the pattern; add or tighten WITHIN "
                    "if the scan does find matches — window closure drains only those, and a scan "
                    "starved before finding any sheds nothing).",
                    per_second("stream_match_recognize_stuck_visit_count"),
                ),
                panels.timeseries_ops(
                    "Match Recognize WITHIN Deadline Overflows",
                    "Buffered rows per second whose WITHIN deadline exceeds the ORDER BY type's "
                    "range; their window never closes through watermark progress.",
                    per_second("stream_match_recognize_within_deadline_overflow_count"),
                ),
            ],
        )
    ]
