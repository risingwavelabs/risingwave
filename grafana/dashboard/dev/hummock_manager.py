from ..common import *
from . import section


@section
def _(outer_panels: Panels):
    panels = outer_panels.sub_panel()
    total_key_size_filter = "metric='total_key_size'"
    total_value_size_filter = "metric='total_value_size'"
    total_key_count_filter = "metric='total_key_count'"
    mv_total_size_filter = "metric='materialized_view_total_size'"
    return [
        outer_panels.row_collapsed(
            "Hummock Manager",
            [
                panels.timeseries_latency(
                    "Lock Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('hummock_manager_lock_time_bucket')}[$__rate_interval])) by (le, method, lock_name, lock_type))",
                                f"Lock Time p{legend}"
                                + " - {{method}} @ {{lock_type}} @ {{lock_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Real Process Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('meta_hummock_manager_real_process_time_bucket')}[$__rate_interval])) by (le, method))",
                                f"Real Process Time p{legend}" + " - {{method}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Version Size",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_version_size')}", "version size"
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Version Id",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_current_version_id')}",
                            "current version id",
                        ),
                        panels.target(
                            f"{metric('storage_checkpoint_version_id')}",
                            "checkpoint version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_pinned_version_id')}",
                            "min pinned version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_safepoint_version_id')}",
                            "min safepoint version id",
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_max_committed_epoch')}",
                            "max committed epoch",
                        ),
                        panels.target(
                            f"{metric('storage_min_committed_epoch')}",
                            "min committed epoch",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Table Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                        panels.target(
                            f"{table_metric('storage_version_stats', total_value_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Materialized View Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_materialized_view_stats', mv_total_size_filter)}/1024",
                            "{{metric}}, mv id - {{table_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Table KV Count",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_count_filter)}",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Object Total Number",
                    """
Objects are classified into 3 groups:
- not referenced by versions: these object are being deleted from object store.
- referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
- referenced by current version: these objects are in the latest version.

Additionally, a metric on all objects (including dangling ones) is updated with low-frequency. The metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.
                    """,
                    [
                        panels.target(
                            f"{metric('storage_stale_object_count')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_count')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_count')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_time_travel_object_count')}",
                            "referenced by time travel",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_count')}",
                            "all objects (including dangling ones)",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Object Total Size",
                    "Refer to `Object Total Number` panel for classification of objects.",
                    [
                        panels.target(
                            f"{metric('storage_stale_object_size')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_size')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_size')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_size')}",
                            "all objects, including dangling ones",
                        ),
                    ],
                ),
                panels.timeseries_count(
                        "Table Change Log Object Count",
                        "Per table change log object count",
                        [
                            panels.target(
                                f"{metric('storage_table_change_log_object_count')}",
                                "{{table_id}}",
                            ),
                        ],
                    ),
                panels.timeseries_bytes(
                        "Table Change Log Object Size",
                        "Per table change log object size",
                        [
                            panels.target(
                                f"{metric('storage_table_change_log_object_size')}",
                                "{{table_id}}",
                            ),
                        ],
                    ),
                panels.timeseries_count(
                    "Delta Log Total Number",
                    "total number of hummock version delta log",
                    [
                        panels.target(
                            f"{metric('storage_delta_log_count')}",
                            "delta log total number",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Version Checkpoint Latency",
                    "hummock version checkpoint latency",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('storage_version_checkpoint_latency_bucket')}[$__rate_interval])) by (le))",
                            f"version_checkpoint_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('storage_version_checkpoint_latency_sum')}[$__rate_interval]) / rate({metric('storage_version_checkpoint_latency_count')}[$__rate_interval]) > 0",
                            "version_checkpoint_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Write Stop Compaction Groups",
                    "When certain per compaction group threshold is exceeded (e.g. number of level 0 sub-level in LSMtree), write op to that compaction group is stopped temporarily. Check log for detail reason of write stop.",
                    [
                        panels.target(
                            f"{metric('storage_write_stop_compaction_groups')}",
                            "compaction_group_{{compaction_group_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Full GC Trigger Count",
                    "total number of attempts to trigger full GC",
                    [
                        panels.target(
                            f"{metric('storage_full_gc_trigger_count')}",
                            "full_gc_trigger_count",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Compaction Event Loop Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "State Table Count",
                    "The number of state_tables in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_state_table_count')}[$__rate_interval])) by (group)",
                            "state table cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Branched SST Count",
                    "The number of branched_sst in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_branched_sst_count')}[$__rate_interval])) by (group)",
                            "branched sst cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Time Travel Replay Latency",
                    "The latency of replaying a hummock version for time travel",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('storage_time_travel_version_replay_latency_bucket')}[$__rate_interval])) by (le))",
                            f"time_travel_version_replay_latency_p{legend}",
                        ),
                        [50, 90, 99, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('storage_time_travel_version_replay_latency_sum')}[$__rate_interval]) / rate({metric('storage_time_travel_version_replay_latency_count')}[$__rate_interval]) > 0",
                            "time_travel_version_replay_avg",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Time Travel Replay Ops",
                    "The frequency of replaying a hummock version for time travel",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_time_travel_version_replay_latency_count')}[$__rate_interval]))",
                            "time_travel_version_replay_ops",
                        ),
                    ],
                ),
            ],
        )
    ]
