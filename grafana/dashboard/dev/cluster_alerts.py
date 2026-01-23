from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row_collapsed(
            "Cluster Alerts",
            [
                panels.subheader(
                    "Streaming Alerts",
"""[Alert Reference]
- Recovery Triggered: cluster recovery is triggered. Check `Errors by Type` / `Node Count` panels to find the root cause. Check the error logs as well.
- Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck.
  Check the following panels to follow-up:
  - `Streaming Backfill`: Check if there's any throughput in the panels, if yes, backfill is in progress. If throughput is high, it could lead to additional pressure on the streaming graph.
  - `Storage Alerts`: Look at the alerts in the storage section, specifically `Write Stall`. That will cause backpressure and the streaming graph being stuck.
  - `Cluster Resource Alerts`: If Relative CPU or Memory usage is high, it can lead to the streaming graph being stuck.
  - `Barrier Latency`: Get the latency of the barrier, it should be high.
  - `Streaming Relations`: Look at the TopN relations by CPU usage and busy rate. These relations are likely to be the bottleneck.
  - `Streaming Operators by Operator`: Look at the alerts in the streaming operators by operator section, the following panels are more likely to be the bottleneck:
    - Merger Barrier Align: If the merger barrier align is high, it means the merger is not able to align the barriers in time.
    - Join Amplification: If the join amplification is high, it means the join is not able to process the data in time.
""",
                    height=9,
                ),
                panels.timeseries_count(
                    "Streaming Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_when(
                                f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by (recovery_type) + "
                                + f"sum(rate({metric('recovery_failure_cnt')}[$__rate_interval])) by (recovery_type)"
                            ),
                            "Recovery Triggered {{recovery_type}}",
                        ),
                        panels.target(
                            alert_threshold(metric("all_barrier_nums"), 200),
                            "Too Many Barriers {{database_id}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Cluster Resource Alerts",
                    """[Alert Reference]
- Unexpected Termination: components are exiting unexpectedly (OOMKilled, Error, etc).
  - Follow-up: Check the termination reasons in `Cluster Errors` > `Termination reasons (OOMKilled, etc...)`.
- CPU Saturation: the average CPU utilization per core is too high, and the system may be throttled.
  - Mitigate: Scale the corresponding components to avoid performance issues.
  - Follow-up: If it's a streaming issue, check `Streaming Relation Metrics` > `CPU Usage Per Streaming Job` and look for top streaming relations by CPU time.
""",
                    height=6,
                ),
                panels.timeseries_count(
                    "Cluster Resource Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_threshold(
                                f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / "
                                + f"avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                                0.9,
                                ">",
                            ),
                            "CPU Saturation (avg/core) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            alert_threshold(
                                'sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval])) by (namespace, pod) / '
                                + 'sum(kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod)',
                                0.9,
                                ">",
                            ),
                            "CPU Saturation (k8s limit) - {{namespace}}/{{pod}}",
                        ),
                        panels.target(
                            alert_when(
                                "changes(("
                                + 'kube_pod_container_status_last_terminated_timestamp{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod"} '
                                + "* on (namespace,pod,container) group_left (reason) "
                                + 'kube_pod_container_status_last_terminated_reason{cluster=~"$cluster",namespace=~"$namespace",pod=~"$pod",reason!~"Completed"}'
                                + ")[$__rate_interval:])"
                            ),
                            "[{{reason}}] {{container}} {{pod}}",
                        ),
                    ],
                    ["last"],
                ),
                panels.subheader(
                    "Storage Alerts",
                    """[Alert Reference]
- Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check `Hummock Manager` section in dev dashboard.
- Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check `Compaction` section in dev dashboard, and take care of the type of `Commit Flush Bytes` and `Compaction Throughput`, whether the throughput is too low.
- Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check `Compaction` section in dev dashboard.
- Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
- Write Stall: Compaction cannot keep up. Stall foreground write, Check `Compaction` section in dev dashboard.
- Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check `Hummock Manager` section in dev dashboard.
- Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check `Hummock Manager` and `Compaction` section in dev dashboard and take care of the type of `Compaction Success Count`, whether the number of trivial-move tasks spiking.
- Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check `Hummock Write` section in dev dashboard and take care of the `Event handle latency`, whether the time consumed exceeds the barrier latency.
- Abnormal Object Storage Failure: object storage failures are occurring. Check `Object Storage` section in dev dashboard and take care of the `Object Storage Failure Rate`, whether the rate is too high.
""",
                    height=12,
                ),
                panels.timeseries_count(
                    "Storage Alert Signals",
                    "",
                    [
                        panels.target(
                            alert_threshold(
                                f"{metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}",
                                100,
                            ),
                            "Lagging Version (checkpoint)",
                        ),
                        panels.target(
                            alert_threshold(
                                f"{metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}",
                                100,
                            ),
                            "Lagging Version (pinned)",
                        ),
                        panels.target(
                            alert_threshold(
                                f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                                + f"{metric('storage_level_total_file_size')}) by (L0)",
                                52428800,
                            ),
                            "Lagging Compaction",
                        ),
                        panels.target(
                            alert_threshold(metric("storage_stale_object_count"), 200),
                            "Lagging Vacuum",
                        ),
                        panels.target(
                            alert_threshold(metric("state_store_meta_cache_usage_ratio"), 1.1),
                            "Abnormal Meta Cache Memory",
                        ),
                        panels.target(
                            alert_threshold(metric("state_store_block_cache_usage_ratio"), 1.1),
                            "Abnormal Block Cache Memory",
                        ),
                        panels.target(
                            alert_threshold(
                                metric("state_store_uploading_memory_usage_ratio"), 0.7
                            ),
                            "Abnormal Uploading Memory Usage",
                        ),
                        panels.target(
                            alert_threshold(
                                metric("storage_write_stop_compaction_groups"), 0, ">"
                            ),
                            "Write Stall (group {{compaction_group_id}})",
                        ),
                        panels.target(
                            alert_threshold(metric("storage_version_size"), 314572800),
                            "Abnormal Version Size",
                        ),
                        panels.target(
                            alert_threshold(metric("storage_delta_log_count"), 5000),
                            "Abnormal Delta Log Number",
                        ),
                        panels.target(
                            alert_threshold(metric("state_store_event_handler_pending_event"), 10000000),
                            "Abnormal Pending Event Number",
                        ),
                        panels.target(
                            alert_when(
                                f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by (type)"
                            ),
                            "Abnormal Object Storage Failure ({{type}})",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]
