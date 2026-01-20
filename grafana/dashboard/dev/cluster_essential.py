from ..common import *
from . import section

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row("[Cluster] Cluster Essential Information"),
        *[
            panels.subheader("Node Status"),
            panels.timeseries_count(
                "Node Count",
                "The number of each type of RisingWave components alive.",
                [
                    panels.target(
                        f"sum({metric('worker_num')}) by (worker_type)",
                        "{{worker_type}}",
                    )
                ],
                ["last"],
            ),
            panels.subheader("Resource Usage (relative)"),
            panels.timeseries_percentage(
                "Node Memory relative",
                "Memory usage relative to k8s resource limit of container. Only works in K8s environment",
                [
                    panels.target(
                        '(avg by(namespace, pod) (container_memory_working_set_bytes{namespace=~"$namespace",pod=~"$pod",container=~"$component"})) / (  sum by(namespace, pod) (kube_pod_container_resource_limits{namespace=~"$namespace", pod=~"$pod", container=~"$component", resource="memory", unit="byte"}))',
                        "avg memory usage @ {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    )
                ],
            ),
            panels.timeseries_cpu(
                "Node CPU relative",
                "CPU usage relative to k8s resource limit of container. Only works in K8s environment",
                [
                    panels.target(
                        '(sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval])) by (namespace, pod)) / (sum(kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod))',
                        "cpu usage @ {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                ],
            ),
            panels.subheader("Resource Usage (absolute)"),
            panels.timeseries_memory(
                "Node Memory",
                "The memory usage of each RisingWave component.",
                [
                    panels.target(
                        f"avg({metric('process_resident_memory_bytes')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                        "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                    )
                ],
            ),
            panels.timeseries_cpu(
                "Node CPU",
                "The CPU usage of each RisingWave component.",
                [
                    panels.target(
                        f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                        "cpu usage (total) - {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                    panels.target(
                        f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                        "cpu usage (avg per core) - {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                ],
            ),
            panels.timeseries_count(
                "Meta Cluster",
                "RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the "
                "rest are the followers.",
                [
                    panels.target(
                        f"sum({metric('meta_num')}) by (worker_addr,role)",
                        f"{{role}} @ {{worker_addr}}",
                    )
                ],
                ["last"],
            ),
            panels.subheader("Recovery"),
            panels.timeseries_ops(
                "Recovery Successful Rate",
                "The rate of successful recovery attempts",
                [
                    panels.target(
                        f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by ({NODE_LABEL}, recovery_type)",
                        "{{%s}} ({{recovery_type}})" % NODE_LABEL,
                    )
                ],
                ["last"],
            ),
            panels.timeseries_count(
                "Failed recovery attempts",
                "Total number of failed reocovery attempts",
                [
                    panels.target(
                        f"sum({metric('recovery_failure_cnt')}) by ({NODE_LABEL}, recovery_type)",
                        "{{%s}} ({{recovery_type}})" % NODE_LABEL,
                    )
                ],
                ["last"],
            ),
            panels.timeseries_latency(
                "Recovery latency",
                "Time spent in a successful recovery attempt",
                [
                    *quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('recovery_latency_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}, recovery_type))",
                            f"recovery latency p{legend}" + " ({{recovery_type}}) - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 99, "max"],
                    ),
                    panels.target(
                        f"sum by (le, recovery_type) (rate({metric('recovery_latency_sum')}[$__rate_interval])) / sum by (le) (rate({metric('recovery_latency_count')}[$__rate_interval])) > 0",
                        "recovery latency avg {{recovery_type}}",
                    ),
                ],
                ["last"],
            ),
            panels.subheader("Barrier"),
            panels.timeseries_count(
                "Barrier Number",
                "The number of barriers that have been ingested but not completely processed. This metric reflects the "
                "current level of congestion within the system.",
                [
                    panels.target(
                        f"{metric('all_barrier_nums')}",
                        "all_barrier (database {{database_id}})",
                    ),
                    panels.target(
                        f"{metric('in_flight_barrier_nums')}",
                        "in_flight_barrier (database {{database_id}})",
                    ),
                    panels.target(
                        f"{metric('meta_snapshot_backfill_inflight_barrier_num')}",
                        "snapshot_backfill_in_flight_barrier {{table_id}}",
                    ),
                ],
            ),
            panels.timeseries(
                "Barrier pending time (secs)",
                "The duration from the last committed barrier's epoch time to the current time. This metric reflects the "
                "data freshness of the system. During this time, no new data has been committed.",
                [
                    panels.target(
                        f"timestamp({metric('last_committed_barrier_time')}) - {metric('last_committed_barrier_time')}",
                        "barrier_pending_time (database {{database_id}})",
                    )
                ],
            ),
        ],
    ]
