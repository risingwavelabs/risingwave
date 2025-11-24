from ..common import *
from . import section


# NOTE(kwannoel):
# Relative busy time is computed as:
# 1) negate the idle time;
# 2) compute the max idle time as a baseline offset; and
# 3) add (2) to (1) to obtain a positive relative busy time.
#
# Limitations:
# - This does not consider database isolation; different databases may have different max idle times.
# - Idle time is cumulative (total to date), not a delta over a window. It is mainly useful for the
#   total busy time cost. To observe busy time over a specific period, use the busy time rate.
def _actor_busy_time_relative_target(panels: Panels):
    # NOTE(kwannoel): The output blocking duration is the duration that the output buffer is blocked.
    # We record this metric per edge, and we dispatch concurrently for each edge.
    output_blocking_per_edge_expr = (
        f"sum({metric('stream_actor_output_buffer_blocking_duration_ns')})"
        f"  by (fragment_id, downstream_fragment_id)"
        f"/ ignoring (downstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
    )

    # NOTE(kwannoel): Due to concurrent dispatching, we take the max of the blocking duration per edge,
    # per fragment, to obtain the blocking duration of the fragment.
    # We make sure to fill in zero if there are no dispatchers for a given fragment.
    # This ensures it can still match when joining with the input blocking duration.
    # We don't do the same for the input blocking duration; otherwise, when actors block on input for a long time,
    # we would still record their input blocking duration as zero.
    # Furthermore, the case where an actor has no input is typically in root nodes (e.g., source or table).
    # These are rarely the bottleneck; if they are, consult the input blocking duration panel.
    output_block_expr = (
        f"("
        f"  max("
        f"    {output_blocking_per_edge_expr}"
        f"  ) by (fragment_id)"
        f"  or (sum({metric('stream_actor_count')}) by (fragment_id) * 0)"
        f")"
    )

    # NOTE(kwannoel): The input blocking duration is the duration that the input buffer is blocked.
    # We record this metric per edge, and we merge inputs concurrently for each edge, since we can have multiple
    # merge executors for a given fragment (e.g. union).
    input_blocking_per_edge_expr = (
        f"sum({metric('stream_actor_input_buffer_blocking_duration_ns')})"
        f"  by (fragment_id, upstream_fragment_id)"
        f"/ ignoring (upstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
    )

    # NOTE(kwannoel): We take the max of the blocking duration per edge, per fragment,
    # to get the input blocking duration of the fragment.
    input_block_expr = (
        f"("
        f"  max("
        f"    {input_blocking_per_edge_expr}"
        f"  ) by (fragment_id)"
        f")"
    )
    # NOTE(kwannoel): Busy time is the sum of the output and input blocking durations.
    # It's critical for output_block_expr to be on the left-hand side (LHS), since it always exists.
    # If input_block_expr doesn't exist, it either means the input is blocked, or the actor has no input.
    # In the first case, we don't want to render the blocking duration; treating it as zero would skew the result.
    # In the second case, rendering is not important. As mentioned above, such actors are usually root nodes
    # (e.g., source or table) and are rarely the bottleneck.
    busy_expr = f"(({output_block_expr}) + ({input_block_expr})) or ({input_block_expr})"

    # NOTE(kwannoel): We ignore `fragment_id` to get a global max of busy time.
    baseline_busy_expr = f"ignoring (fragment_id) group_left() max({busy_expr})"

    return panels.target(
        f"-({busy_expr}) + {baseline_busy_expr}",
        "fragment {{fragment_id}}",
    )

# NOTE(kwannoel): Busy rate is analogous to busy time, but over an interval.
# - We divide by the rate interval to obtain a rate, and by 1e9 to convert ns to seconds
#   (rate is per second).
# - Busy rate equals: 1 - idle rate.
# - Idle time is approximated by max(output_blocking_duration, input_blocking_duration) per fragment,
#   because output and input blocking events can overlap across actors and edges. We therefore use the max as an estimate.
def _actor_busy_rate_expr(rate_interval: str):
    output_blocking_rate_per_edge_expr = (
        f"sum(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[{rate_interval}]))"
        f"  by (fragment_id, downstream_fragment_id)"
        f"/ ignoring (downstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
        f"/ 1000000000"
    )
    output_blocking_rate_expr = (
        f"max("
        f"  {output_blocking_rate_per_edge_expr}"
        f") by (fragment_id)"
        f"  or (sum({metric('stream_actor_count')}) by (fragment_id) * 0)"
    )
    input_blocking_rate_per_edge_expr = (
        f"sum(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[{rate_interval}]))"
        f"  by (fragment_id, upstream_fragment_id)"
        f"/ ignoring (upstream_fragment_id) group_left"
        f"  sum({metric('stream_actor_count')}) by (fragment_id)"
        f"/ 1000000000"
    )
    input_blocking_rate_expr = (
        f"max("
        f"    {input_blocking_rate_per_edge_expr}"
        f") by (fragment_id)"
    )
    busy_rate_expr = (
        f"clamp_min("
        f"  1 - ("
        f"    ({output_blocking_rate_expr}) + ({input_blocking_rate_expr}) or"
        f"    ({input_blocking_rate_expr})"
        f"  ), 0"
        f")"
    )
    return busy_rate_expr

def _actor_busy_rate_target(panels: Panels, rate_interval: str):
    actor_busy_rate_expr = _actor_busy_rate_expr(rate_interval)
    return panels.target(
        f"({actor_busy_rate_expr})",
        "fragment {{fragment_id}}",
    )

def _relation_busy_rate_expr(rate_interval: str):
    actor_busy_rate_expr = _actor_busy_rate_expr(rate_interval)
    relation_busy_rate_expr = (
        f"topk(1,"
        f"  ({actor_busy_rate_expr}) * on (fragment_id) group_right {metric('table_info')}"
        f") by (materialized_view_id)"
    )
    relation_busy_rate_with_metadata_expr = (
        f"label_replace(({relation_busy_rate_expr}), 'id', '$1', 'materialized_view_id', '(.*)')"
        f"* on (id) group_left (name, type) {metric('relation_info')}"
    )
    return relation_busy_rate_with_metadata_expr

def _relation_busy_rate_target(panels: Panels, rate_interval: str):
    return panels.target(
        _relation_busy_rate_expr(rate_interval),
        "name {{name}} id {{id}} type {{type}} fragment {{fragment_id}}",
    )

@section
def _(outer_panels: Panels):
    panels = outer_panels
    return [
        outer_panels.row("Cluster Essential Information"),
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
            panels.timeseries_percentage(
                "Node Memory relative",
                "Memory usage relative to k8s resource limit of container. Only works in K8s environment",
                [
                    panels.target(
                        '(avg by(namespace, pod) (container_memory_working_set_bytes{namespace=~"$namespace",pod=~"$pod",container=~"$component"})) / (  sum by(namespace, pod) (kube_pod_container_resource_limits{namespace=~"$namespace", pod=~"$pod", container="$component", resource="memory", unit="byte"}))',
                        "avg memory usage @ {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
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
            panels.timeseries_count(
                "Meta Cluster",
                "RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the "
                "rest are the followers.",
                [
                    panels.target(
                        f"sum({metric('meta_num')}) by (worker_addr,role)",
                        "{{worker_addr}} @ {{role}}",
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
            panels.timeseries_latency(
                "Barrier Latency",
                "The time that the data between two consecutive barriers gets fully processed, i.e. the computation "
                "results are made durable into materialized views or sink to external systems. This metric shows to users "
                "the freshness of materialized views.",
                quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, database_id))",
                        f"barrier_latency_p{legend} " + " (database {{database_id}})",
                    ),
                    [50, 90, 99, 999, "max"],
                )
                + [
                    panels.target(
                        f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval]) > 0",
                        "barrier_latency_avg (database {{database_id}})",
                    ),
                ]
                + quantile(
                    lambda quantile, legend: panels.target(
                        f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id, barrier_type))",
                        f"snapshot_backfill_barrier_latency_p{legend} table_id[{{{{table_id}}}}] {{{{barrier_type}}}}",
                    ),
                    [50, 90, 99, 999, "max"],
                ),
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
            panels.subheader("Backpressure"),
            panels.timeseries_percentage(
                "Actor Output Blocking Rate (Downstream Backpressure)",
                "The rate that an actor is blocked by its downstream.",
                [
                    # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                    # Thus to calculate the average, we need to manually divide the actor count.
                    #
                    # Note: actor_count is equal to the number of dispatchers for a given downstream fragment,
                    # this holds true as long as we don't support multiple edges between two fragments.
                    panels.target(
                        f"sum(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, downstream_fragment_id) \
                            / ignoring (downstream_fragment_id) group_left sum({metric('stream_actor_count')}) by (fragment_id) \
                            / 1000000000",
                        "fragment {{fragment_id}}->{{downstream_fragment_id}}",
                    ),
                ],
            ),
            panels.timeseries_percentage(
                "Actor Input Blocking Rate (Upstream Backpressure)",
                "The rate that an actor is blocked by its upstream.",
                [
                    # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                    # Thus to calculate the average, we need to manually divide the actor count.
                    panels.target(
                        f"sum(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, upstream_fragment_id) \
                            / ignoring (upstream_fragment_id) group_left sum({metric('stream_actor_count')}) by (fragment_id) \
                            / 1000000000",
                        "fragment {{fragment_id}}<-{{upstream_fragment_id}}",
                    ),
                ],
            ),
            panels.subheader("Busy Rate"),
            panels.timeseries_percentage(
                "Relation Busy Rate",
                "The rate that a relation is busy, i.e. the busy rate of its busiest actor.",
                [
                    _relation_busy_rate_target(panels, "$__rate_interval"),
                ],
            ),
            panels.timeseries_percentage(
                "Relation Busy Rate (10m)",
                "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 10 minutes.",
                [
                    _relation_busy_rate_target(panels, "10m"),
                ],
            ),
            panels.timeseries_percentage(
                "Relation Busy Rate (5m)",
                "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 5 minutes.",
                [
                    _relation_busy_rate_target(panels, "5m"),
                ],
            ),
            panels.timeseries_percentage(
                "Relation Busy Rate (3m)",
                "The rate that a relation is busy, i.e. the busy rate of its busiest actor, over the last 3 minutes.",
                [
                    _relation_busy_rate_target(panels, "3m"),
                ],
            ),
            panels.timeseries_percentage(
                "Actor Busy Rate",
                "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream.",
                [
                    _actor_busy_rate_target(panels, "$__rate_interval"),
                ],
            ),
            panels.timeseries_percentage(
                "Actor Busy Rate (10m)",
                "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 10 minutes.",
                [
                    _actor_busy_rate_target(panels, "10m"),
                ],
            ),
            panels.timeseries_percentage(
                "Actor Busy Rate (5m)",
                "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 5 minutes.",
                [
                    _actor_busy_rate_target(panels, "5m"),
                ],
            ),
            panels.timeseries_percentage(
                "Actor Busy Rate (3m)",
                "The rate that an actor is busy, i.e. the rate that an actor is not blocked by its downstream or upstream, over the last 3 minutes.",
                [
                    _actor_busy_rate_target(panels, "3m"),
                ],
            ),
            panels.timeseries_latency_ns(
                "Actor Busy Time (Relative)",
                "The relative busy time of an actor, i.e. the time that an actor is not blocked by its downstream or upstream.",
                [
                    _actor_busy_time_relative_target(panels),
                ],
            ),
            panels.subheader(
                "Alerts",
                """Alerts in the system group by type:
- Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck or under heavy load. Check 'Barrier Latency' panel.
- Recovery Triggered: cluster recovery is triggered. Check 'Errors by Type' / 'Node Count' panels.
- Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check 'Hummock Manager' section in dev dashboard.
- Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard, and take care of the type of 'Commit Flush Bytes' and 'Compaction Throughput', whether the throughput is too low.
- Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
- Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
- Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
- Write Stall: Compaction cannot keep up. Stall foreground write, Check 'Compaction' section in dev dashboard.
- Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check 'Hummock Manager' section in dev dashboard.
- Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check 'Hummock Manager' and `Compaction` section in dev dashboard and take care of the type of 'Compaction Success Count', whether the number of trivial-move tasks spiking.
- Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check 'Hummock Write' section in dev dashboard and take care of the 'Event handle latency', whether the time consumed exceeds the barrier latency.
- Abnormal Object Storage Failure: the number of object storage failures is too large, exceeding the expected 50. Check 'Object Storage' section in dev dashboard and take care of the 'Object Storage Failure Rate', whether the rate is too high.
""",
                height=10,
            ),
            panels.timeseries_count(
                "Alerts",
                "",
                [
                    panels.target(
                        f"{metric('all_barrier_nums')} >= bool 200",
                        "Too Many Barriers {{database_id}}",
                    ),
                    panels.target(
                        f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) > bool 0 + sum({metric('recovery_failure_cnt')}) > bool 0",
                        "Recovery Triggered {{recovery_type}}",
                    ),
                    panels.target(
                        f"(({metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}) >= bool 100) + "
                        + f"(({metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}) >= bool 100)",
                        "Lagging Version",
                    ),
                    panels.target(
                        f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                        + f"{metric('storage_level_total_file_size')}) by (L0) >= bool 52428800",
                        "Lagging Compaction",
                    ),
                    panels.target(
                        f"{metric('storage_stale_object_count')} >= bool 200",
                        "Lagging Vacuum",
                    ),
                    panels.target(
                        f"{metric('state_store_meta_cache_usage_ratio')} >= bool 1.1",
                        "Abnormal Meta Cache Memory",
                    ),
                    panels.target(
                        f"{metric('state_store_block_cache_usage_ratio')} >= bool 1.1",
                        "Abnormal Block Cache Memory",
                    ),
                    panels.target(
                        f"{metric('state_store_uploading_memory_usage_ratio')} >= bool 0.7",
                        "Abnormal Uploading Memory Usage",
                    ),
                    panels.target(
                        f"{metric('storage_write_stop_compaction_groups')} > bool 0",
                        "Write Stall",
                    ),
                    panels.target(
                        f"{metric('storage_version_size')} >= bool 314572800",
                        "Abnormal Version Size",
                    ),
                    panels.target(
                        f"{metric('storage_delta_log_count')} >= bool 5000",
                        "Abnormal Delta Log Number",
                    ),
                    panels.target(
                        f"{metric('state_store_event_handler_pending_event')} >= bool 10000000",
                        "Abnormal Pending Event Number",
                    ),
                    panels.target(
                        f"{metric('object_store_failure_count')} >= bool 50",
                        "Abnormal Object Storage Failure",
                    ),
                ],
                ["last"],
            ),
            panels.timeseries_count(
                "Errors",
                "Errors in the system group by type",
                [
                    panels.target(
                        f"sum({metric('user_compute_error')}) by (error_type, executor_name, fragment_id)",
                        "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                    ),
                    panels.target(
                        f"sum({metric('user_source_error')}) by (error_type, source_id, source_name, fragment_id)",
                        "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                    ),
                    panels.target(
                        f"sum({metric('user_sink_error')}) by (error_type, sink_id, sink_name, fragment_id)",
                        "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                    ),
                    panels.target(
                        f"{metric('source_status_is_up')} == 0",
                        "source error: source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                        % NODE_LABEL,
                    ),
                    panels.target(
                        f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                        "remote storage error {{type}}: {{%s}} @ {{%s}}"
                        % (COMPONENT_LABEL, NODE_LABEL),
                    ),
                    # We add a small constant 0.05 to make sure that the counter jumps from null to not-null,
                    # the line will be flat at y=0.05 instead of disappearing.
                     panels.target(
                        f"sum(irate({metric('user_compute_error_cnt')}[$__rate_interval])) by (error_type, executor_name, fragment_id) or "
                        + f"sum({metric('user_compute_error_cnt')}) by (error_type, executor_name, fragment_id) * 0 + 0.05 "
                        + f"unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_compute_error_cnt')}[20s])) > 0)",
                        "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                    ),
                    panels.target(
                        f"sum(irate({metric('user_source_error_cnt')}[$__rate_interval])) by (error_type, source_id, source_name, fragment_id) or "
                        + f"sum({metric('user_source_error_cnt')}) by (error_type, source_id, source_name, fragment_id) * 0 + 0.05 "
                        + f"unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_source_error_cnt')}[20s])) > 0)",
                        "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                    ),
                    panels.target(
                        f"sum(irate({metric('user_sink_error_cnt')}[$__rate_interval])) by (error_type, sink_id, sink_name, fragment_id) or "
                        + f"sum({metric('user_sink_error_cnt')}) by (error_type, sink_id, sink_name, fragment_id) * 0 + 0.05 "
                        + f"unless on({COMPONENT_LABEL}, {NODE_LABEL}) ((absent_over_time({metric('user_sink_error_cnt')}[20s])) > 0)",
                        "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                    ),
                ],
            ),
            panels.subheader("User Streaming Errors"),
            panels.timeseries_count(
                "Compute Errors by Type",
                "Errors that happened during computation. Check the logs for detailed error message.",
                [
                    panels.target(
                        f"sum({metric('user_compute_error')}) by (error_type, executor_name, fragment_id)",
                        "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                    ),
                ],
            ),
            panels.timeseries_count(
                "Source Errors by Type",
                "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                [
                    panels.target(
                        f"sum({metric('user_source_error')}) by (error_type, source_id, source_name, fragment_id)",
                        "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                    ),
                ],
            ),
            panels.timeseries_count(
                "Parquet Source Skip Count",
                "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                [
                    panels.target(
                        f"{metric('parquet_source_skip_row_count')}",
                        "source_id={{source_id}} @ source_name =  {{source_name}}",
                    )
                ],
            ),
            panels.timeseries_count(
                "Sink Errors by Type",
                "Errors that happened during data sink out. Check the logs for detailed error message.",
                [
                    panels.target(
                        f"sum({metric('user_sink_error')}) by (error_type, sink_id, sink_name, fragment_id)",
                        "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                    ),
                ],
            ),
        ],
    ]
