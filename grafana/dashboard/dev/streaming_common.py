from ..common import Panels, metric
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

