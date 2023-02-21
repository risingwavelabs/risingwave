// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::{min, Ordering};
use std::collections::BinaryHeap;
use std::iter;
use std::ops::{Range, RangeInclusive};

use itertools::Itertools;
use num_integer::Integer;
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::VirtualNode;

use crate::model::ActorId;

/// (start, end, `actor_id`)
type OccupiedVnodeRangeInclusive = (usize, usize, ActorId);
/// (length, `start_position`)
type FreeSegment = (usize, usize);
type FreeSegments = BinaryHeap<FreeSegment>;
type Builder = (ActorId, Vec<Range<usize>>);

// Calculate length of a closed interval `[start, end]`. `seg.0` represents `start` and `seg.1`
// represents `end`.
#[inline(always)]
fn get_segment_len(seg: &OccupiedVnodeRangeInclusive) -> usize {
    seg.1 + 1 - seg.0
}

/// Calculate the weight (based on its length) of gap between two segments.
///
/// Note that caller should ensure that `!fixed_segments.is_empty()`
fn calc_gap_weight(
    fixed_segments: &[OccupiedVnodeRangeInclusive],
    expected: usize,
    index_original: usize,
) -> (usize, usize) {
    // 'Gap #k' (starting from zero) indicates the gap before 'Segment #k' (starting from zero).
    let base_len = if index_original == 0 {
        fixed_segments[index_original].0
    } else if index_original == fixed_segments.len() {
        VirtualNode::COUNT - 1 - fixed_segments[index_original - 1].1
    } else {
        fixed_segments[index_original].0 - 1 - fixed_segments[index_original - 1].1
    };
    (
        if expected == 0 {
            // When `expected == 0`, we do not need to consider locality.
            base_len
        } else {
            // When there is an extra segment whose length is `expected + 1`, we need to shrink one
            // segment. As a result, the corresponding gap will increase by one. With a modulus of
            // `expected`, larger gap is chosen. e.g. Increasing a gap from zero to one with a
            // modulus of `expected` will result in an isolate vnode, which has no locality at all.
            base_len % expected
        },
        base_len,
    )
}

/// Check whether the lengths of the left adjacent segment and the right adjacent segment of a gap
/// is exactly `quota`. 'Gap #k' (starting from zero) indicates the gap before 'Segment #k'
/// (starting from zero).
fn is_adjacent_quota(
    fixed_segments: &[OccupiedVnodeRangeInclusive],
    quota: usize,
    index_original: usize,
) -> (bool, bool) {
    let left = if index_original == 0 {
        // In this case the gap does not have a left adjacent segment.
        false
    } else {
        get_segment_len(&fixed_segments[index_original - 1]) == quota
    };
    let right = if index_original == fixed_segments.len() {
        // In this case the gap does not have a right adjacent segment.
        false
    } else {
        get_segment_len(&fixed_segments[index_original]) == quota
    };
    (left, right)
}

/// Select indices of segments whose length is exactly `quota`.
fn select_quota_length_segments_indices(
    fixed_segments: &[OccupiedVnodeRangeInclusive],
    quota: usize,
) -> Vec<usize> {
    fixed_segments
        .iter()
        .enumerate()
        .filter(|(_, segment)| get_segment_len(segment) == quota)
        .map(|(idx, _)| idx)
        .collect_vec()
}

/// Given some existing segments, calculate the set of vnodes that do not belong to these segments.
fn calc_uncovered_segments(fixed_segments: &[OccupiedVnodeRangeInclusive]) -> Vec<FreeSegment> {
    let mut uncovered_segments = Vec::with_capacity(fixed_segments.len() + 1);
    let mut uncovered_start_point = 0;
    for (start, end, _) in fixed_segments {
        if *start > uncovered_start_point {
            uncovered_segments.push((*start - uncovered_start_point, uncovered_start_point));
        }
        uncovered_start_point = end + 1;
    }
    // Handle the rightest uncovered segment. (The leftest has been handled at the first iteration
    // of loop.)
    if VirtualNode::COUNT > uncovered_start_point {
        uncovered_segments.push((
            VirtualNode::COUNT - uncovered_start_point,
            uncovered_start_point,
        ));
    }
    uncovered_segments
}

/// Calculate gaps which are adjacent to the segments whose length is exactly `quota`.
fn calc_neighbourhood_space_of_quota_length_segments(
    fixed_segments: &[OccupiedVnodeRangeInclusive],
    quota_length_segments_indices: Vec<usize>,
    expected: usize,
) -> Vec<((usize, usize), usize)> {
    let quota_length_segments_cnt = quota_length_segments_indices.len();
    let mut neighbourhood_spaces = Vec::with_capacity(quota_length_segments_cnt * 2);
    for i in 0..quota_length_segments_cnt {
        let cur = quota_length_segments_indices[i];
        // If `quota_length_segments_indices[i - 1] + 1 == cur`, the left gap has already been
        // calculated as the right gap of last segment.
        if i == 0 || quota_length_segments_indices[i - 1] + 1 < cur {
            // 'Gap #k' (starting from zero) indicates the gap before 'Segment #k' (starting from
            // zero), so the left gap of 'Segment #k' is 'Gap #k'.
            neighbourhood_spaces.push((calc_gap_weight(fixed_segments, expected, cur), cur));
        }
        // 'Gap #k' (starting from zero) indicates the gap before 'Segment #k' (starting from zero),
        // so the right gap of 'Segment #k' is 'Gap #(k+1)'.
        neighbourhood_spaces.push((calc_gap_weight(fixed_segments, expected, cur + 1), cur + 1));
    }
    neighbourhood_spaces
}

pub(super) fn divide_vnode_into_occupied_and_vacant(
    rest: &[(ActorId, Bitmap)],
    quota: usize,
    expected: usize,
    remain: usize,
    flexible_actors: &mut Vec<ActorId>,
) -> (Vec<OccupiedVnodeRangeInclusive>, usize, Vec<FreeSegment>) {
    // Generate occupied part and prune excess actors which contain `expected + 1` vnodes. View
    // comments in function `gen_fixed_segments_with_at_most_remain_longer_than_expected` for
    // details.
    let (fixed_segments, unallocated_remain) =
        gen_fixed_segments_with_at_most_remain_longer_than_expected(
            rest,
            quota,
            expected,
            remain,
            flexible_actors,
        );

    // Select vnodes except the occupied part above.
    let uncovered_segments = calc_uncovered_segments(&fixed_segments);

    // `unallocated_remain` is the result of subtracting the number of actors which now have
    // `expected + 1` vnodes from `remain`.
    (fixed_segments, unallocated_remain, uncovered_segments)
}

/// Generate occupied part with at most `remain` actors which have `expected + 1` vnodes.
fn gen_fixed_segments_with_at_most_remain_longer_than_expected(
    rest: &[(ActorId, Bitmap)],
    quota: usize,
    expected: usize,
    remain: usize,
    flexible_actors: &mut Vec<ActorId>,
) -> (Vec<OccupiedVnodeRangeInclusive>, usize) {
    let mut fixed_segments = generate_fixed_segments(rest, quota, flexible_actors);

    let unallocated_remain =
        prune_excess_remainder_segments(&mut fixed_segments, quota, expected, remain);

    (fixed_segments, unallocated_remain)
}

/// If the number of actors which have `expected + 1` vnodes is greater than `remain`, we will prune
/// until it is equal to `remain`.
fn prune_excess_remainder_segments(
    fixed_segments: &mut [OccupiedVnodeRangeInclusive],
    quota: usize,
    expected: usize,
    remain: usize,
) -> usize {
    if remain > 0 {
        let quota_length_segments_indices =
            select_quota_length_segments_indices(fixed_segments, quota);
        let quota_length_segments_cnt = quota_length_segments_indices.len();
        if quota_length_segments_cnt > remain {
            let overflow_cnt = quota_length_segments_cnt - remain;
            extend_neighbourhood_to_prune_quota_length_segments(
                fixed_segments,
                quota_length_segments_indices,
                quota,
                expected,
                overflow_cnt,
            );
            // After pruning, the number of actors which have `expected + 1` vnodes must be equal to
            // `remain`, so the difference is 0.
            0
        } else {
            // If the number of actors which have `expected + 1` vnodes is less than `remain`, we
            // record the difference and do nothing.
            remain - quota_length_segments_cnt
        }
    } else {
        // We don't need to anything if `remain == 0`, i.e., `expected == quota`.
        0
    }
}

/// When we need to shrink one segment by one, the corresponding gap (neighbourhood space which is
/// adjacent to the segment) will increase by one. Alternatively, we can choose one neighbourhood
/// space of the segment whose length is `expected + 1` and extend this neighbourhood by one so as
/// to shrink the segment.
fn extend_neighbourhood_to_prune_quota_length_segments(
    fixed_segments: &mut [OccupiedVnodeRangeInclusive],
    quota_length_segments_indices: Vec<usize>,
    quota: usize,
    expected: usize,
    overflow_cnt: usize,
) {
    let neighbourhood_spaces = calc_neighbourhood_space_of_quota_length_segments(
        fixed_segments,
        quota_length_segments_indices,
        expected,
    );
    prune_quota_length_segments(
        fixed_segments,
        neighbourhood_spaces,
        quota,
        expected,
        overflow_cnt,
    );
}

fn prune_quota_length_segments(
    fixed_segments: &mut [OccupiedVnodeRangeInclusive],
    neighbourhood_spaces: Vec<((usize, usize), usize)>,
    quota: usize,
    expected: usize,
    mut overflow_cnt: usize,
) {
    // The weight strategy has been explained in comments in function `calc_gap_weight`.
    let mut neighbourhood_spaces = BinaryHeap::from(neighbourhood_spaces);
    while overflow_cnt > 0 {
        let (index_original, left, right) = loop {
            // We can call `unwrap()` because the number of gaps which are adjacent to quota length
            // segments are always more than the number of quota length segments. i.e. We can always
            // find a valid gap.
            let (_, index_original) = neighbourhood_spaces.pop().unwrap();
            let (left, right) = is_adjacent_quota(fixed_segments, quota, index_original);
            // If the left segment or the right segment is still has the length `expected + 1`, this
            // gap is a valid gap (because we can then shrink that segment).
            if left || right {
                break (index_original, left, right);
            }
            // When a segment shrank due to its left gap, its right gap might become invalid at that
            // moment. Therefore the top of heap may be invalid. In this situation, we need to loop
            // until a valid gap item.
        };
        // Shrink the segment by one.
        if left {
            // 'Gap #k' (starting from zero) indicates the gap before 'Segment #k' (starting from
            // zero), so the left segment of 'Gap #k' is 'Segment #(k-1)'.
            fixed_segments[index_original - 1].1 -= 1;
            overflow_cnt -= 1;
            if right {
                // The gap not only serves as the right gap of its left segment, but also serves as
                // the left gap of its right segment. Since its right segment has not yet shrunk,
                // this gap can still serve as a candidate gap (and indicates its right segment).
                neighbourhood_spaces.push((
                    calc_gap_weight(fixed_segments, expected, index_original),
                    index_original,
                ));
            }
        } else {
            // 'Gap #k' (starting from zero) indicates the gap before 'Segment #k' (starting from
            // zero), so the right segment of 'Gap #k' is 'Segment #k'.
            fixed_segments[index_original].0 += 1;
            overflow_cnt -= 1;
        }
    }
}

/// Generate fixed segments based on the original plan.
fn generate_fixed_segments(
    rest: &[(ActorId, Bitmap)],
    quota: usize,
    flexible_actors: &mut Vec<ActorId>,
) -> Vec<OccupiedVnodeRangeInclusive> {
    let immovable_segments = fetch_immovable_segments(rest, flexible_actors);
    let flexible_actor_cnt = flexible_actors.len();

    // Old vnode set which an actor had in the past may not contain exactly `quota` vnodes, so we
    // need some adjustment.
    adjust_segments_to_quota(immovable_segments, quota, flexible_actor_cnt)
}

/// The result of this function will have exactly `min(target_actor_count, VirtualNode::COUNT)`
/// segments.
fn combine_fixed_and_other_segments(
    fixed_segments: Vec<OccupiedVnodeRangeInclusive>,
    other_segments: Vec<OccupiedVnodeRangeInclusive>,
    quota: usize,
) -> Vec<(usize, usize, usize, ActorId)> {
    fixed_segments
        .into_iter()
        .chain(other_segments.into_iter())
        // The estimation `quota - (end + 1 - start)` will be amended later in function
        // `appoint_actors_to_vacant_interval_sets`.
        .map(|(start, end, actor_id)| (quota - (end + 1 - start), start, end, actor_id))
        .collect_vec()
}

/// Pick the longest continuous coverage among different plans.
fn pick_longest_coverage(
    last_utleast_processed: usize,
    last_cost2coverages: &[Vec<usize>],
    quota: usize,
    flexible_actor_cnt: usize,
) -> (usize, (usize, usize, usize)) {
    let mut best = None;
    for (position_delta, last_cost2coverage) in last_cost2coverages.iter().enumerate() {
        // `prev_processed` is the position of the first uncovered vnode.
        let prev_processed = last_utleast_processed + position_delta;
        // `last_cost2coverage` is a mapping from `cost` to `coverage`. `cost` is the amount of
        // usage of flexible actors.
        for (last_cost, last_coverage) in last_cost2coverage.iter().enumerate() {
            // We can get more coverage by the last assignment here. (View details below.)
            let whole_coverage = last_coverage
                + min(
                    // We can assign `quota` continuous vnodes to each unused flexible actor
                    // (`cost` is the amount of usage of flexible actors) so the coverage can
                    // increase `(flexible_actor_cnt - last_cost) * quota`.
                    (flexible_actor_cnt - last_cost) * quota,
                    // However, since `prev_processed` is the position of the first uncovered
                    // vnode, the remaining space for our assignment is only `VirtualNode::COUNT -
                    // prev_processed`.
                    VirtualNode::COUNT - prev_processed,
                );
            if best.map_or(true, |(best_coverage, _)| best_coverage < whole_coverage) {
                best = Some((whole_coverage, (position_delta, last_cost, *last_coverage)));
            }
        }
    }

    best.unwrap()
}

/// Fill vnodes which are not occupied, i.e., assigning them to actors.
#[allow(clippy::too_many_arguments)]
pub(super) fn appoint_actors_to_vacant_interval_sets(
    fixed_segments: Vec<OccupiedVnodeRangeInclusive>,
    uncovered_segments: Vec<FreeSegment>,
    quota: usize,
    expected: usize,
    remain: usize,
    unallocated_remain: usize,
    target_actor_count: usize,
    flexible_actors: &mut Vec<ActorId>,
) -> Vec<Builder> {
    let mut uncovered_segments = FreeSegments::from(uncovered_segments);

    // If `target_actor_count > VirtualNode::COUNT`, not all actors can be allocated to.
    let unallocated = min(target_actor_count, VirtualNode::COUNT) - fixed_segments.len();
    let other_segments = allocate_other_segments(
        &mut uncovered_segments,
        quota,
        expected,
        unallocated,
        unallocated_remain,
        flexible_actors,
    );

    // The length of `segments` should be `min(target_actor_count, VirtualNode::COUNT)`.
    let mut segments = combine_fixed_and_other_segments(fixed_segments, other_segments, quota);
    let segments_len = segments.len();
    if remain > 0 {
        // Let actors which have the TopN (N is `remain`) vnodes already allocated own `expected +
        // 1` vnodes in the end and other actors own `expected` vnodes in the end.
        if remain < segments_len {
            // Since the first item of tuple is `quota - XX`, actors which have more vnodes
            // allocated will appear first after selection.
            segments.select_nth_unstable(remain);
        }
        // Other actors can only have `expected` vnodes in the end but we used `quota - XX` (i.e.
        // `expected + 1 - XX`) to calculate, so we need adjustment here.
        for segment in segments.iter_mut().skip(remain) {
            segment.0 -= 1;
        }
    }

    // The different (filling) targets for actors (`expected` or `quota`) have been set above.
    fill_uncovered_vnodes(segments, &mut uncovered_segments)
}

fn allocate_other_segments(
    uncovered_segments: &mut FreeSegments,
    quota: usize,
    expected: usize,
    mut unallocated: usize,
    mut unallocated_remain: usize,
    flexible_actors: &mut Vec<ActorId>,
) -> Vec<OccupiedVnodeRangeInclusive> {
    let mut other_segments = Vec::with_capacity(unallocated);
    while unallocated > 0 {
        unallocated -= 1;
        // We allocate `quota` in the first `unallocated_remain` allocation, and allocate `expected`
        // then.
        let current_target = if unallocated_remain > 0 {
            unallocated_remain -= 1;
            quota
        } else {
            expected
        };
        let (longest_uncovered, start_position) = uncovered_segments.pop().unwrap();
        if longest_uncovered > current_target {
            // Free space is larger than allocation target. Use prefix of free space.
            other_segments.push((
                start_position,
                start_position + current_target - 1,
                flexible_actors.pop().unwrap(),
            ));
            // The remain of free space should be sent back.
            uncovered_segments.push((
                longest_uncovered - current_target,
                start_position + current_target,
            ));
        } else {
            // Free space is not larger than allocation target. Use the whole free space.
            other_segments.push((
                start_position,
                start_position + longest_uncovered - 1,
                flexible_actors.pop().unwrap(),
            ));
        }
    }
    other_segments
}

fn fetch_immovable_segments(
    rest: &[(ActorId, Bitmap)],
    flexible_actors: &mut Vec<ActorId>,
) -> Vec<(RangeInclusive<usize>, ActorId)> {
    let mut immovable_segments = vec![];
    for (actor_id, bitmap) in rest {
        let maximum_len = (bitmap.count_ones() + 1) / 2;
        let mut is_flexible = true;
        for high_range in bitmap.high_ranges() {
            if high_range.end() - high_range.start() + 1 >= maximum_len {
                immovable_segments.push((high_range, *actor_id));
                is_flexible = false;
                break;
            }
        }
        if is_flexible {
            flexible_actors.push(*actor_id);
        }
    }
    immovable_segments
}

/// Assign unallocated vnodes to actors.
fn fill_uncovered_vnodes(
    segments: Vec<(usize, usize, usize, ActorId)>,
    uncovered_segments: &mut FreeSegments,
) -> Vec<Builder> {
    // `request` is the number of vnodes that an actor needs, i.e., the result of subtracting the
    // amount of vnode that it already has from its target (`quota` or `expected`, view
    // function `appoint_actors_to_vacant_interval_sets` for details).
    let (mut builders, mut requests) =
        create_builders_and_calculate_demands_from_segments(segments);

    // `req_cnt` is the amount of vnodes of demand.
    while let Some((req_cnt, idx)) = requests.pop() {
        // We can call `unwrap()` because there must be enough unused vnodes to allocate, or the
        // calculation of `request` is incorrect.
        let (longest_uncovered, start_position) = uncovered_segments.pop().unwrap();
        let response = match longest_uncovered.cmp(&req_cnt) {
            // Free space is less than allocation target.
            Ordering::Less => {
                // The remain of allocation request should be sent back.
                requests.push((req_cnt - longest_uncovered, idx));
                // The current response is the whole free space.
                start_position..start_position + longest_uncovered
            }
            // Free space is equal to allocation target. The response is the whole free space.
            Ordering::Equal => start_position..start_position + longest_uncovered,
            // Free space is larger than allocation target.
            Ordering::Greater => {
                // The remain of free space should be sent back.
                uncovered_segments.push((longest_uncovered - req_cnt, start_position + req_cnt));
                // The response is prefix of free space.
                start_position..start_position + req_cnt
            }
        };
        builders[idx].1.push(response);
    }
    builders
}

/// `builder` is the vnode ranges that an actor has. `demand` is the number of vnodes that an
/// actor needs, i.e., the result of subtracting the amount of vnode that it already has from its
/// target (`quota` or `expected`, view function
/// `appoint_actors_to_vacant_interval_sets` for details).
fn create_builders_and_calculate_demands_from_segments(
    segments: Vec<(usize, usize, usize, ActorId)>,
) -> (Vec<Builder>, FreeSegments) {
    let builders = segments
        .iter()
        // We are transforming a closed interval to an open interval.
        .map(|(_, start, end, actor_id)| (*actor_id, vec![*start..(*end + 1)]))
        .collect_vec();
    let requests = BinaryHeap::from_iter(
        segments
            .iter()
            .enumerate()
            .filter(|(_, (req_cnt, _, _, _))| *req_cnt > 0)
            .map(|(idx, (req_cnt, _, _, _))| (*req_cnt, idx)),
    );

    (builders, requests)
}

/// The definition of `rightest_start_position`: If previous segments exceed the point
/// `rightest_start_position`, the actor have to suffer some extra vnode movement to achieve `quota`
/// (i.e. The actor will lose vnode affinity).
fn calc_rightest_start_positions_in_immovable_segments(
    immovable_segments: &[(RangeInclusive<usize>, ActorId)],
    quota: usize,
) -> Vec<usize> {
    immovable_segments
        .iter()
        .map(|(high_range, _)| {
            if high_range.end() - high_range.start() < quota {
                // Since the amount of vnodes is less than `quota`, each vnode should be retained
                // and cannot be moved. Therefore previous segments cannot exceed the start point.
                *high_range.start()
            } else {
                // A continuous vnode range starting with `high_range.end() - quota + 1` can still
                // end with `high_range.end()` and the vnodes used throughout the range are all
                // vnodes that the actor owned, so vnode affinity is kept.
                high_range.end() - quota + 1
            }
        })
        // Guard.
        .chain(iter::once(VirtualNode::COUNT))
        .collect_vec()
}

/// Old vnode set which an actor had in the past may not contain exactly `quota` vnodes, so we
/// need some adjustment.
fn adjust_segments_to_quota(
    mut immovable_segments: Vec<(RangeInclusive<usize>, ActorId)>,
    quota: usize,
    flexible_actor_cnt: usize,
) -> Vec<OccupiedVnodeRangeInclusive> {
    immovable_segments.sort_by_key(|(high_range, _)| *high_range.start());

    let rightest_start_positions =
        calc_rightest_start_positions_in_immovable_segments(&immovable_segments, quota);

    // Record intermediate results to get the scheme of the longest coverage later.
    let mut archived_results = Vec::with_capacity(immovable_segments.len());
    let (_, mut target) = {
        let mut last_utleast_processed = 0;
        let mut last_cost2coverages = vec![vec![0]];
        for (idx, (high_range, _)) in immovable_segments.iter().enumerate() {
            // `utleast_processed` is the leftest possible value of the position just after the end
            // of current segment. If the length of `high_range` exceeds quota, the
            // current segment may end before `high_range.end()`. (e.g. We choose to retain the
            // prefix of `high_range`)
            let utleast_processed = min(high_range.end() + 1, high_range.start() + quota);
            // `utmost_processed` is the rightest possible value of the position just after the end
            // of current segment.
            let utmost_processed = min(
                rightest_start_positions[idx] + quota,
                // Limited by next segment, because of the definition of `rightest_start_position`.
                // (View comments in function
                // `calc_rightest_start_positions_in_immovable_segments` for details).
                rightest_start_positions[idx + 1],
            );
            // `cost2coverage` is a mapping from `cost` to `coverage`. `cost` is the amount of usage
            // of flexible actors.
            // `cost2coverage[i][j]` indicates the longest coverage when we end before position `i`
            // and have used `j` flexible actors (i.e. `j` segments whose length is `quota`).
            // However, to save memory, we subtract `utleast_processed` from real position in index.
            // As a result, `cost2coverage[i][j]` represents that the longest coverage when we end
            // before position `i + utleast_processed` and have used `j` flexible
            // actors.
            let mut cost2coverages = vec![vec![]; utmost_processed - utleast_processed + 1];
            cost2coverages[0].reserve_exact(flexible_actor_cnt + 1);
            cost2coverages[utmost_processed - utleast_processed]
                .reserve_exact(flexible_actor_cnt + 1);
            for (position_delta, last_cost2coverage) in last_cost2coverages.iter().enumerate() {
                // To save memory, we subtract `utleast_processed` from real position in index.
                // Here we resume the real position which is just after the end of the last segment.
                let prev_processed = last_utleast_processed + position_delta;
                let last_len = last_cost2coverage.len();
                let (full_case, must_fulfill_seg_cnt) = if utleast_processed > prev_processed {
                    // We can fill the gap between the current segment and the last segment by
                    // segments which will be allocated to flexible actors.
                    let (perfect_seg_cnt, remainder_seg_len) =
                        (utleast_processed - prev_processed).div_rem(&quota);
                    if perfect_seg_cnt > 0 {
                        // This branch handles the case that we choose to use less than or equal to
                        // `this_budget` PERFECT flexible actors (i.e. less than or equal to
                        // `this_budget` segments whose length is `quota`)
                        // to fill the gap between the current segment and
                        // the last segment.
                        let this_budget = perfect_seg_cnt - 1;
                        // In this case, we can end current segment at the earliest position, aka
                        // `utleast_processed`. Since we subtract
                        // `utleast_processed` from real position in index to save memory,
                        // the index should be `utleast_processed - utleast_processed`, aka `0`.
                        let this = &mut cost2coverages[0];
                        let last_len_impacts = min(last_len + this_budget, flexible_actor_cnt + 1);
                        if last_len_impacts > this.len() {
                            this.resize(last_len_impacts, 0);
                        }
                        for (last_cost, last_coverage) in last_cost2coverage.iter().enumerate() {
                            // We cannot use more than `flexible_actor_cnt` flexible actors.
                            let cur_cost = min(last_cost + this_budget, flexible_actor_cnt);
                            // `cur_cost - last_cost` is the number of PERFECT flexible actors we
                            // use in this stage. i.e. We have use
                            // `cur_cost - last_cost` segments
                            // whose length is `quota`, so the sum of length is
                            // `(cur_cost - last_cost) * quota`.
                            // Another `quota` is for the current segment itself.
                            if *last_coverage + (cur_cost - last_cost) * quota + quota
                                > this[cur_cost]
                            {
                                this[cur_cost] =
                                    *last_coverage + (cur_cost - last_cost) * quota + quota;
                            }
                        }
                    }
                    // If remainder is not zero, we can try `this_budget + 1` flexible actors, aka
                    // `perfect_seg_cnt` flexible actors, BUT one of flexible actors may not be a
                    // PERFECT actor, i.e., the length of its segment may be
                    // less than `quota`. (Because we do not have enough space.)
                    (remainder_seg_len > 0, perfect_seg_cnt)
                } else {
                    // There is not enough space for flexible actors. We can use no flexible actors.
                    (true, 0)
                };
                if full_case {
                    // This branch handles the case that we choose to use more than
                    // `this_budget` flexible actors to fill the gap between the current segment and
                    // the last segment. However, one of flexible actors may not be a PERFECT
                    // actor, i.e., the length of its segment may be less than `quota`.
                    // (Because we do not have enough space.)
                    //
                    // The position just after the end of current segment in this situation
                    // is `cur_processed`.
                    let cur_processed = min(
                        prev_processed + (must_fulfill_seg_cnt + 1) * quota,
                        // This item represents our space's limit, which is mentioned above.
                        utmost_processed,
                    );
                    // The coverage we gain in current stage.
                    let gain = cur_processed - prev_processed;
                    // We subtract `utleast_processed` from real position in index to save memory.
                    let this = &mut cost2coverages[cur_processed - utleast_processed];
                    // We cannot use more than `flexible_actor_cnt` flexible actors.
                    if must_fulfill_seg_cnt <= flexible_actor_cnt {
                        // Since we cannot use more than `flexible_actor_cnt` flexible actors,
                        // if we have used too many flexible actors before,
                        // we cannot use more than `this_budget` flexible actors here.
                        // As a result, we can only have used a few flexible actors before,
                        // which is represented by `meaningful_last_len < last_len`.
                        let meaningful_last_len =
                            min(last_len, flexible_actor_cnt + 1 - must_fulfill_seg_cnt);
                        if meaningful_last_len + must_fulfill_seg_cnt > this.len() {
                            this.resize(meaningful_last_len + must_fulfill_seg_cnt, 0);
                        }
                        for last_cost in 0..meaningful_last_len {
                            if last_cost2coverage[last_cost] + gain
                                > this[last_cost + must_fulfill_seg_cnt]
                            {
                                this[last_cost + must_fulfill_seg_cnt] =
                                    last_cost2coverage[last_cost] + gain;
                            }
                        }
                    }
                }
            }
            archived_results.push((last_utleast_processed, last_cost2coverages));
            last_utleast_processed = utleast_processed;
            last_cost2coverages = cost2coverages;
        }

        pick_longest_coverage(
            last_utleast_processed,
            &last_cost2coverages,
            quota,
            flexible_actor_cnt,
        )
    };

    // `target.0` is position, `target.1` is cost (usage of flexible actors) and `target.2` is
    // coverage.

    // We seek backward to get how we get the `longest_coverage` above.
    let mut result_segments = Vec::with_capacity(immovable_segments.len());
    for (idx, (high_range, actor_id)) in immovable_segments.iter().enumerate().rev() {
        let utleast_processed = min(high_range.end() + 1, high_range.start() + quota);
        let utmost_processed = min(
            rightest_start_positions[idx] + quota,
            rightest_start_positions[idx + 1],
        );
        let (last_utleast_processed, last_cost2coverages) = archived_results.pop().unwrap();
        for (position_delta, last_cost2coverage) in last_cost2coverages.iter().enumerate() {
            let prev_processed = last_utleast_processed + position_delta;
            let last_len = last_cost2coverage.len();
            let (full_case, must_fulfill_seg_cnt) = if utleast_processed > prev_processed {
                let (perfect_seg_cnt, remainder_seg_len) =
                    (utleast_processed - prev_processed).div_rem(&quota);
                if perfect_seg_cnt > 0 && target.0 == 0 {
                    // This branch handles the case that we choose to use less than or equal to
                    // `this_budget` flexible actors here.
                    let this_budget = perfect_seg_cnt - 1;
                    if target.1 == flexible_actor_cnt || target.1 >= this_budget {
                        // Because we used
                        // `let cur_cost = min(last_cost + this_budget, flexible_actor_cnt);`
                        // in the previous calculation.
                        let last_cost_range = if target.1 == flexible_actor_cnt {
                            flexible_actor_cnt.saturating_sub(this_budget)..=flexible_actor_cnt
                        } else {
                            let only_case = target.1 - this_budget;
                            only_case..=only_case
                        };
                        let mut found = false;
                        for last_cost in last_cost_range {
                            if last_cost >= last_len {
                                break;
                            }
                            if last_cost2coverage[last_cost]
                                + (target.1 - last_cost) * quota
                                + quota
                                == target.2
                            {
                                // We had enough space of `this_budget` PERFECT flexible actors
                                // (view comments above),
                                // which indicates the length of its segment is `quota`,
                                // and the length of current segment is still `quota`.
                                result_segments.push((
                                    utleast_processed - quota,
                                    utleast_processed - 1,
                                    *actor_id,
                                ));
                                // `target.0` is position, `target.1` is cost (usage of flexible
                                // actors) and `target.2` is coverage.
                                // Once we found, we update `target` and continue to seek backward
                                // recursively.
                                target = (position_delta, last_cost, last_cost2coverage[last_cost]);
                                found = true;
                                // It will jumps to the next `break` below.
                                break;
                            }
                        }
                        if found {
                            break;
                        }
                    }
                }
                (remainder_seg_len > 0, perfect_seg_cnt)
            } else {
                (true, 0)
            };
            if full_case {
                // This branch handles the case that we choose to use more than
                // `this_budget` flexible actors here.
                let cur_processed = min(
                    prev_processed + (must_fulfill_seg_cnt + 1) * quota,
                    utmost_processed,
                );
                let gain = cur_processed - prev_processed;
                if must_fulfill_seg_cnt <= flexible_actor_cnt
                    && target.0 == cur_processed - utleast_processed
                    && target.1 >= must_fulfill_seg_cnt
                {
                    let last_cost = target.1 - must_fulfill_seg_cnt;
                    if last_cost < last_len && last_cost2coverage[last_cost] + gain == target.2 {
                        result_segments.push((
                            // We may not have enough space (view comments above) so the length
                            // of current segment may be less than `quota`.
                            //
                            // If `must_fulfill_seg_cnt > 0` (`gain > quota`),
                            // although one of flexible actors may not be a PERFECT actor,
                            // the length of current segment can
                            // still achieve `quota` by shifting among it and flexible actors.
                            //
                            // If `must_fulfill_seg_cnt == 0` (`gain <= quota`),
                            // there are no flexible actors to shift.
                            cur_processed - min(gain, quota),
                            cur_processed - 1,
                            *actor_id,
                        ));
                        // `target.0` is position, `target.1` is cost (usage of flexible
                        // actors) and `target.2` is coverage.
                        // Once we found, we update `target` and continue to seek backward
                        // recursively.
                        target = (position_delta, last_cost, last_cost2coverage[last_cost]);
                        break;
                    }
                }
            }
        }
    }
    // Since we sought backward, we need to reverse seek results.
    result_segments.reverse();

    result_segments
}
