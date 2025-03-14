// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet, LinkedList, VecDeque};
use std::ops::BitOrAssign;

use itertools::Itertools;
use num_integer::Integer;
use risingwave_common::hash::WorkerSlotId;
use risingwave_pb::common::WorkerNode;

use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::hash::{VirtualNode, WorkerSlotMapping};

/// Calculate a new vnode mapping, keeping locality and balance on a best effort basis.
/// The strategy is similar to `rebalance_actor_vnode` used in meta node, but is modified to
/// consider `max_parallelism` too.
pub fn place_vnode(
    hint_worker_slot_mapping: Option<&WorkerSlotMapping>,
    workers: &[WorkerNode],
    max_parallelism: Option<usize>,
    vnode_count: usize,
) -> Option<WorkerSlotMapping> {
    if let Some(mapping) = hint_worker_slot_mapping {
        assert_eq!(mapping.len(), vnode_count);
    }

    // Get all serving worker slots from all available workers, grouped by worker id and ordered
    // by worker slot id in each group.
    let mut worker_slots: LinkedList<_> = workers
        .iter()
        .filter(|w| w.property.as_ref().is_some_and(|p| p.is_serving))
        .sorted_by_key(|w| w.id)
        .map(|w| (0..w.compute_node_parallelism()).map(|idx| WorkerSlotId::new(w.id, idx)))
        .collect();

    // Set serving parallelism to the minimum of total number of worker slots, specified
    // `max_parallelism` and total number of virtual nodes.
    let serving_parallelism = std::cmp::min(
        worker_slots.iter().map(|slots| slots.len()).sum(),
        std::cmp::min(max_parallelism.unwrap_or(usize::MAX), vnode_count),
    );

    // Select `serving_parallelism` worker slots in a round-robin fashion, to distribute workload
    // evenly among workers.
    let mut selected_slots = Vec::new();
    while !worker_slots.is_empty() {
        worker_slots
            .extract_if(|slots| {
                if let Some(slot) = slots.next() {
                    selected_slots.push(slot);
                    false
                } else {
                    true
                }
            })
            .for_each(drop);
    }
    selected_slots.drain(serving_parallelism..);
    let selected_slots_set: HashSet<WorkerSlotId> = selected_slots.iter().cloned().collect();
    if selected_slots_set.is_empty() {
        return None;
    }

    // Calculate balance for each selected worker slot. Initially, each worker slot is assigned
    // no vnodes. Thus its negative balance means that many vnodes should be assigned to it later.
    // `is_temp` is a mark for a special temporary worker slot, only to simplify implementation.
    #[derive(Debug)]
    struct Balance {
        slot: WorkerSlotId,
        balance: i32,
        builder: BitmapBuilder,
        is_temp: bool,
    }

    let (expected, mut remain) = vnode_count.div_rem(&selected_slots.len());
    let mut balances: HashMap<WorkerSlotId, Balance> = HashMap::default();

    for slot in &selected_slots {
        let mut balance = Balance {
            slot: *slot,
            balance: -(expected as i32),
            builder: BitmapBuilder::zeroed(vnode_count),
            is_temp: false,
        };

        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
        balances.insert(*slot, balance);
    }

    // Now to maintain affinity, if a hint has been provided via `hint_worker_slot_mapping`, follow
    // that mapping to adjust balances.
    let mut temp_slot = Balance {
        slot: WorkerSlotId::new(0u32, usize::MAX), /* This id doesn't matter for `temp_slot`. It's distinguishable via `is_temp`. */
        balance: 0,
        builder: BitmapBuilder::zeroed(vnode_count),
        is_temp: true,
    };
    match hint_worker_slot_mapping {
        Some(hint_worker_slot_mapping) => {
            for (vnode, worker_slot) in hint_worker_slot_mapping.iter_with_vnode() {
                let b = if selected_slots_set.contains(&worker_slot) {
                    // Assign vnode to the same worker slot as hint.
                    balances.get_mut(&worker_slot).unwrap()
                } else {
                    // Assign vnode that doesn't belong to any worker slot to `temp_slot`
                    // temporarily. They will be reassigned later.
                    &mut temp_slot
                };

                b.balance += 1;
                b.builder.set(vnode.to_index(), true);
            }
        }
        None => {
            // No hint is provided, assign all vnodes to `temp_pu`.
            for vnode in VirtualNode::all(vnode_count) {
                temp_slot.balance += 1;
                temp_slot.builder.set(vnode.to_index(), true);
            }
        }
    }

    // The final step is to move vnodes from worker slots with positive balance to worker slots
    // with negative balance, until all worker slots are of 0 balance.
    // A double-ended queue with worker slots ordered by balance in descending order is consumed:
    // 1. Peek 2 worker slots from front and back.
    // 2. It any of them is of 0 balance, pop it and go to step 1.
    // 3. Otherwise, move vnodes from front to back.
    let mut balances: VecDeque<_> = balances
        .into_values()
        .chain(std::iter::once(temp_slot))
        .sorted_by_key(|b| b.balance)
        .rev()
        .collect();

    let mut results: HashMap<WorkerSlotId, Bitmap> = HashMap::default();

    while !balances.is_empty() {
        if balances.len() == 1 {
            let single = balances.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !single.is_temp {
                results.insert(single.slot, single.builder.finish());
            }
            break;
        }
        let mut src = balances.pop_front().unwrap();
        let mut dst = balances.pop_back().unwrap();
        let n = std::cmp::min(src.balance.abs(), dst.balance.abs());
        let mut moved = 0;
        for idx in 0..vnode_count {
            if moved >= n {
                break;
            }
            if src.builder.is_set(idx) {
                src.builder.set(idx, false);
                assert!(!dst.builder.is_set(idx));
                dst.builder.set(idx, true);
                moved += 1;
            }
        }
        src.balance -= n;
        dst.balance += n;
        if src.balance != 0 {
            balances.push_front(src);
        } else if !src.is_temp {
            results.insert(src.slot, src.builder.finish());
        }

        if dst.balance != 0 {
            balances.push_back(dst);
        } else if !dst.is_temp {
            results.insert(dst.slot, dst.builder.finish());
        }
    }

    let mut worker_result = HashMap::new();

    for (worker_slot, bitmap) in results {
        worker_result
            .entry(worker_slot)
            .or_insert(Bitmap::zeros(vnode_count))
            .bitor_assign(&bitmap);
    }

    Some(WorkerSlotMapping::from_bitmaps(&worker_result))
}

#[cfg(test)]
mod tests {

    use risingwave_common::hash::WorkerSlotMapping;
    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{WorkerNode, WorkerType};

    use crate::hash::VirtualNode;

    /// [`super::place_vnode`] with [`VirtualNode::COUNT_FOR_TEST`] as the vnode count.
    fn place_vnode(
        hint_worker_slot_mapping: Option<&WorkerSlotMapping>,
        workers: &[WorkerNode],
        max_parallelism: Option<usize>,
    ) -> Option<WorkerSlotMapping> {
        super::place_vnode(
            hint_worker_slot_mapping,
            workers,
            max_parallelism,
            VirtualNode::COUNT_FOR_TEST,
        )
    }

    #[test]
    fn test_place_vnode() {
        assert_eq!(VirtualNode::COUNT_FOR_TEST, 256);

        let serving_property = Property {
            is_unschedulable: false,
            is_serving: true,
            is_streaming: false,
            ..Default::default()
        };

        let count_same_vnode_mapping = |wm1: &WorkerSlotMapping, wm2: &WorkerSlotMapping| {
            assert_eq!(wm1.len(), 256);
            assert_eq!(wm2.len(), 256);
            let mut count: usize = 0;
            for idx in 0..VirtualNode::COUNT_FOR_TEST {
                let vnode = VirtualNode::from_index(idx);
                if wm1.get(vnode) == wm2.get(vnode) {
                    count += 1;
                }
            }
            count
        };

        let mut property = serving_property.clone();
        property.parallelism = 1;
        let worker_1 = WorkerNode {
            id: 1,
            r#type: WorkerType::ComputeNode.into(),
            property: Some(property),
            ..Default::default()
        };

        assert!(
            place_vnode(None, &[worker_1.clone()], Some(0)).is_none(),
            "max_parallelism should >= 0"
        );

        let re_worker_mapping_2 = place_vnode(None, &[worker_1.clone()], None).unwrap();
        assert_eq!(re_worker_mapping_2.iter_unique().count(), 1);

        let mut property = serving_property.clone();
        property.parallelism = 50;
        let worker_2 = WorkerNode {
            id: 2,
            property: Some(property),
            r#type: WorkerType::ComputeNode.into(),
            ..Default::default()
        };

        let re_worker_mapping = place_vnode(
            Some(&re_worker_mapping_2),
            &[worker_1.clone(), worker_2.clone()],
            None,
        )
        .unwrap();

        assert_eq!(re_worker_mapping.iter_unique().count(), 51);
        // 1 * 256 + 0 -> 51 * 5 + 1
        let score = count_same_vnode_mapping(&re_worker_mapping_2, &re_worker_mapping);
        assert!(score >= 5);

        let mut property = serving_property.clone();
        property.parallelism = 60;
        let worker_3 = WorkerNode {
            id: 3,
            r#type: WorkerType::ComputeNode.into(),
            property: Some(property),
            ..Default::default()
        };
        let re_pu_mapping_2 = place_vnode(
            Some(&re_worker_mapping),
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            None,
        )
        .unwrap();

        // limited by total pu number
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // 51 * 5 + 1 -> 111 * 2 + 34
        let score = count_same_vnode_mapping(&re_pu_mapping_2, &re_worker_mapping);
        assert!(score >= (2 + 50 * 2));
        let re_pu_mapping = place_vnode(
            Some(&re_pu_mapping_2),
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            Some(50),
        )
        .unwrap();
        // limited by max_parallelism
        assert_eq!(re_pu_mapping.iter_unique().count(), 50);
        // 111 * 2 + 34 -> 50 * 5 + 6
        let score = count_same_vnode_mapping(&re_pu_mapping, &re_pu_mapping_2);
        assert!(score >= 50 * 2);
        let re_pu_mapping_2 = place_vnode(
            Some(&re_pu_mapping),
            &[worker_1.clone(), worker_2, worker_3.clone()],
            None,
        )
        .unwrap();
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // 50 * 5 + 6 -> 111 * 2 + 34
        let score = count_same_vnode_mapping(&re_pu_mapping_2, &re_pu_mapping);
        assert!(score >= 50 * 2);
        let re_pu_mapping =
            place_vnode(Some(&re_pu_mapping_2), &[worker_1, worker_3.clone()], None).unwrap();
        // limited by total pu number
        assert_eq!(re_pu_mapping.iter_unique().count(), 61);
        // 111 * 2 + 34 -> 61 * 4 + 12
        let score = count_same_vnode_mapping(&re_pu_mapping, &re_pu_mapping_2);
        assert!(score >= 61 * 2);
        assert!(place_vnode(Some(&re_pu_mapping), &[], None).is_none());
        let re_pu_mapping = place_vnode(Some(&re_pu_mapping), &[worker_3], None).unwrap();
        assert_eq!(re_pu_mapping.iter_unique().count(), 60);
        assert!(place_vnode(Some(&re_pu_mapping), &[], None).is_none());
    }
}
