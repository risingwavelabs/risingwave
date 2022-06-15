// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::collections::HashMap;

use risingwave_pb::common::VNodeRanges;

use crate::hash::{VirtualNode, VIRTUAL_NODE_COUNT};

pub type ParallelUnitId = u32;

pub fn full_vnode_range() -> VNodeRanges {
    VNodeRanges {
        starts: vec![0],
        ends: vec![VIRTUAL_NODE_COUNT as u64 - 1],
    }
}

#[expect(clippy::type_complexity)]
pub fn build_vnode_mapping(
    parallel_units: &[u32],
) -> (
    Vec<ParallelUnitId>,
    HashMap<ParallelUnitId, Vec<VirtualNode>>,
    HashMap<ParallelUnitId, VNodeRanges>,
) {
    let mut vnode_mapping = Vec::with_capacity(VIRTUAL_NODE_COUNT);
    let mut owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();
    let mut owner_mapping_ranges: HashMap<ParallelUnitId, VNodeRanges> = HashMap::new();

    let hash_shard_size = VIRTUAL_NODE_COUNT / parallel_units.len();

    for (i, &parallel_unit_id) in parallel_units.iter().enumerate() {
        vnode_mapping.extend(vec![parallel_unit_id; hash_shard_size]);
        let vnodes = (i * hash_shard_size..(i + 1) * hash_shard_size)
            .map(|id| id as VirtualNode)
            .collect();
        owner_mapping.insert(parallel_unit_id, vnodes);
        let ranges = owner_mapping_ranges.entry(parallel_unit_id).or_default();
        ranges.starts.push((i * hash_shard_size) as u64);
        ranges.ends.push(((i + 1) * hash_shard_size - 1) as u64);
    }

    let mut parallel_unit_iter = parallel_units.iter().cycle();
    for vnode in
        (VIRTUAL_NODE_COUNT - VIRTUAL_NODE_COUNT % parallel_units.len())..VIRTUAL_NODE_COUNT
    {
        let id = *parallel_unit_iter.next().unwrap();
        vnode_mapping.push(id);
        owner_mapping
            .entry(id)
            .or_default()
            .push(vnode as VirtualNode);
        let ranges = owner_mapping_ranges.entry(id).or_default();
        ranges.starts.push(vnode as u64);
        ranges.ends.push(vnode as u64);
    }

    (vnode_mapping, owner_mapping, owner_mapping_ranges)
}
