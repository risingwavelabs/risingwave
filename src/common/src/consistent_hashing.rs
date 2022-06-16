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
    let mut one_more_count = VIRTUAL_NODE_COUNT % parallel_units.len();
    let mut init_bound = 0;

    parallel_units.iter().for_each(|&parallel_unit_id| {
        let vnode_count = if one_more_count > 0 {
            one_more_count -= 1;
            hash_shard_size + 1
        } else {
            hash_shard_size
        };
        init_bound += vnode_count;
        vnode_mapping.resize(init_bound, parallel_unit_id);
        let vnode_range = init_bound - vnode_count..init_bound;
        let vnodes = vnode_range.clone().map(|id| id as VirtualNode).collect();
        owner_mapping.insert(parallel_unit_id, vnodes);
        let ranges = owner_mapping_ranges.entry(parallel_unit_id).or_default();
        ranges.starts.push(vnode_range.start as u64);
        ranges.ends.push((vnode_range.end - 1) as u64);

        init_bound += hash_shard_size;
    });

    (vnode_mapping, owner_mapping, owner_mapping_ranges)
}
