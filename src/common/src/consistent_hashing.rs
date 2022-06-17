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

use crate::types::{ParallelUnitId, VirtualNode, VIRTUAL_NODE_COUNT};

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
) {
    let mut vnode_mapping = Vec::with_capacity(VIRTUAL_NODE_COUNT);
    let mut owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();

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

        init_bound += hash_shard_size;
    });

    (vnode_mapping, owner_mapping)
}

pub fn vnode_mapping_to_ranges(
    vnode_mapping: &[ParallelUnitId],
) -> HashMap<ParallelUnitId, VNodeRanges> {
    let mut ranges_mapping: HashMap<ParallelUnitId, VNodeRanges> = HashMap::new();

    let mut start: usize = 0;

    for i in 1..vnode_mapping.len() {
        if vnode_mapping[i - 1] != vnode_mapping[i] {
            let ranges = ranges_mapping.entry(vnode_mapping[i - 1]).or_default();
            ranges.starts.push(start as u64);
            ranges.ends.push(i as u64 - 1);
            start = i;
        }
    }

    if let Some(&last) = vnode_mapping.last() {
        let ranges = ranges_mapping.entry(last).or_default();
        ranges.starts.push(start as u64);
        ranges.ends.push(vnode_mapping.len() as u64 - 1);
    }

    ranges_mapping
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_ranges() {
        // Simple
        {
            let vnode_mapping = [3, 3, 3, 3, 3, 4, 4, 5, 5, 6, 7, 8, 8, 8, 9];
            let ranges_mapping = vnode_mapping_to_ranges(&vnode_mapping);
            assert_eq!(ranges_mapping.len(), 7);
            assert_eq!(ranges_mapping[&3].starts, vec![0]);
            assert_eq!(ranges_mapping[&3].ends, vec![4]);
            assert_eq!(ranges_mapping[&4].starts, vec![5]);
            assert_eq!(ranges_mapping[&4].ends, vec![6]);
            assert_eq!(ranges_mapping[&5].starts, vec![7]);
            assert_eq!(ranges_mapping[&5].ends, vec![8]);
            assert_eq!(ranges_mapping[&6].starts, vec![9]);
            assert_eq!(ranges_mapping[&6].ends, vec![9]);
            assert_eq!(ranges_mapping[&7].starts, vec![10]);
            assert_eq!(ranges_mapping[&7].ends, vec![10]);
            assert_eq!(ranges_mapping[&8].starts, vec![11]);
            assert_eq!(ranges_mapping[&8].ends, vec![13]);
            assert_eq!(ranges_mapping[&9].starts, vec![14]);
            assert_eq!(ranges_mapping[&9].ends, vec![14]);
        }

        // Complex
        {
            let mut vnode_mapping = Vec::new();
            vnode_mapping.resize(512, 1);
            vnode_mapping.resize(1024, 2);
            vnode_mapping.resize(1536, 3);
            vnode_mapping.resize(2048, 4);
            vnode_mapping[0] = 5;
            vnode_mapping[2046] = 5;
            let ranges_mapping = vnode_mapping_to_ranges(&vnode_mapping);
            assert_eq!(ranges_mapping.len(), 5);
            assert_eq!(ranges_mapping[&1].starts, vec![1]);
            assert_eq!(ranges_mapping[&1].ends, vec![511]);
            assert_eq!(ranges_mapping[&2].starts, vec![512]);
            assert_eq!(ranges_mapping[&2].ends, vec![1023]);
            assert_eq!(ranges_mapping[&3].starts, vec![1024]);
            assert_eq!(ranges_mapping[&3].ends, vec![1535]);
            assert_eq!(ranges_mapping[&4].starts, vec![1536, 2047]);
            assert_eq!(ranges_mapping[&4].ends, vec![2045, 2047]);
            assert_eq!(ranges_mapping[&5].starts, vec![0, 2046]);
            assert_eq!(ranges_mapping[&5].ends, vec![0, 2046]);
        }

        // Empty
        {
            let ranges_mapping = vnode_mapping_to_ranges(&[]);
            assert_eq!(ranges_mapping.len(), 0);
        }
    }
}
