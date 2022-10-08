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

use itertools::Itertools;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::types::{ParallelUnitId, VnodeMapping, VIRTUAL_NODE_COUNT};
use risingwave_common::util::compress::compress_data;
use risingwave_pb::common::{ParallelUnit, ParallelUnitMapping};
use risingwave_pb::stream_plan::ActorMapping;

use crate::model::{ActorId, FragmentId};

/// Build a vnode mapping according to parallel units where the fragment is scheduled.
/// For example, if `parallel_units` is `[0, 1, 2]`, and the total vnode count is 10, we'll
/// generate mapping like `[0, 0, 0, 0, 1, 1, 1, 2, 2, 2]`.
pub(crate) fn build_vnode_mapping(parallel_units: &[ParallelUnit]) -> VnodeMapping {
    let mut vnode_mapping = Vec::with_capacity(VIRTUAL_NODE_COUNT);

    let hash_shard_size = VIRTUAL_NODE_COUNT / parallel_units.len();
    let mut one_more_count = VIRTUAL_NODE_COUNT % parallel_units.len();
    let mut init_bound = 0;

    parallel_units.iter().for_each(|parallel_unit| {
        let vnode_count = if one_more_count > 0 {
            one_more_count -= 1;
            hash_shard_size + 1
        } else {
            hash_shard_size
        };
        let parallel_unit_id = parallel_unit.id;
        init_bound += vnode_count;
        vnode_mapping.resize(init_bound, parallel_unit_id);
    });

    vnode_mapping
}

pub(crate) fn vnode_mapping_to_bitmaps(
    vnode_mapping: VnodeMapping,
) -> HashMap<ParallelUnitId, Bitmap> {
    let mut vnode_bitmaps = HashMap::new();
    vnode_mapping
        .iter()
        .enumerate()
        .for_each(|(vnode, parallel_unit)| {
            vnode_bitmaps
                .entry(*parallel_unit)
                .or_insert_with(|| BitmapBuilder::zeroed(VIRTUAL_NODE_COUNT))
                .set(vnode, true);
        });
    vnode_bitmaps
        .into_iter()
        .map(|(u, b)| (u, b.finish()))
        .collect()
}

pub(crate) fn actor_mapping_from_bitmaps(bitmaps: &HashMap<ActorId, Bitmap>) -> ActorMapping {
    let mut raw = vec![0 as ActorId; VIRTUAL_NODE_COUNT];

    for (actor_id, bitmap) in bitmaps {
        for (idx, pos) in raw.iter_mut().enumerate() {
            if bitmap.is_set(idx) {
                *pos = *actor_id;
            }
        }
    }
    let (original_indices, data) = compress_data(&raw);

    ActorMapping {
        original_indices,
        data,
    }
}

pub(crate) fn parallel_unit_mapping_to_actor_mapping(
    parallel_unit_mapping: &ParallelUnitMapping,
    parallel_unit_to_actor_map: &HashMap<ParallelUnitId, ActorId>,
) -> ActorMapping {
    let ParallelUnitMapping {
        original_indices,
        data,
        ..
    } = parallel_unit_mapping;

    let actor_data = data
        .iter()
        .map(|parallel_unit_id| parallel_unit_to_actor_map[parallel_unit_id])
        .collect_vec();

    ActorMapping {
        original_indices: original_indices.clone(),
        data: actor_data,
    }
}

pub fn actor_mapping_to_parallel_unit_mapping(
    fragment_id: FragmentId,
    actor_to_parallel_unit_map: &HashMap<ActorId, ParallelUnitId>,
    actor_mapping: &ActorMapping,
) -> ParallelUnitMapping {
    let ActorMapping {
        original_indices,
        data,
    } = actor_mapping;

    let parallel_unit_data = data
        .iter()
        .map(|actor_id| actor_to_parallel_unit_map[actor_id])
        .collect_vec();

    ParallelUnitMapping {
        fragment_id,
        original_indices: original_indices.clone(),
        data: parallel_unit_data,
    }
}
