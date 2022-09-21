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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use itertools::Itertools;
    use maplit::btreeset;
    use risingwave_common::buffer::Bitmap;
    use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
    use risingwave_common::util::compress::decompress_data;
    use risingwave_pb::common::ParallelUnit;
    use risingwave_pb::stream_plan::{ActorMapping, StreamActor};

    use crate::model::ActorId;
    use crate::stream::mapping::{
        actor_mapping_from_bitmaps, build_vnode_mapping, vnode_mapping_to_bitmaps,
    };
    use crate::stream::scale::rebalance_actor_vnode;
    use crate::stream::{
        actor_mapping_to_parallel_unit_mapping, parallel_unit_mapping_to_actor_mapping,
    };

    fn simulated_parallel_unit_nums(min: Option<usize>, max: Option<usize>) -> Vec<usize> {
        let mut raw = vec![1, 3, 12, 42, VIRTUAL_NODE_COUNT];
        if let Some(min) = min {
            raw.drain_filter(|n| *n <= min);
            raw.push(min);
        }
        if let Some(max) = max {
            raw.drain_filter(|n| *n >= max);
            raw.push(max);
        }
        raw
    }

    fn build_fake_actors(info: &[(ActorId, ParallelUnitId)]) -> Vec<StreamActor> {
        let parallel_units = generate_parallel_units(info);

        let vnode_bitmaps = vnode_mapping_to_bitmaps(build_vnode_mapping(&parallel_units));

        info.iter()
            .map(|(actor_id, parallel_unit_id)| StreamActor {
                actor_id: *actor_id,
                vnode_bitmap: vnode_bitmaps
                    .get(parallel_unit_id)
                    .map(|bitmap| bitmap.to_protobuf()),
                ..Default::default()
            })
            .collect()
    }

    fn generate_parallel_units(info: &[(ActorId, ParallelUnitId)]) -> Vec<ParallelUnit> {
        info.iter()
            .map(|(_, parallel_unit_id)| ParallelUnit {
                id: *parallel_unit_id,
                ..Default::default()
            })
            .collect_vec()
    }

    fn check_affinity_for_scale_in(bitmap: &Bitmap, actor: &StreamActor) {
        let prev_bitmap = Bitmap::from(actor.vnode_bitmap.as_ref().unwrap());

        for idx in 0..VIRTUAL_NODE_COUNT {
            if prev_bitmap.is_set(idx).unwrap() {
                assert!(bitmap.is_set(idx).unwrap());
            }
        }
    }

    fn check_bitmaps<T>(bitmaps: &HashMap<T, Bitmap>) {
        let mut target = (0..VIRTUAL_NODE_COUNT).map(|_| false).collect_vec();

        for bitmap in bitmaps.values() {
            for (idx, pos) in target.iter_mut().enumerate() {
                if bitmap.is_set(idx).unwrap() {
                    // *pos should be false
                    assert!(!*pos);
                    *pos = true;
                }
            }
        }

        for (idx, b) in target.iter().enumerate() {
            assert!(*b, "vnode {} should be set", idx);
        }

        let vnodes = bitmaps.values().map(|bitmap| bitmap.num_high_bits());
        let (min, max) = vnodes.minmax().into_option().unwrap();

        assert!((max - min) <= 1, "min {} max {}", min, max);
    }

    #[test]
    fn test_build_vnode_mapping() {
        for parallel_units_num in simulated_parallel_unit_nums(None, None) {
            let info = (0..parallel_units_num)
                .map(|i| (i as ActorId, i as ParallelUnitId))
                .collect_vec();
            let parallel_units = generate_parallel_units(&info);
            let vnode_mapping = build_vnode_mapping(&parallel_units);

            assert_eq!(vnode_mapping.len(), VIRTUAL_NODE_COUNT);

            let mut check = HashMap::new();
            for (idx, parallel_unit_id) in vnode_mapping.into_iter().enumerate() {
                check.entry(parallel_unit_id).or_insert(vec![]).push(idx);
            }

            assert_eq!(check.len(), parallel_units_num);

            let (min, max) = check
                .values()
                .map(|indexes| indexes.len())
                .minmax()
                .into_option()
                .unwrap();

            assert!(max - min <= 1);
        }
    }

    #[test]
    fn test_vnode_mapping_to_bitmaps() {
        for parallel_units_num in simulated_parallel_unit_nums(None, None) {
            let info = (0..parallel_units_num)
                .map(|i| (i as ActorId, i as ParallelUnitId))
                .collect_vec();
            let parallel_units = generate_parallel_units(&info);
            let bitmaps = vnode_mapping_to_bitmaps(build_vnode_mapping(&parallel_units));
            check_bitmaps(&bitmaps);
        }
    }

    #[test]
    fn test_mapping_convert() {
        for parallel_unit_num in simulated_parallel_unit_nums(None, None) {
            let (actor_mapping, _) = generate_actor_mapping(parallel_unit_num);

            let actor_to_parallel_unit_map = (0..parallel_unit_num)
                .map(|i| (i as ActorId, i as ParallelUnitId))
                .collect();
            let parallel_unit_mapping = actor_mapping_to_parallel_unit_mapping(
                1,
                &actor_to_parallel_unit_map,
                &actor_mapping,
            );

            let parallel_unit_to_actor_map: HashMap<_, _> = actor_to_parallel_unit_map
                .into_iter()
                .map(|(k, v)| (v, k))
                .collect();

            let new_actor_mapping = parallel_unit_mapping_to_actor_mapping(
                &parallel_unit_mapping,
                &parallel_unit_to_actor_map,
            );

            assert!(actor_mapping.eq(&new_actor_mapping))
        }
    }

    fn generate_actor_mapping(
        parallel_unit_num: usize,
    ) -> (ActorMapping, HashMap<ActorId, Bitmap>) {
        let parallel_units = (0..parallel_unit_num)
            .map(|i| (i as ActorId, i as ParallelUnitId))
            .collect_vec();
        let actors = build_fake_actors(&parallel_units);

        let bitmaps: HashMap<_, _> = actors
            .into_iter()
            .map(|actor| {
                (
                    actor.actor_id as ActorId,
                    Bitmap::from(actor.vnode_bitmap.as_ref().unwrap()),
                )
            })
            .collect();

        (actor_mapping_from_bitmaps(&bitmaps), bitmaps)
    }

    #[test]
    fn test_actor_mapping_from_bitmaps() {
        for parallel_unit_num in simulated_parallel_unit_nums(None, None) {
            let (actor_mapping, bitmaps) = generate_actor_mapping(parallel_unit_num);
            check_bitmaps(&bitmaps);

            let ActorMapping {
                original_indices,
                data,
            } = actor_mapping;

            let raw = decompress_data(&original_indices, &data);

            for (actor_id, bitmap) in &bitmaps {
                for (idx, value) in raw.iter().enumerate() {
                    if bitmap.is_set(idx).unwrap() {
                        assert_eq!(*value, *actor_id);
                    }
                }
            }
        }
    }

    #[test]
    fn test_rebalance_empty() {
        let actors = build_fake_actors(&(0..3).map(|i| (i, i)).collect_vec());

        // empty input
        let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(result.len(), actors.len());
    }

    #[test]
    fn test_rebalance_scale_in() {
        for parallel_unit_num in simulated_parallel_unit_nums(Some(3), None) {
            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );

            // remove 1
            let actors_to_remove = btreeset! {0};
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &BTreeSet::new());
            assert_eq!(result.len(), actors.len() - actors_to_remove.len());
            check_bitmaps(&result);
            check_affinity_for_scale_in(result.get(&(1 as ActorId)).unwrap(), &actors[1]);

            // remove n-1
            let actors_to_remove = (1..parallel_unit_num as ActorId).collect();
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &BTreeSet::new());
            assert_eq!(result.len(), 1);
            check_bitmaps(&result);

            let (_, bitmap) = result.iter().exactly_one().unwrap();
            assert!(bitmap.is_all_set());
        }
    }

    #[test]
    fn test_rebalance_scale_out() {
        for parallel_unit_num in simulated_parallel_unit_nums(Some(3), Some(VIRTUAL_NODE_COUNT - 1))
        {
            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );

            // add 1
            let actors_to_add = btreeset! {parallel_unit_num as ActorId};
            let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &actors_to_add);
            assert_eq!(result.len(), actors.len() + actors_to_add.len());
            check_bitmaps(&result);

            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );
            // add to VIRTUAL_NODE_COUNT
            let actors_to_add =
                (parallel_unit_num as ActorId..VIRTUAL_NODE_COUNT as ActorId).collect();
            let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &actors_to_add);
            assert_eq!(result.len(), actors.len() + actors_to_add.len());
            check_bitmaps(&result);
        }
    }

    #[test]
    fn test_rebalance_migration() {
        for parallel_unit_num in simulated_parallel_unit_nums(Some(3), None) {
            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );

            for idx in 0..parallel_unit_num {
                let actors_to_remove = btreeset! {idx as ActorId};
                let actors_to_add = btreeset! {parallel_unit_num as ActorId};
                let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

                assert_eq!(
                    result.len(),
                    actors.len() - actors_to_remove.len() + actors_to_add.len()
                );

                check_bitmaps(&result);

                for actor in &actors {
                    if actor.actor_id == idx as ActorId {
                        continue;
                    }

                    let target_bitmap = result.get(&actor.actor_id).unwrap();
                    let prev_bitmap = Bitmap::from(actor.vnode_bitmap.as_ref().unwrap());
                    assert!(prev_bitmap.eq(target_bitmap));
                }
            }

            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );

            for migration_count in 1..parallel_unit_num {
                let actors_to_remove = (0..migration_count as ActorId).collect();
                let actors_to_add = (parallel_unit_num as ActorId
                    ..(parallel_unit_num + migration_count) as ActorId)
                    .collect();
                let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

                assert_eq!(
                    result.len(),
                    actors.len() - actors_to_remove.len() + actors_to_add.len()
                );

                check_bitmaps(&result);
            }
        }
    }

    #[test]
    fn test_rebalance_scale() {
        for parallel_unit_num in simulated_parallel_unit_nums(Some(3), None) {
            let actors = build_fake_actors(
                &(0..parallel_unit_num)
                    .map(|i| (i as ActorId, i as ParallelUnitId))
                    .collect_vec(),
            );

            let parallel_unit_num = parallel_unit_num as ActorId;
            let actors_to_remove = btreeset! {0};
            let actors_to_add = btreeset! {parallel_unit_num, parallel_unit_num+1};
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

            assert_eq!(
                result.len(),
                actors.len() - actors_to_remove.len() + actors_to_add.len()
            );
            check_bitmaps(&result);

            let actors_to_remove = btreeset! {0, 1};
            let actors_to_add = btreeset! {parallel_unit_num};
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

            assert_eq!(
                result.len(),
                actors.len() - actors_to_remove.len() + actors_to_add.len()
            );
            check_bitmaps(&result);

            check_affinity_for_scale_in(result.get(&(2 as ActorId)).unwrap(), &actors[2]);
        }
    }

    #[test]
    fn test_rebalance_scale_real() {
        let parallel_units = (0..(VIRTUAL_NODE_COUNT - 1) as ActorId)
            .map(|i| (i, i))
            .collect_vec();
        let actors = build_fake_actors(&parallel_units);

        let actors_to_remove = btreeset! {0, 1};
        let actors_to_add = btreeset! {255};
        let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

        check_bitmaps(&result);
    }
}
