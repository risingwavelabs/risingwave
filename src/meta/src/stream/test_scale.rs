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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use itertools::Itertools;
    use maplit::btreeset;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::hash::{ActorMapping, VirtualNode};

    use crate::model::ActorId;
    use crate::stream::CustomActorInfo;
    use crate::stream::scale::rebalance_actor_vnode;

    fn simulated_parallelism(min: Option<usize>, max: Option<usize>) -> Vec<usize> {
        let mut raw = vec![1, 3, 12, 42, VirtualNode::COUNT_FOR_TEST];
        if let Some(min) = min {
            raw.retain(|n| *n > min);
            raw.push(min);
        }
        if let Some(max) = max {
            raw.retain(|n| *n < max);
            raw.push(max);
        }
        raw
    }

    fn build_fake_actors(actor_ids: Vec<ActorId>) -> Vec<CustomActorInfo> {
        let actor_bitmaps =
            ActorMapping::new_uniform(actor_ids.clone().into_iter(), VirtualNode::COUNT_FOR_TEST)
                .to_bitmaps();
        actor_ids
            .iter()
            .map(|actor_id| CustomActorInfo {
                actor_id: *actor_id,
                vnode_bitmap: actor_bitmaps.get(actor_id).cloned(),
                ..Default::default()
            })
            .collect()
    }

    fn check_affinity_for_scale_in(bitmap: &Bitmap, actor: &CustomActorInfo) {
        let prev_bitmap = actor.vnode_bitmap.as_ref().unwrap();

        for idx in 0..VirtualNode::COUNT_FOR_TEST {
            if prev_bitmap.is_set(idx) {
                assert!(bitmap.is_set(idx));
            }
        }
    }

    fn check_bitmaps<T>(bitmaps: &HashMap<T, Bitmap>) {
        let mut target = (0..VirtualNode::COUNT_FOR_TEST)
            .map(|_| false)
            .collect_vec();

        for bitmap in bitmaps.values() {
            for (idx, pos) in target.iter_mut().enumerate() {
                if bitmap.is_set(idx) {
                    // *pos should be false
                    assert!(!*pos);
                    *pos = true;
                }
            }
        }

        for (idx, b) in target.iter().enumerate() {
            assert!(*b, "vnode {} should be set", idx);
        }

        let vnodes = bitmaps.values().map(|bitmap| bitmap.count_ones());
        let (min, max) = vnodes.minmax().into_option().unwrap();

        assert!((max - min) <= 1, "min {} max {}", min, max);
    }

    #[test]
    fn test_build_actor_mapping() {
        for parallelism in simulated_parallelism(None, None) {
            let actor_ids = (0..parallelism as ActorId).collect_vec();
            let actor_mapping =
                ActorMapping::new_uniform(actor_ids.into_iter(), VirtualNode::COUNT_FOR_TEST);

            assert_eq!(actor_mapping.len(), VirtualNode::COUNT_FOR_TEST);

            let mut check: HashMap<u32, Vec<_>> = HashMap::new();
            for (vnode, actor_id) in actor_mapping.iter_with_vnode() {
                check.entry(actor_id).or_default().push(vnode);
            }

            assert_eq!(check.len(), parallelism);

            let (min, max) = check
                .values()
                .map(|indexes| indexes.len())
                .minmax()
                .into_option()
                .unwrap();

            assert!(max - min <= 1);
        }
    }

    fn generate_actor_mapping(parallelism: usize) -> (ActorMapping, HashMap<ActorId, Bitmap>) {
        let actor_ids = (0..parallelism).map(|i| i as ActorId).collect_vec();
        let actors = build_fake_actors(actor_ids);

        let bitmaps: HashMap<_, _> = actors
            .into_iter()
            .map(|actor| {
                (
                    actor.actor_id as ActorId,
                    actor.vnode_bitmap.unwrap().clone(),
                )
            })
            .collect();

        (ActorMapping::from_bitmaps(&bitmaps), bitmaps)
    }

    #[test]
    fn test_actor_mapping_from_bitmaps() {
        for parallelism in simulated_parallelism(None, None) {
            let (actor_mapping, bitmaps) = generate_actor_mapping(parallelism);
            check_bitmaps(&bitmaps);

            for (actor_id, bitmap) in &bitmaps {
                for (vnode, value) in actor_mapping.iter_with_vnode() {
                    if bitmap.is_set(vnode.to_index()) {
                        assert_eq!(value, *actor_id);
                    }
                }
            }
        }
    }

    #[test]
    fn test_rebalance_empty() {
        let actors = build_fake_actors((0..3 as ActorId).collect_vec());

        // empty input
        let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &BTreeSet::new());
        assert_eq!(result.len(), actors.len());
    }

    #[test]
    fn test_rebalance_scale_in() {
        for parallelism in simulated_parallelism(Some(3), None) {
            let actors = build_fake_actors((0..parallelism as ActorId).collect_vec());

            // remove 1
            let actors_to_remove = btreeset! {0};
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &BTreeSet::new());
            assert_eq!(result.len(), actors.len() - actors_to_remove.len());
            check_bitmaps(&result);
            check_affinity_for_scale_in(result.get(&(1 as ActorId)).unwrap(), &actors[1]);

            // remove n-1
            let actors_to_remove = (1..parallelism as ActorId).collect();
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &BTreeSet::new());
            assert_eq!(result.len(), 1);
            check_bitmaps(&result);

            let (_, bitmap) = result.iter().exactly_one().unwrap();
            assert!(bitmap.all());
        }
    }

    #[test]
    fn test_rebalance_scale_out() {
        for parallelism in simulated_parallelism(Some(3), Some(VirtualNode::COUNT_FOR_TEST - 1)) {
            let actors = build_fake_actors((0..parallelism as ActorId).collect_vec());

            // add 1
            let actors_to_add = btreeset! {parallelism as ActorId};
            let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &actors_to_add);
            assert_eq!(result.len(), actors.len() + actors_to_add.len());
            check_bitmaps(&result);

            let actors = build_fake_actors((0..parallelism as ActorId).collect_vec());

            // add to VirtualNode::COUNT_FOR_TEST
            let actors_to_add =
                (parallelism as ActorId..VirtualNode::COUNT_FOR_TEST as ActorId).collect();
            let result = rebalance_actor_vnode(&actors, &BTreeSet::new(), &actors_to_add);
            assert_eq!(result.len(), actors.len() + actors_to_add.len());
            check_bitmaps(&result);
        }
    }

    #[test]
    fn test_rebalance_migration() {
        for parallelism in simulated_parallelism(Some(3), None) {
            let actors = build_fake_actors((0..parallelism as ActorId).collect_vec());

            for idx in 0..parallelism {
                let actors_to_remove = btreeset! {idx as ActorId};
                let actors_to_add = btreeset! {parallelism as ActorId};
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
                    let prev_bitmap = actor.vnode_bitmap.as_ref().unwrap();
                    assert!(prev_bitmap.eq(target_bitmap));
                }
            }
            let actors = build_fake_actors((0..parallelism as ActorId).collect_vec());

            for migration_count in 1..parallelism {
                let actors_to_remove = (0..migration_count as ActorId).collect();
                let actors_to_add =
                    (parallelism as ActorId..(parallelism + migration_count) as ActorId).collect();
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
        for parallelism in simulated_parallelism(Some(3), None) {
            let actor_ids = (0..parallelism as ActorId).collect_vec();
            let actors = build_fake_actors(actor_ids);

            let parallelism = parallelism as ActorId;
            let actors_to_remove = btreeset! {0};
            let actors_to_add = btreeset! {parallelism, parallelism+1};
            let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

            assert_eq!(
                result.len(),
                actors.len() - actors_to_remove.len() + actors_to_add.len()
            );
            check_bitmaps(&result);

            let actors_to_remove = btreeset! {0, 1};
            let actors_to_add = btreeset! {parallelism};
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
        let actor_ids = (0..(VirtualNode::COUNT_FOR_TEST - 1) as ActorId).collect_vec();
        let actors = build_fake_actors(actor_ids);
        let actors_to_remove = btreeset! {0, 1};
        let actors_to_add = btreeset! {255};
        let result = rebalance_actor_vnode(&actors, &actors_to_remove, &actors_to_add);

        check_bitmaps(&result);
    }
}
