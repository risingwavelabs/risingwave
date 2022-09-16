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

use std::collections::{BTreeMap, HashMap, LinkedList};
use std::iter::empty;

use anyhow::{anyhow, Context};
use itertools::Itertools;
use rand::prelude::SliceRandom;
use risingwave_common::bail;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::types::{ParallelUnitId, VnodeMapping, VIRTUAL_NODE_COUNT};
use risingwave_common::util::compress::compress_data;
use risingwave_pb::common::{ActorInfo, ParallelUnit, ParallelUnitMapping, WorkerNode};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::Fragment;

use crate::manager::{WorkerId, WorkerLocations};
use crate::model::ActorId;
use crate::MetaResult;

/// [`Scheduler`] defines schedule logic for mv actors.
pub struct Scheduler {
    /// The parallel units of the cluster in a round-robin manner on each worker.
    all_parallel_units: Vec<ParallelUnit>,
}

/// [`ScheduledLocations`] represents the location of scheduled result.
pub struct ScheduledLocations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, ParallelUnit>,
    /// worker location map.
    pub worker_locations: WorkerLocations,
}

impl ScheduledLocations {
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn new() -> Self {
        Self::with_workers(empty())
    }

    pub fn with_workers(workers: impl IntoIterator<Item = WorkerNode>) -> Self {
        Self {
            actor_locations: Default::default(),
            worker_locations: workers.into_iter().map(|w| (w.id, w)).collect(),
        }
    }

    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        let mut worker_actors = HashMap::new();
        self.actor_locations
            .iter()
            .for_each(|(actor_id, parallel_unit)| {
                worker_actors
                    .entry(parallel_unit.worker_node_id)
                    .or_insert_with(Vec::new)
                    .push(*actor_id);
            });

        worker_actors
    }

    /// Returns the `ActorInfo` map for every actor.
    pub fn actor_info_map(&self) -> HashMap<ActorId, ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| {
                (
                    *actor_id,
                    ActorInfo {
                        actor_id: *actor_id,
                        host: self.worker_locations[&parallel_unit.worker_node_id]
                            .host
                            .clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>()
    }

    /// Returns an iterator of `ActorInfo`.
    pub fn actor_infos(&self) -> impl Iterator<Item = ActorInfo> + '_ {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| ActorInfo {
                actor_id: *actor_id,
                host: self.worker_locations[&parallel_unit.worker_node_id]
                    .host
                    .clone(),
            })
    }

    /// Find a placement location that is on the same worker node of given actor ids.
    pub fn schedule_colocate_with(&self, actor_ids: &[ActorId]) -> MetaResult<ParallelUnit> {
        let mut result_location = None;
        for actor_id in actor_ids {
            let location = self
                .actor_locations
                .get(actor_id)
                .ok_or_else(|| anyhow!("actor location not found: {}", actor_id))?;
            match &result_location {
                None => result_location = Some(location.clone()),
                Some(result_location) if result_location != location => {
                    bail!(
                        "cannot satisfy placement rule: {} is at {:?}, while others are on {:?}",
                        actor_id,
                        location,
                        result_location
                    );
                }
                _ => {}
            }
        }
        Ok(result_location.unwrap())
    }
}

impl Scheduler {
    pub fn new(parallel_units: impl IntoIterator<Item = ParallelUnit>) -> Self {
        // Group parallel units with worker node.
        let mut parallel_units_map = BTreeMap::new();
        for p in parallel_units {
            parallel_units_map
                .entry(p.worker_node_id)
                .or_insert_with(Vec::new)
                .push(p);
        }
        let mut parallel_units: LinkedList<_> = parallel_units_map
            .into_iter()
            .map(|(_k, v)| v.into_iter())
            .collect();

        // Visit the parallel units in a round-robin manner on each worker.
        let mut round_robin = Vec::new();
        while !parallel_units.is_empty() {
            parallel_units.drain_filter(|ps| {
                if let Some(p) = ps.next() {
                    round_robin.push(p);
                    false
                } else {
                    true
                }
            });
        }

        Self {
            all_parallel_units: round_robin,
        }
    }

    /// Schedules input fragments to different parallel units (workers).
    /// The schedule procedure is two-fold:
    /// (1) For singleton fragments, we schedule each to one parallel unit randomly.
    /// (2) For normal fragments, we schedule them to each worker node in a round-robin manner.
    pub fn schedule(
        &self,
        fragment: &mut Fragment,
        locations: &mut ScheduledLocations,
    ) -> MetaResult<()> {
        if fragment.actors.is_empty() {
            bail!("fragment has no actor");
        }

        if fragment.distribution_type == FragmentDistributionType::Single as i32 {
            // Singleton fragment
            let [actor] = fragment.actors.as_slice() else {
                panic!("singleton fragment should only have one actor")
            };

            let parallel_unit =
                if actor.same_worker_node_as_upstream && !actor.upstream_actor_id.is_empty() {
                    // Schedule the fragment to the same parallel unit as upstream.
                    locations.schedule_colocate_with(&actor.upstream_actor_id)?
                } else {
                    // Randomly choose one parallel unit to schedule from all parallel units.
                    self.all_parallel_units
                        .choose(&mut rand::thread_rng())
                        .cloned()
                        .context("no parallel unit to schedule")?
                };

            // Build vnode mapping. However, we'll leave vnode field of actors unset for singletons.
            let _vnode_mapping =
                self.set_fragment_vnode_mapping(fragment, &[parallel_unit.clone()])?;

            // Record actor locations.
            locations
                .actor_locations
                .insert(fragment.actors[0].actor_id, parallel_unit);
        } else {
            // Normal fragment
            let parallel_units = if self.all_parallel_units.len() < fragment.actors.len() {
                bail!(
                    "not enough parallel units to schedule, required {} got {}",
                    fragment.actors.len(),
                    self.all_parallel_units.len(),
                );
            } else {
                // By taking a prefix of all parallel units, we schedule the actors round-robin-ly.
                // Then sort them by parallel unit id to make the actor ids continuous against the
                // parallel unit id.
                let mut parallel_units = self.all_parallel_units[..fragment.actors.len()].to_vec();
                parallel_units.sort_unstable_by_key(|p| p.id);
                parallel_units
            };

            // Build vnode mapping according to the parallel units.
            let vnode_mapping = self.set_fragment_vnode_mapping(fragment, &parallel_units)?;

            let vnode_bitmaps = vnode_mapping_to_bitmaps(vnode_mapping);

            // Record actor locations and set vnodes into the actors.
            for (actor, parallel_unit) in fragment.actors.iter_mut().zip_eq(parallel_units) {
                let parallel_unit =
                    if actor.same_worker_node_as_upstream && !actor.upstream_actor_id.is_empty() {
                        locations.schedule_colocate_with(&actor.upstream_actor_id)?
                    } else {
                        parallel_unit.clone()
                    };

                actor.vnode_bitmap =
                    Some(vnode_bitmaps.get(&parallel_unit.id).unwrap().to_protobuf());
                locations
                    .actor_locations
                    .insert(actor.actor_id, parallel_unit);
            }
        }

        Ok(())
    }

    /// `set_fragment_vnode_mapping` works by following steps:
    /// 1. Build a vnode mapping according to parallel units where the fragment is scheduled.
    /// 2. Set the vnode mapping into the fragment.
    /// 3. Record the relationship between state tables and vnode mappings.
    fn set_fragment_vnode_mapping(
        &self,
        fragment: &mut Fragment,
        parallel_units: &[ParallelUnit],
    ) -> MetaResult<VnodeMapping> {
        let vnode_mapping = build_vnode_mapping(parallel_units);
        let (original_indices, data) = compress_data(&vnode_mapping);
        fragment.vnode_mapping = Some(ParallelUnitMapping {
            original_indices,
            data,
            ..Default::default()
        });

        Ok(vnode_mapping)
    }
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_common::buffer::Bitmap;
    use risingwave_common::types::VIRTUAL_NODE_COUNT;
    use risingwave_pb::catalog::Table;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::{MaterializeNode, StreamActor, StreamNode, TopNNode};

    use super::*;
    use crate::manager::{ClusterManager, MetaSrvEnv};

    #[tokio::test]
    async fn test_schedule() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_manager =
            Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);

        let node_count = 4;
        let fake_parallelism = 4;
        for i in 0..node_count {
            let host = HostAddress {
                host: "127.0.0.1".to_string(),
                port: i as i32,
            };
            cluster_manager
                .add_worker_node(WorkerType::ComputeNode, host.clone(), fake_parallelism)
                .await?;
            cluster_manager.activate_worker_node(host).await?;
        }

        let scheduler = Scheduler::new(cluster_manager.list_active_parallel_units().await);
        let mut locations = ScheduledLocations::new();

        let mut actor_id = 1u32;
        let mut single_fragments = (1..6u32)
            .map(|id| {
                let fragment = Fragment {
                    fragment_id: id,
                    fragment_type: 0,
                    distribution_type: FragmentDistributionType::Single as i32,
                    actors: vec![StreamActor {
                        actor_id,
                        fragment_id: id,
                        nodes: Some(StreamNode {
                            node_body: Some(NodeBody::TopN(TopNNode {
                                table: Some(Table {
                                    id: 0,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        dispatcher: vec![],
                        upstream_actor_id: vec![],
                        same_worker_node_as_upstream: false,
                        vnode_bitmap: None,
                    }],
                    ..Default::default()
                };
                actor_id += 1;
                fragment
            })
            .collect_vec();

        let parallel_degree = fake_parallelism;
        let mut normal_fragments = (6..8u32)
            .map(|fragment_id| {
                let actors = (actor_id..actor_id + node_count * parallel_degree as u32)
                    .map(|id| StreamActor {
                        actor_id: id,
                        fragment_id,
                        nodes: Some(StreamNode {
                            node_body: Some(NodeBody::Materialize(MaterializeNode {
                                table_id: fragment_id as u32,
                                ..Default::default()
                            })),
                            ..Default::default()
                        }),
                        dispatcher: vec![],
                        upstream_actor_id: vec![],
                        same_worker_node_as_upstream: false,
                        vnode_bitmap: None,
                    })
                    .collect_vec();
                actor_id += node_count * parallel_degree as u32;
                Fragment {
                    fragment_id,
                    fragment_type: 0,
                    distribution_type: FragmentDistributionType::Hash as i32,
                    actors,
                    ..Default::default()
                }
            })
            .collect_vec();

        // Test round robin schedule for singleton fragments
        for fragment in &mut single_fragments {
            scheduler.schedule(fragment, &mut locations).unwrap();
        }
        for fragment in single_fragments {
            assert_ne!(fragment.vnode_mapping, None);
            for actor in fragment.actors {
                assert!(actor.vnode_bitmap.is_none());
            }
        }

        // Test normal schedule for other fragments
        for fragment in &mut normal_fragments {
            scheduler.schedule(fragment, &mut locations).unwrap();
        }
        assert_eq!(
            locations
                .actor_locations
                .iter()
                .filter(|(actor_id, _)| {
                    normal_fragments[1]
                        .actors
                        .iter()
                        .map(|actor| actor.actor_id)
                        .contains(actor_id)
                })
                .count(),
            node_count as usize * parallel_degree
        );
        for fragment in normal_fragments {
            assert_ne!(fragment.vnode_mapping, None,);
            let mut vnode_sum = 0;
            for actor in fragment.actors {
                vnode_sum += Bitmap::from(actor.get_vnode_bitmap()?).num_high_bits();
            }
            assert_eq!(vnode_sum as usize, VIRTUAL_NODE_COUNT);
        }

        Ok(())
    }
}
