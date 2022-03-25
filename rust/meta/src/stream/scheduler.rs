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

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::{ActorInfo, ParallelUnit, ParallelUnitType};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::Fragment;

use crate::cluster::{NodeId, NodeLocations, StoredClusterManager};
use crate::model::ActorId;
use crate::storage::MetaStore;

/// [`Scheduler`] defines schedule logic for mv actors.
pub struct Scheduler<S> {
    cluster_manager: Arc<StoredClusterManager<S>>,
    /// Round robin counter for singleton fragments
    single_rr: AtomicUsize,
}
/// [`ScheduledLocations`] represents the location of scheduled result.
pub struct ScheduledLocations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, ParallelUnit>,
    /// worker location map.
    pub node_locations: NodeLocations,
}

impl ScheduledLocations {
    pub fn new() -> Self {
        Self {
            actor_locations: BTreeMap::new(),
            node_locations: HashMap::new(),
        }
    }

    /// [`node_actors`] returns all actors for every node.
    pub fn node_actors(&self) -> HashMap<NodeId, Vec<ActorId>> {
        let mut node_actors = HashMap::new();
        self.actor_locations
            .iter()
            .for_each(|(actor_id, parallel_unit)| {
                node_actors
                    .entry(parallel_unit.worker_node_id)
                    .or_insert_with(Vec::new)
                    .push(*actor_id);
            });

        node_actors
    }

    /// [`actor_info_map`] returns the `ActorInfo` map for every actor.
    pub fn actor_info_map(&self) -> HashMap<ActorId, ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| {
                (
                    *actor_id,
                    ActorInfo {
                        actor_id: *actor_id,
                        host: self.node_locations[&parallel_unit.worker_node_id]
                            .host
                            .clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>()
    }

    /// [`actor_infos`] returns the `ActorInfo` slice.
    pub fn actor_infos(&self) -> Vec<ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| ActorInfo {
                actor_id: *actor_id,
                host: self.node_locations[&parallel_unit.worker_node_id]
                    .host
                    .clone(),
            })
            .collect::<Vec<_>>()
    }
}

impl<S> Scheduler<S>
where
    S: MetaStore,
{
    pub fn new(cluster_manager: Arc<StoredClusterManager<S>>) -> Self {
        Self {
            cluster_manager,
            single_rr: AtomicUsize::new(0),
        }
    }

    /// [`schedule`] schedules input fragments to different parallel units (workers).
    /// The schedule procedure is two-fold:
    /// (1) For normal fragments, we schedule them to all the hash parallel units in the cluster.
    /// (2) For singleton fragments, we apply the round robin strategy. One single parallel unit in
    /// the cluster is assigned to a singleton fragment once, and all the single parallel units take
    /// turns.
    pub async fn schedule(
        &self,
        fragment: Fragment,
        locations: &mut ScheduledLocations,
    ) -> Result<()> {
        if fragment.actors.is_empty() {
            return Err(InternalError("fragment has no actor".to_string()).into());
        }

        if fragment.distribution_type == FragmentDistributionType::Single as i32 {
            // singleton fragment
            let single_parallel_units = self
                .cluster_manager
                .list_parallel_units(Some(ParallelUnitType::Single))
                .await;
            if let Ok(single_idx) =
                self.single_rr
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |idx| {
                        Some((idx + 1) % single_parallel_units.len())
                    })
            {
                locations.actor_locations.insert(
                    fragment.actors[0].actor_id,
                    single_parallel_units[single_idx].clone(),
                );
            }
        } else {
            // normal fragment
            let parallel_units = self
                .cluster_manager
                .list_parallel_units(Some(ParallelUnitType::Hash))
                .await;
            fragment.actors.iter().enumerate().for_each(|(idx, actor)| {
                locations.actor_locations.insert(
                    actor.actor_id,
                    parallel_units[idx % parallel_units.len()].clone(),
                );
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::stream_plan::StreamActor;

    use super::*;
    use crate::manager::{MetaSrvEnv, NotificationManager};

    #[tokio::test]
    async fn test_schedule() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let notification_manager = Arc::new(NotificationManager::new(env.epoch_generator_ref()));
        let cluster_manager = Arc::new(
            StoredClusterManager::new(env.clone(), notification_manager, Duration::from_secs(3600))
                .await?,
        );

        let node_count = 4;
        for i in 0..node_count {
            let host = HostAddress {
                host: "127.0.0.1".to_string(),
                port: i as i32,
            };
            cluster_manager
                .add_worker_node(host.clone(), WorkerType::ComputeNode)
                .await?;
            cluster_manager.activate_worker_node(host).await?;
        }

        let scheduler = Scheduler::new(cluster_manager);
        let mut locations = ScheduledLocations::new();

        let mut actor_id = 1u32;
        let single_fragments = (1..6u32)
            .map(|id| {
                let fragment = Fragment {
                    fragment_id: id,
                    fragment_type: 0,
                    distribution_type: FragmentDistributionType::Single as i32,
                    actors: vec![StreamActor {
                        actor_id,
                        fragment_id: id,
                        nodes: None,
                        dispatcher: vec![],
                        upstream_actor_id: vec![],
                    }],
                };
                actor_id += 1;
                fragment
            })
            .collect_vec();

        let normal_fragments = (6..8u32)
            .map(|fragment_id| {
                let actors = (actor_id..actor_id + node_count * 7)
                    .map(|id| StreamActor {
                        actor_id: id,
                        fragment_id,
                        nodes: None,
                        dispatcher: vec![],
                        upstream_actor_id: vec![],
                    })
                    .collect_vec();
                actor_id += node_count * 7;
                Fragment {
                    fragment_id,
                    fragment_type: 0,
                    distribution_type: FragmentDistributionType::Hash as i32,
                    actors,
                }
            })
            .collect_vec();

        // Test round robin schedule for singleton fragments
        for fragment in single_fragments {
            scheduler.schedule(fragment, &mut locations).await.unwrap();
        }
        assert_eq!(locations.actor_locations.get(&1).unwrap().id, 0);
        assert_eq!(
            locations.actor_locations.get(&1),
            locations.actor_locations.get(&5)
        );

        // Test normal schedule for other fragments
        for fragment in &normal_fragments {
            scheduler
                .schedule(fragment.clone(), &mut locations)
                .await
                .unwrap();
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
            (node_count * 7) as usize
        );

        Ok(())
    }
}
