use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::{ActorInfo, ParallelUnit, ParallelUnitType};
use risingwave_pb::meta::table_fragments::Fragment;

use crate::cluster::{NodeId, NodeLocations, StoredClusterManager};
use crate::model::ActorId;
use crate::storage::MetaStore;

/// [`Scheduler`] defines schedule logic for mv actors.
pub struct Scheduler<S>
where
    S: MetaStore,
{
    cluster_manager: Arc<StoredClusterManager<S>>,
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
                    .entry(parallel_unit.node_id)
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
                        host: self.node_locations[&parallel_unit.node_id].host.clone(),
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
                host: self.node_locations[&parallel_unit.node_id].host.clone(),
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

    /// [`schedule`] schedules input actors to different workers.
    /// The schedule procedure is two-fold:
    /// (1) For regular actors, we use some strategies to schedule them.
    /// (2) For source actors under certain cases (determined elsewhere), we enforce round robin
    /// strategy to ensure that each compute node will have one source node.
    ///
    /// The result `Vec<WorkerNode>` contains two parts.
    /// The first part is the schedule result of `actors`, the second part is the schedule result of
    /// `enforced_round_actors`.
    pub async fn schedule(
        &self,
        fragment: Fragment,
        locations: &mut ScheduledLocations,
    ) -> Result<()> {
        if fragment.actors.is_empty() {
            return Err(InternalError("fragment has no actor".to_string()).into());
        }

        if fragment.actors.len() == 1 {
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

    use risingwave_pb::common::{HostAddress, WorkerType};

    use super::*;
    use crate::manager::{MetaSrvEnv, NotificationManager};

    #[tokio::test]
    async fn test_schedule() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let notification_manager = Arc::new(NotificationManager::new());
        let cluster_manager = Arc::new(
            StoredClusterManager::new(
                env.clone(),
                None,
                notification_manager,
                Duration::from_secs(3600),
            )
            .await?,
        );
        let actors = (0..15).collect::<Vec<u32>>();
        for i in 0..10 {
            let host = HostAddress {
                host: "127.0.0.1".to_string(),
                port: i as i32,
            };
            cluster_manager
                .add_worker_node(host.clone(), WorkerType::ComputeNode)
                .await?;
            cluster_manager.activate_worker_node(host).await?;
        }
        let workers = cluster_manager
            .list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            )
            .await;

        // let scheduler =
        //     Scheduler::new(cluster_manager.clone());
        // let mut locations = ScheduledLocations::new();
        // let nodes = scheduler.schedule(&actors, &mut locations).await?;
        // assert_eq!(nodes.actor_locations.len(), actors.len());
        // assert!(nodes
        //     .actor_locations
        //     .iter()
        //     .enumerate()
        //     .all(|(idx, (_, &n))| n == workers[idx % workers.len()].id));

        Ok(())
    }
}
