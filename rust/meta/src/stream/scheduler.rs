use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::{ActorInfo, WorkerType};

use crate::cluster::{NodeId, NodeLocations, StoredClusterManager};
use crate::model::{ActorId, ActorLocations};

/// [`ScheduleCategory`] defines all supported categories.
pub enum ScheduleCategory {
    /// `Simple` always schedules the first node in cluster.
    #[allow(dead_code)]
    Simple = 1,
    /// `RoundRobin` schedules node in cluster with round robin.
    RoundRobin = 2,
    #[allow(dead_code)]
    /// `Hash` schedules node using hash(actor_id) as its index.
    Hash = 3,
}

/// [`Scheduler`] defines schedule logic for mv actors.
pub struct Scheduler {
    cluster_manager: Arc<StoredClusterManager>,
    category: ScheduleCategory,
}

/// [`ScheduledLocations`] represents the location of scheduled result.
pub struct ScheduledLocations {
    /// actor location map.
    pub actor_locations: ActorLocations,
    /// worker location map.
    pub node_locations: NodeLocations,
}

impl ScheduledLocations {
    /// [`node_actors`] returns all actors for every node.
    pub fn node_actors(&self) -> HashMap<NodeId, Vec<ActorId>> {
        let mut node_actors = HashMap::new();
        self.actor_locations.iter().for_each(|(actor_id, node_id)| {
            node_actors
                .entry(*node_id)
                .or_insert_with(Vec::new)
                .push(*actor_id);
        });

        node_actors
    }

    /// [`actor_info_map`] returns the `ActorInfo` map for every actor.
    pub fn actor_info_map(&self) -> HashMap<ActorId, ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, node_id)| {
                (
                    *actor_id,
                    ActorInfo {
                        actor_id: *actor_id,
                        host: self.node_locations[node_id].host.clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>()
    }

    /// [`actor_infos`] returns the `ActorInfo` slice.
    pub fn actor_infos(&self) -> Vec<ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, node_id)| ActorInfo {
                actor_id: *actor_id,
                host: self.node_locations[node_id].host.clone(),
            })
            .collect::<Vec<_>>()
    }
}

impl Scheduler {
    pub fn new(category: ScheduleCategory, cluster_manager: Arc<StoredClusterManager>) -> Self {
        Self {
            cluster_manager,
            category,
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
    pub fn schedule(
        &self,
        actors: &[ActorId],
        enforce_round_actors: &[ActorId],
    ) -> Result<ScheduledLocations> {
        let nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode);
        if nodes.is_empty() {
            return Err(InternalError("no available node exist".to_string()).into());
        }
        // Assume that the number of actors to be forcefully scheduled by round robin is the same as
        // the number of worker nodes.
        if !enforce_round_actors.is_empty() && enforce_round_actors.len() % nodes.len() != 0 {
            return Err(InternalError(
                "the source actor number does not match worker number!".to_string(),
            )
            .into());
        }
        let mut actor_locations = BTreeMap::new();
        enforce_round_actors
            .iter()
            .enumerate()
            .for_each(|(idx, actor)| {
                actor_locations.insert(*actor, nodes[idx % nodes.len()].id);
            });

        actors
            .iter()
            .enumerate()
            .for_each(|(idx, actor)| match self.category {
                ScheduleCategory::Simple => {
                    actor_locations.insert(*actor, nodes[0].id);
                }
                ScheduleCategory::RoundRobin => {
                    actor_locations.insert(*actor, nodes[idx % nodes.len()].id);
                }
                ScheduleCategory::Hash => {
                    let mut hasher = DefaultHasher::new();
                    actor.hash(&mut hasher);
                    let hash_value = hasher.finish() as usize;
                    actor_locations.insert(*actor, nodes[hash_value % nodes.len()].id);
                }
            });

        Ok(ScheduledLocations {
            actor_locations,
            node_locations: nodes.iter().map(|node| (node.id, node.clone())).collect(),
        })
    }
}

#[cfg(test)]
mod test {
    use risingwave_pb::common::HostAddress;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_schedule() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_manager = Arc::new(StoredClusterManager::new(env.clone(), None).await?);
        let actors = (0..15).collect::<Vec<u32>>();
        let source_actors = (20..30).collect::<Vec<u32>>();
        for i in 0..10 {
            cluster_manager
                .add_worker_node(
                    HostAddress {
                        host: "127.0.0.1".to_string(),
                        port: i as i32,
                    },
                    WorkerType::ComputeNode,
                )
                .await?;
        }
        let workers = cluster_manager.list_worker_node(WorkerType::ComputeNode);

        let simple_schedule = Scheduler::new(ScheduleCategory::Simple, cluster_manager.clone());
        let nodes = simple_schedule.schedule(&actors, &[])?;
        assert_eq!(nodes.actor_locations.len(), actors.len());
        assert!(nodes
            .actor_locations
            .iter()
            .all(|(_, &n)| n == workers[0].id));

        let round_bin_schedule =
            Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager.clone());
        let nodes = round_bin_schedule.schedule(&actors, &[])?;
        assert_eq!(nodes.actor_locations.len(), actors.len());
        assert!(nodes
            .actor_locations
            .iter()
            .enumerate()
            .all(|(idx, (_, &n))| n == workers[idx % workers.len()].id));

        let hash_schedule = Scheduler::new(ScheduleCategory::Hash, cluster_manager.clone());
        let nodes = hash_schedule.schedule(&actors, &[])?;
        assert_eq!(nodes.actor_locations.len(), actors.len());

        let round_bin_schedule2 =
            Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager.clone());
        let nodes = round_bin_schedule2.schedule(&actors, &source_actors)?;
        assert_eq!(nodes.actor_locations.len(), 25);
        assert!(actors.iter().enumerate().all(|(idx, actor)| {
            nodes.actor_locations[actor] == workers[idx % workers.len()].id
        }));
        assert!(source_actors.iter().enumerate().all(|(idx, actor)| {
            nodes.actor_locations[actor] == workers[idx % workers.len()].id
        }));

        Ok(())
    }
}
