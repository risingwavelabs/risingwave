use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::ClusterType;

use crate::cluster::{StoredClusterManager, WorkerNodeMetaManager};

/// [`ScheduleCategory`] defines all supported categories.
pub enum ScheduleCategory {
    /// `Simple` always schedules the first node in cluster.
    Simple = 1,
    /// `RoundRobin` schedules node in cluster with round robin.
    RoundRobin = 2,
    /// `Hash` schedules node using hash(actor_id) as its index.
    Hash = 3,
}

/// [`Scheduler`] defines schedule logic for mv actors.
pub struct Scheduler {
    cluster_manager: Arc<StoredClusterManager>,
    category: ScheduleCategory,
}

impl Scheduler {
    pub fn new(category: ScheduleCategory, cluster_manager: Arc<StoredClusterManager>) -> Self {
        Self {
            cluster_manager,
            category,
        }
    }

    /// [`schedule`] schedules node for input actors.
    /// The schedule procedure is two-fold:
    /// (1) For regular actors, we use some strategies to schedule them.
    /// (2) For source actors under certain cases (determined elsewhere), we enforce round robin
    /// strategy to ensure that each compute node will have one source node.
    ///
    /// Note that we assume there is no
    ///
    /// The result `Vec<WorkerNode>` contains two parts.
    /// The first part is the schedule result of `actors`, the second part is the schedule result of
    /// `enforced_round_actors`.
    pub async fn schedule(
        &self,
        actors: &[u32],
        enforce_round_actors: &[u32],
    ) -> Result<Vec<WorkerNode>> {
        let nodes = self
            .cluster_manager
            .list_worker_node(ClusterType::ComputeNode)
            .await?;
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
        let enforced_round_actor_schedule = (0..enforce_round_actors.len())
            .map(|i| nodes.get(i % nodes.len()).unwrap().clone())
            .collect::<Vec<_>>();

        let mut ret_list = match self.category {
            ScheduleCategory::Simple => {
                // Make rust analyzer happy.
                let ret = vec![nodes.get(0).unwrap().clone(); actors.len()];
                ret
            }
            ScheduleCategory::RoundRobin => (0..actors.len())
                .map(|i| nodes.get(i % nodes.len()).unwrap().clone())
                .collect::<Vec<_>>(),
            ScheduleCategory::Hash => actors
                .iter()
                .map(|f| {
                    let mut hasher = DefaultHasher::new();
                    f.hash(&mut hasher);
                    nodes
                        .get(hasher.finish() as usize % nodes.len())
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>(),
        };
        ret_list.extend(enforced_round_actor_schedule);
        Ok(ret_list)
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
        let cluster_manager = Arc::new(StoredClusterManager::new(env.clone()));
        let actors = (0..15).collect::<Vec<u32>>();
        let source_actors = (20..30).collect::<Vec<u32>>();
        let source_actors2 = (20..40).collect::<Vec<u32>>();
        for i in 0..10 {
            cluster_manager
                .add_worker_node(
                    HostAddress {
                        host: "127.0.0.1".to_string(),
                        port: i as i32,
                    },
                    ClusterType::ComputeNode,
                )
                .await?;
        }

        let simple_schedule = Scheduler::new(ScheduleCategory::Simple, cluster_manager.clone());
        let nodes = simple_schedule.schedule(&actors, &[]).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0; 15]
        );

        let round_bin_schedule =
            Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager.clone());
        let nodes = round_bin_schedule.schedule(&actors, &[]).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
        );

        let hash_schedule = Scheduler::new(ScheduleCategory::Hash, cluster_manager.clone());
        let nodes = hash_schedule.schedule(&actors, &[]).await?;
        assert_eq!(nodes.len(), 15);

        let round_bin_schedule2 =
            Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager.clone());
        let nodes = round_bin_schedule2
            .schedule(&actors, &source_actors)
            .await?;
        assert_eq!(nodes.len(), 25);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        let simple_schedule2 = Scheduler::new(ScheduleCategory::Simple, cluster_manager.clone());
        let nodes = simple_schedule2.schedule(&actors, &source_actors).await?;
        assert_eq!(nodes.len(), 25);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        let simple_schedule3 = Scheduler::new(ScheduleCategory::Simple, cluster_manager.clone());
        let nodes = simple_schedule3.schedule(&actors, &source_actors2).await?;
        assert_eq!(nodes.len(), 35);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
                3, 4, 5, 6, 7, 8, 9
            ]
        );

        Ok(())
    }
}
