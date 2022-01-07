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

/// [`Scheduler`] defines schedule logic for mv fragments.
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

    /// [`schedule`] schedules node for input fragments.
    pub async fn schedule(&self, fragments: &[u32]) -> Result<Vec<WorkerNode>> {
        let nodes = self
            .cluster_manager
            .list_worker_node(ClusterType::ComputeNode)
            .await?;
        if nodes.is_empty() {
            return Err(InternalError("no available node exist".to_string()).into());
        }

        match self.category {
            ScheduleCategory::Simple => Ok(vec![nodes.get(0).unwrap().clone(); fragments.len()]),
            ScheduleCategory::RoundRobin => Ok((0..fragments.len())
                .map(|i| nodes.get(i % nodes.len()).unwrap().clone())
                .collect::<Vec<_>>()),
            ScheduleCategory::Hash => Ok(fragments
                .iter()
                .map(|f| {
                    let mut hasher = DefaultHasher::new();
                    f.hash(&mut hasher);
                    nodes
                        .get(hasher.finish() as usize % nodes.len())
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>()),
        }
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
        let fragments = (0..15).collect::<Vec<u32>>();
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
        let nodes = simple_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0; 15]
        );

        let round_bin_schedule =
            Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager.clone());
        let nodes = round_bin_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
        );

        let round_bin_schedule = Scheduler::new(ScheduleCategory::Hash, cluster_manager.clone());
        let nodes = round_bin_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);

        Ok(())
    }
}
