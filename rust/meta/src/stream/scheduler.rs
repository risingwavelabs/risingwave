use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::meta::cluster::Node;

/// FIXME: This is a mock trait, replace it when worker management implementation ready in cluster
/// mod.
#[async_trait]
pub trait NodeManager: Sync + Send {
    async fn list_nodes(&self) -> Result<Vec<Node>>;
}

pub type NodeManagerRef = Arc<dyn NodeManager>;

/// [`ScheduleCategory`] defines all supported categories.
pub enum ScheduleCategory {
    /// `Simple` always schedules the first node in cluster.
    Simple = 1,
    /// `RoundRobin` schedules node in cluster with round robin.
    RoundRobin = 2,
    /// `Hash` schedules node using hash(fragment_id) as its index.
    Hash = 3,
}

/// [`Scheduler`] defines schedule logic for mv fragments.
pub struct Scheduler {
    node_manager_ref: NodeManagerRef,
    category: ScheduleCategory,
}

impl Scheduler {
    pub fn new(category: ScheduleCategory, node_manager_ref: NodeManagerRef) -> Self {
        Self {
            node_manager_ref,
            category,
        }
    }

    /// [`schedule`] schedules node for input fragments.
    pub async fn schedule(&self, fragments: &[u32]) -> Result<Vec<Node>> {
        let nodes = self.node_manager_ref.list_nodes().await?;
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
    use risingwave_pb::task_service::HostAddress;

    use super::*;

    pub struct MockNodeManager {}

    #[async_trait]
    impl NodeManager for MockNodeManager {
        async fn list_nodes(&self) -> Result<Vec<Node>> {
            Ok((0..10)
                .map(|i| Node {
                    id: i,
                    host: Some(HostAddress {
                        host: "127.0.0.1".to_string(),
                        port: i as i32,
                    }),
                })
                .collect::<Vec<_>>())
        }
    }

    #[tokio::test]
    async fn test_schedule() -> Result<()> {
        let node_manager_ref = Arc::new(MockNodeManager {});
        let fragments = (0..15).collect::<Vec<u32>>();

        let simple_schedule = Scheduler::new(ScheduleCategory::Simple, node_manager_ref.clone());
        let nodes = simple_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0; 15]
        );

        let round_bin_schedule =
            Scheduler::new(ScheduleCategory::RoundRobin, node_manager_ref.clone());
        let nodes = round_bin_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);
        assert_eq!(
            nodes.iter().map(|n| n.get_id()).collect::<Vec<u32>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4]
        );

        let round_bin_schedule = Scheduler::new(ScheduleCategory::Hash, node_manager_ref.clone());
        let nodes = round_bin_schedule.schedule(&fragments).await?;
        assert_eq!(nodes.len(), 15);

        Ok(())
    }
}
