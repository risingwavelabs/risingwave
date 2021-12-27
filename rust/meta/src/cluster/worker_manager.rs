use async_trait::async_trait;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::common::{Cluster, HostAddress, WorkerNode};
use risingwave_pb::meta::ClusterType;

use crate::cluster::{ClusterMetaManager, StoredClusterManager};

#[async_trait]
pub trait WorkerNodeMetaManager: Sync + Send + 'static {
    async fn add_worker_node(
        &self,
        host_address: HostAddress,
        cluster_type: ClusterType,
    ) -> Result<WorkerNode>;
    async fn delete_worker_node(&self, node: WorkerNode, cluster_type: ClusterType) -> Result<()>;
    async fn list_worker_node(&self, cluster_type: ClusterType) -> Result<Vec<WorkerNode>>;
}

#[async_trait]
impl WorkerNodeMetaManager for StoredClusterManager {
    async fn add_worker_node(
        &self,
        host_address: HostAddress,
        cluster_type: ClusterType,
    ) -> Result<WorkerNode> {
        let cluster_id = cluster_type as u32;
        let mut cluster = match self.get_cluster(cluster_id).await {
            Ok(cluster) => cluster,
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    return Err(err);
                }
                Cluster {
                    id: cluster_id,
                    nodes: vec![],
                    config: Default::default(),
                }
            }
        };
        let next_id = (cluster.nodes.clone().len()) as u32;
        let mut contained = false;
        cluster
            .clone()
            .nodes
            .into_iter()
            .for_each(|node| contained = contained || (node.host.eq(&Some(host_address.clone()))));

        match contained {
            true => Err(RwError::from(InternalError(
                "Worker already exists!".to_string(),
            ))),
            false => {
                let ret_node = WorkerNode {
                    id: next_id,
                    host: Some(host_address.clone()),
                };
                cluster.nodes.push(ret_node.clone());
                let _res = self.put_cluster(cluster).await?;
                Ok(ret_node)
            }
        }
    }

    async fn delete_worker_node(&self, node: WorkerNode, cluster_type: ClusterType) -> Result<()> {
        let cluster_id = cluster_type as u32;
        let cluster = self.get_cluster(cluster_id).await?;
        let mut contained = false;
        let mut new_worker_list = Vec::new();
        cluster.nodes.into_iter().for_each(|e| {
            let equal_check = e.eq(&node);
            contained = contained || equal_check;
            if !equal_check {
                new_worker_list.push(e);
            }
        });

        let new_cluster = Cluster {
            id: cluster.id,
            nodes: new_worker_list,
            config: cluster.config,
        };
        let _res = self.put_cluster(new_cluster).await?;

        match contained {
            true => Ok(()),
            false => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
        }
    }

    async fn list_worker_node(&self, cluster_type: ClusterType) -> Result<Vec<WorkerNode>> {
        let cluster_id = cluster_type as u32;
        let cluster = self.get_cluster(cluster_id).await?;
        Ok(cluster.nodes)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use risingwave_pb::common::Cluster;

    use super::*;
    use crate::manager::Config;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_worker_manager() -> Result<()> {
        // Initialize cluster store manager.
        let meta_store_ref = Arc::new(MemStore::new());
        let cluster_manager = StoredClusterManager::new(meta_store_ref.clone(), Config::default());

        assert!(cluster_manager.list_cluster().await.is_ok());
        assert!(cluster_manager.get_cluster(0).await.is_err());

        // Initialize olap cluster and streaming cluster.
        let hosts = (0..100)
            .map(|e| HostAddress {
                host: "127.0.0.1".to_string(),
                port: (8888 + e) as i32,
            })
            .collect::<Vec<_>>();

        let worker_node = WorkerNode {
            id: 1_u32,
            host: Some(hosts[0].clone()),
        };

        let olap_cluster = Cluster {
            id: 0,
            nodes: vec![worker_node.clone()],
            config: Default::default(),
        };

        let streaming_cluster = Cluster {
            id: 1,
            nodes: vec![worker_node],
            config: Default::default(),
        };

        cluster_manager.put_cluster(olap_cluster).await?;
        cluster_manager.put_cluster(streaming_cluster).await?;

        // Test cases.
        let res1 = cluster_manager
            .add_worker_node(hosts[1].clone(), ClusterType::ComputeNode)
            .await;
        assert_matches!(res1,Ok(node) => {
          assert_eq!(node.id, 1);
          if let Some(node_host) = node.host{
            let expect_host = hosts[1].clone();
            assert_eq!(node_host.host,expect_host.host);
            assert_eq!(node_host.port,expect_host.port);
          }
        });

        let res2 = cluster_manager
            .add_worker_node(hosts[2].clone(), ClusterType::ComputeNode)
            .await;
        assert_matches!(res2,Ok(node) => {
          assert_eq!(node.id, 2);
          if let Some(node_host) = node.host{
            let expect_host = hosts[2].clone();
            assert_eq!(node_host.host,expect_host.host);
            assert_eq!(node_host.port,expect_host.port);
          }
        });
        let res3 = cluster_manager
            .add_worker_node(hosts[2].clone(), ClusterType::ComputeNode)
            .await;
        assert!(res3.is_err());

        let res4 = cluster_manager
            .add_worker_node(hosts[3].clone(), ClusterType::ComputeNode)
            .await;
        assert_matches!(res4,Ok(node) => {
          assert_eq!(node.id, 3);
          if let Some(node_host) = node.host{
            let expect_host = hosts[3].clone();
            assert_eq!(node_host.host,expect_host.host);
            assert_eq!(node_host.port,expect_host.port);
          }
        });

        let res5 = cluster_manager
            .add_worker_node(hosts[2].clone(), ClusterType::Frontend)
            .await;
        assert_matches!(res5,Ok(node) => {
          assert_eq!(node.id, 1);
          if let Some(node_host) = node.host{
            let expect_host = hosts[2].clone();
            assert_eq!(node_host.host,expect_host.host);
            assert_eq!(node_host.port,expect_host.port);
          }
        });

        let delete_node = WorkerNode {
            id: 2,
            host: Some(hosts[2].clone()),
        };
        let res6 = cluster_manager
            .delete_worker_node(delete_node, ClusterType::ComputeNode)
            .await;
        assert!(res6.is_ok());

        let list_olap_nodes = cluster_manager
            .list_worker_node(ClusterType::ComputeNode)
            .await?;
        assert_eq!(list_olap_nodes.len(), 3);
        let list_stream_nodes = cluster_manager
            .list_worker_node(ClusterType::Frontend)
            .await?;
        assert_eq!(list_stream_nodes.len(), 2);

        Ok(())
    }
}
