use std::sync::Arc;

use prost::Message;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::common::{Cluster, HostAddress, WorkerNode};
use risingwave_pb::meta::ClusterType;

use crate::manager::{Config, IdCategory, IdGeneratorManagerRef, MetaSrvEnv, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct StoredClusterManager {
    meta_store_ref: MetaStoreRef,
    id_gen_manager_ref: IdGeneratorManagerRef,
    config: Arc<Config>,
}

impl StoredClusterManager {
    pub fn new(env: MetaSrvEnv) -> Self {
        Self {
            meta_store_ref: env.meta_store_ref(),
            id_gen_manager_ref: env.id_gen_manager_ref(),
            config: env.config(),
        }
    }
}

impl StoredClusterManager {
    pub async fn list_cluster(&self) -> Result<Vec<Cluster>> {
        let clusters_pb = self
            .meta_store_ref
            .list_cf(self.config.get_cluster_cf())
            .await?;

        Ok(clusters_pb
            .iter()
            .map(|c| Cluster::decode(c.as_slice()).unwrap())
            .collect::<Vec<_>>())
    }

    pub async fn get_cluster(&self, cluster_id: u32) -> Result<Cluster> {
        let cluster_pb = self
            .meta_store_ref
            .get_cf(
                self.config.get_cluster_cf(),
                &cluster_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;

        Ok(Cluster::decode(cluster_pb.as_slice())?)
    }

    pub async fn put_cluster(&self, cluster: Cluster) -> Result<()> {
        self.meta_store_ref
            .put_cf(
                self.config.get_cluster_cf(),
                &cluster.get_id().to_be_bytes(),
                &cluster.encode_to_vec(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }

    pub async fn delete_cluster(&self, cluster_id: u32) -> Result<()> {
        self.meta_store_ref
            .delete_cf(
                self.config.get_cluster_cf(),
                &cluster_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }

    pub async fn add_worker_node(
        &self,
        host_address: HostAddress,
        cluster_type: ClusterType,
    ) -> Result<(WorkerNode, bool)> {
        let cluster_id = cluster_type as u32;
        // FIXME: there's is a consistency problem between get/set, fix this after refactor
        // metastore.
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
        match cluster
            .nodes
            .iter()
            // TODO: should use a hashmap here
            .position(|n| n.get_host() == Ok(&host_address))
        {
            // If exist already, return its info(id) directly. Worker might be added duplicated cuz
            // reboot is quite normal for workers.
            Some(idx) => Ok((cluster.nodes.get(idx).unwrap().clone(), false)),
            None => {
                let next_id = self
                    .id_gen_manager_ref
                    .generate::<{ IdCategory::Worker }>()
                    .await?;
                let ret_node = WorkerNode {
                    id: next_id as u32,
                    host: Some(host_address.clone()),
                };
                cluster.nodes.push(ret_node.clone());
                let _res = self.put_cluster(cluster).await?;
                Ok((ret_node, true))
            }
        }
    }

    pub async fn delete_worker_node(
        &self,
        node: WorkerNode,
        cluster_type: ClusterType,
    ) -> Result<()> {
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

    pub async fn list_worker_node(&self, cluster_type: ClusterType) -> Result<Vec<WorkerNode>> {
        let cluster_id = cluster_type as u32;
        let cluster = self.get_cluster(cluster_id).await?;
        Ok(cluster.nodes)
    }
}

#[cfg(test)]
mod tests {

    use risingwave_pb::common::WorkerNode;

    use super::*;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        let cluster_manager = StoredClusterManager::new(MetaSrvEnv::for_test().await);

        assert!(cluster_manager.list_cluster().await.is_ok());
        assert!(cluster_manager.get_cluster(0).await.is_err());

        for i in 0..100 {
            assert!(cluster_manager
                .put_cluster(Cluster {
                    id: i,
                    nodes: vec![WorkerNode {
                        id: i * 2,
                        host: None
                    }],
                    config: Default::default()
                })
                .await
                .is_ok());
        }

        let cluster = cluster_manager.get_cluster(10).await?;
        assert_eq!(cluster.id, 10);
        assert_eq!(cluster.nodes[0].id, 20);
        let clusters = cluster_manager.list_cluster().await?;
        assert_eq!(clusters.len(), 100);

        Ok(())
    }
}
