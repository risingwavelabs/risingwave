use std::hash::{Hash, Hasher};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};

use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{MetadataModel, Worker};
use crate::storage::MetaStoreRef;

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct StoredClusterManager {
    meta_store_ref: MetaStoreRef,
    id_gen_manager_ref: IdGeneratorManagerRef,
    workers: DashMap<WorkerKey, Worker>,
}

struct WorkerKey(HostAddress);

impl PartialEq<Self> for WorkerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for WorkerKey {}

impl Hash for WorkerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.host.hash(state);
        self.0.port.hash(state);
    }
}

impl StoredClusterManager {
    pub async fn new(env: MetaSrvEnv) -> Result<Self> {
        let meta_store_ref = env.meta_store_ref();
        let workers =
            try_match_expand!(Worker::list(&meta_store_ref).await, Ok, "Worker::list fail")?;
        let worker_map = DashMap::new();

        workers.iter().for_each(|w| {
            worker_map.insert(WorkerKey(w.key().unwrap()), w.clone());
        });

        Ok(Self {
            meta_store_ref,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            workers: worker_map,
        })
    }

    pub async fn add_worker_node(
        &self,
        host_address: HostAddress,
        r#type: WorkerType,
    ) -> Result<(WorkerNode, bool)> {
        match self.workers.entry(WorkerKey(host_address.clone())) {
            Entry::Occupied(o) => Ok((o.get().to_protobuf(), false)),
            Entry::Vacant(v) => {
                let id = self
                    .id_gen_manager_ref
                    .generate::<{ IdCategory::Worker }>()
                    .await?;
                let worker = Worker::from_protobuf(WorkerNode {
                    id: id as u32,
                    r#type: r#type as i32,
                    host: Some(host_address),
                });
                worker.insert(&self.meta_store_ref).await?;
                Ok((v.insert(worker).to_protobuf(), true))
            }
        }
    }

    pub async fn delete_worker_node(&self, host_address: HostAddress) -> Result<()> {
        match self.workers.remove(&WorkerKey(host_address.clone())) {
            None => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
            Some(_) => Worker::delete(&self.meta_store_ref, &host_address).await,
        }
    }

    pub fn list_worker_node(&self, worker_type: WorkerType) -> Result<Vec<WorkerNode>> {
        Ok(self
            .workers
            .iter()
            .map(|entry| entry.value().to_protobuf())
            .filter(|w| w.r#type == worker_type as i32)
            .collect::<Vec<_>>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        Ok(())
    }
}
