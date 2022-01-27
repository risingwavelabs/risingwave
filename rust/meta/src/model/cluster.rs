use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};

use crate::model::MetadataModel;
use crate::storage::MetaStoreRef;

/// Column family name for cluster.
const WORKER_CF_NAME: &str = "cf/worker";

#[derive(Clone)]
pub struct Worker(WorkerNode);

impl MetadataModel for Worker {
    type ProstType = WorkerNode;
    type KeyType = HostAddress;

    fn cf_name() -> String {
        WORKER_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.0.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self(prost)
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(self.0.get_host()?.clone())
    }
}

impl Worker {
    /// TODO: refine this when metastore support prefix scan, using type as its key prefix.
    pub async fn filter(store: &MetaStoreRef, r#type: WorkerType) -> Result<Vec<Self>> {
        let workers = Self::list(store).await?;
        Ok(workers
            .into_iter()
            .filter(|w| w.0.r#type == r#type as i32)
            .collect::<Vec<_>>())
    }
}
