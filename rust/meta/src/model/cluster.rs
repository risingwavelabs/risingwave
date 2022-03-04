use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};

use crate::model::MetadataModel;

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
    pub fn worker_type(&self) -> WorkerType {
        WorkerType::from_i32(self.0.r#type).unwrap()
    }

    pub fn worker_node(&self) -> WorkerNode {
        self.0.clone()
    }

    pub fn worker_id(&self) -> u32 {
        self.0.id
    }
}
