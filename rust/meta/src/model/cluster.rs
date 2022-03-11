use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};

use crate::model::MetadataModel;

/// Column family name for cluster.
const WORKER_CF_NAME: &str = "cf/worker";

pub const INVALID_EXPIRE_AT: u64 = 0;

#[derive(Clone, Debug)]
pub struct Worker {
    worker_node: WorkerNode,
    expire_at: u64,
}

impl MetadataModel for Worker {
    type ProstType = WorkerNode;
    type KeyType = HostAddress;

    fn cf_name() -> String {
        WORKER_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.worker_node.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self {
            worker_node: prost,
            expire_at: INVALID_EXPIRE_AT,
        }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(self.worker_node.get_host()?.clone())
    }
}

impl Worker {
    pub fn worker_type(&self) -> WorkerType {
        WorkerType::from_i32(self.worker_node.r#type).unwrap()
    }

    pub fn worker_node(&self) -> WorkerNode {
        self.worker_node.clone()
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_node.id
    }

    pub fn expire_at(&self) -> u64 {
        self.expire_at
    }

    pub fn set_expire_at(&mut self, expire_at: u64) {
        self.expire_at = expire_at;
    }
}
