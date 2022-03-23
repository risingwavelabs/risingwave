// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerNode};

use crate::model::MetadataModel;

/// Column family name for cluster.
const WORKER_CF_NAME: &str = "cf/worker";

pub const INVALID_EXPIRE_AT: u64 = 0;

#[derive(Clone, Debug)]
pub struct Worker {
    pub worker_node: WorkerNode,
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
