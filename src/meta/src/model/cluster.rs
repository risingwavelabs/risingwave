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

use std::cmp;
use std::ops::Add;
use std::time::{Duration, SystemTime};

use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use risingwave_pb::meta::heartbeat_request::extra_info::Info;

use crate::model::{MetadataModel, MetadataModelResult};

/// Column family name for cluster.
const WORKER_CF_NAME: &str = "cf/worker";

pub const INVALID_EXPIRE_AT: u64 = 0;

#[derive(Clone, Debug)]
pub struct Worker {
    pub worker_node: WorkerNode,

    // Volatile values updated by meta node as follows.
    //
    // Unix timestamp that the worker will expire at.
    expire_at: u64,

    // Volatile values updated by worker as follows:
    //
    // Monotonic increasing id since meta node bootstrap.
    info_version_id: u64,
    // GC watermark.
    hummock_gc_watermark: Option<HummockSstableId>,
}

impl MetadataModel for Worker {
    type KeyType = HostAddress;
    type ProstType = WorkerNode;

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
            info_version_id: 0,
            hummock_gc_watermark: Default::default(),
        }
    }

    fn key(&self) -> MetadataModelResult<Self::KeyType> {
        Ok(self.worker_node.get_host()?.clone())
    }
}

impl Worker {
    pub fn worker_id(&self) -> u32 {
        self.worker_node.id
    }

    pub fn worker_type(&self) -> WorkerType {
        WorkerType::from_i32(self.worker_node.r#type).expect("Invalid worker type")
    }

    pub fn expire_at(&self) -> u64 {
        self.expire_at
    }

    pub fn update_ttl(&mut self, ttl: Duration) {
        let expire_at = cmp::max(
            self.expire_at(),
            SystemTime::now()
                .add(ttl)
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Clock may have gone backwards")
                .as_secs(),
        );
        self.expire_at = expire_at;
    }

    pub fn update_info(&mut self, info: Vec<Info>) {
        self.info_version_id += 1;
        for i in info {
            match i {
                Info::HummockGcWatermark(info) => {
                    self.hummock_gc_watermark = Some(info);
                }
            }
        }
    }

    pub fn hummock_gc_watermark(&self) -> Option<HummockSstableId> {
        self.hummock_gc_watermark
    }

    pub fn info_version_id(&self) -> u64 {
        self.info_version_id
    }
}
