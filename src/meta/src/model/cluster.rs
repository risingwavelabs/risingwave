// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::ops::{Add, Deref};
use std::time::{Duration, SystemTime};

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use risingwave_pb::meta::heartbeat_request::extra_info::Info;
use uuid::Uuid;

use super::MetadataModelError;
use crate::model::{MetadataModel, MetadataModelResult};
use crate::storage::{MetaStore, MetaStoreError, Snapshot};

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
    hummock_gc_watermark: Option<HummockSstableObjectId>,
}

impl MetadataModel for Worker {
    type KeyType = HostAddress;
    type PbType = WorkerNode;

    fn cf_name() -> String {
        WORKER_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::PbType {
        self.worker_node.clone()
    }

    fn from_protobuf(prost: Self::PbType) -> Self {
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
        self.worker_node.r#type()
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

    pub fn hummock_gc_watermark(&self) -> Option<HummockSstableObjectId> {
        self.hummock_gc_watermark
    }

    pub fn info_version_id(&self) -> u64 {
        self.info_version_id
    }
}

const CLUSTER_ID_CF_NAME: &str = "cf";
const CLUSTER_ID_KEY: &[u8] = "cluster_id".as_bytes();

#[derive(Clone, Debug)]
pub struct ClusterId(String);

impl ClusterId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    fn from_bytes(bytes: Vec<u8>) -> MetadataModelResult<Self> {
        Ok(Self(
            String::from_utf8(bytes).map_err(MetadataModelError::internal)?,
        ))
    }

    pub(crate) async fn from_meta_store<S: MetaStore>(
        meta_store: &S,
    ) -> MetadataModelResult<Option<Self>> {
        Self::from_snapshot::<S>(&meta_store.snapshot().await).await
    }

    pub(crate) async fn from_snapshot<S: MetaStore>(
        s: &S::Snapshot,
    ) -> MetadataModelResult<Option<Self>> {
        match s.get_cf(CLUSTER_ID_CF_NAME, CLUSTER_ID_KEY).await {
            Ok(bytes) => Ok(Some(Self::from_bytes(bytes)?)),
            Err(e) => match e {
                MetaStoreError::ItemNotFound(_) => Ok(None),
                _ => Err(e.into()),
            },
        }
    }

    pub(crate) async fn put_at_meta_store<S: MetaStore>(
        &self,
        meta_store: &S,
    ) -> MetadataModelResult<()> {
        Ok(meta_store
            .put_cf(
                CLUSTER_ID_CF_NAME,
                CLUSTER_ID_KEY.to_vec(),
                self.0.clone().into_bytes(),
            )
            .await?)
    }
}

impl From<ClusterId> for String {
    fn from(value: ClusterId) -> Self {
        value.0
    }
}

impl From<String> for ClusterId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Deref for ClusterId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}
