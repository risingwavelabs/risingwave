// Copyright 2025 RisingWave Labs
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

use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_hummock_sdk::{HummockRawObjectId, HummockSstableObjectId, ObjectIdRange};
use risingwave_pb::hummock::GetNewObjectIdsRequest;
use risingwave_rpc_client::{GrpcCompactorProxyClient, HummockMetaClient};
use sync_point::sync_point;
use thiserror_ext::AsReport;
use tokio::sync::oneshot;

use crate::hummock::{HummockError, HummockResult};
pub type ObjectIdManagerRef = Arc<ObjectIdManager>;

#[async_trait::async_trait]
pub trait GetObjectId: Send + Sync {
    async fn get_new_sst_object_id(&self) -> HummockResult<HummockSstableObjectId>;
}

/// Caches SST object ids fetched from meta.
pub struct ObjectIdManager {
    // Lock order: `wait_queue` before `available_object_ids`.
    wait_queue: Mutex<Option<Vec<oneshot::Sender<bool>>>>,
    available_object_ids: Mutex<ObjectIdRange>,
    remote_fetch_number: u32,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl ObjectIdManager {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>, remote_fetch_number: u32) -> Self {
        Self {
            wait_queue: Default::default(),
            available_object_ids: Mutex::new(ObjectIdRange::new(u64::MIN, u64::MIN)),
            remote_fetch_number,
            hummock_meta_client,
        }
    }

    /// Executes `f` with next SST id.
    /// May fetch new SST ids via RPC.
    async fn map_next_object_id<F>(&self, f: F) -> HummockResult<HummockRawObjectId>
    where
        F: Fn(&mut ObjectIdRange) -> Option<HummockRawObjectId>,
    {
        loop {
            // 1. Try to get
            if let Some(new_id) = f(self.available_object_ids.lock().deref_mut()) {
                return Ok(new_id);
            }
            // 2. Otherwise either fetch new ids, or wait for previous fetch if any.
            let waiter = {
                let mut guard = self.wait_queue.lock();
                if let Some(new_id) = f(self.available_object_ids.lock().deref_mut()) {
                    return Ok(new_id);
                }
                let wait_queue = guard.deref_mut();
                if let Some(wait_queue) = wait_queue {
                    let (tx, rx) = oneshot::channel();
                    wait_queue.push(tx);
                    Some(rx)
                } else {
                    *wait_queue = Some(vec![]);
                    None
                }
            };
            if let Some(waiter) = waiter {
                // Wait for previous fetch
                sync_point!("MAP_NEXT_SST_OBJECT_ID.AS_FOLLOWER");
                let _ = waiter.await;
                continue;
            }
            // Fetch new ids.
            sync_point!("MAP_NEXT_SST_OBJECT_ID.AS_LEADER");
            sync_point!("MAP_NEXT_SST_OBJECT_ID.BEFORE_FETCH");
            async move {
                let new_sst_ids = match self
                    .hummock_meta_client
                    .get_new_object_ids(self.remote_fetch_number)
                    .await
                    .map_err(HummockError::meta_error)
                {
                    Ok(new_sst_ids) => new_sst_ids,
                    Err(err) => {
                        self.notify_waiters(false);
                        return Err(err);
                    }
                };
                sync_point!("MAP_NEXT_SST_OBJECT_ID.AFTER_FETCH");
                sync_point!("MAP_NEXT_SST_OBJECT_ID.BEFORE_FILL_CACHE");
                // Update local cache.
                let result = {
                    let mut guard = self.available_object_ids.lock();
                    let available_object_ids = guard.deref_mut();
                    if new_sst_ids.start_id < available_object_ids.end_id {
                        Err(HummockError::meta_error(format!(
                            "SST id moves backwards. new {} < old {}",
                            new_sst_ids.start_id, available_object_ids.end_id
                        )))
                    } else {
                        *available_object_ids = new_sst_ids;
                        Ok(())
                    }
                };
                self.notify_waiters(result.is_ok());
                result
            }
            .await?;
        }
    }

    fn notify_waiters(&self, success: bool) {
        let mut guard = self.wait_queue.lock();
        let wait_queue = guard.deref_mut().take().unwrap();
        for notify in wait_queue {
            let _ = notify.send(success);
        }
    }
}

#[async_trait::async_trait]
impl GetObjectId for ObjectIdManager {
    /// Returns a new SST id.
    /// The id is guaranteed to be monotonic increasing.
    async fn get_new_sst_object_id(&self) -> HummockResult<HummockSstableObjectId> {
        self.map_next_object_id(|available_object_ids| available_object_ids.get_next_object_id())
            .await
            .map(Into::into)
    }
}

struct SharedComapctorObjectIdManagerCore {
    output_object_ids: VecDeque<HummockSstableObjectId>,
    client: Option<GrpcCompactorProxyClient>,
    sstable_id_remote_fetch_number: u32,
}
impl SharedComapctorObjectIdManagerCore {
    pub fn new(
        output_object_ids: VecDeque<HummockSstableObjectId>,
        client: GrpcCompactorProxyClient,
        sstable_id_remote_fetch_number: u32,
    ) -> Self {
        Self {
            output_object_ids,
            client: Some(client),
            sstable_id_remote_fetch_number,
        }
    }

    pub fn for_test(output_object_ids: VecDeque<u64>) -> Self {
        Self {
            output_object_ids: output_object_ids.into_iter().map(|x| x.into()).collect(),
            client: None,
            sstable_id_remote_fetch_number: 0,
        }
    }
}
/// `SharedComapctorObjectIdManager` is used to get output sst id for serverless compaction.
pub struct SharedComapctorObjectIdManager {
    core: tokio::sync::Mutex<SharedComapctorObjectIdManagerCore>,
}

impl SharedComapctorObjectIdManager {
    pub fn new(
        output_object_ids: VecDeque<HummockSstableObjectId>,
        client: GrpcCompactorProxyClient,
        sstable_id_remote_fetch_number: u32,
    ) -> Arc<Self> {
        Arc::new(Self {
            core: tokio::sync::Mutex::new(SharedComapctorObjectIdManagerCore::new(
                output_object_ids,
                client,
                sstable_id_remote_fetch_number,
            )),
        })
    }

    pub fn for_test(output_object_ids: VecDeque<u64>) -> Arc<Self> {
        Arc::new(Self {
            core: tokio::sync::Mutex::new(SharedComapctorObjectIdManagerCore::for_test(
                output_object_ids,
            )),
        })
    }
}

#[async_trait::async_trait]
impl GetObjectId for SharedComapctorObjectIdManager {
    async fn get_new_sst_object_id(&self) -> HummockResult<HummockSstableObjectId> {
        let mut guard = self.core.lock().await;
        let core = guard.deref_mut();

        if let Some(first_element) = core.output_object_ids.pop_front() {
            Ok(first_element)
        } else {
            tracing::warn!(
                "The pre-allocated object ids are used up, and new object id are obtained through RPC."
            );
            let request = GetNewObjectIdsRequest {
                number: core.sstable_id_remote_fetch_number,
            };
            match core
                .client
                .as_mut()
                .expect("GrpcCompactorProxyClient is None")
                .get_new_sst_ids(request)
                .await
            {
                Ok(response) => {
                    let resp = response.into_inner();
                    let start_id = resp.start_id;
                    core.output_object_ids.extend(
                        ((start_id + 1)..resp.end_id).map(Into::<HummockSstableObjectId>::into),
                    );
                    Ok(start_id.into())
                }
                Err(e) => Err(HummockError::other(format!(
                    "Fail to get new sst id: {}",
                    e.as_report()
                ))),
            }
        }
    }
}
