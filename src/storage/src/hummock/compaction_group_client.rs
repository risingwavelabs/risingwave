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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::oneshot;

use crate::hummock::{HummockError, HummockResult};

pub enum CompactionGroupClientImpl {
    Meta(Arc<MetaCompactionGroupClient>),
    Dummy(DummyCompactionGroupClient),
}

impl CompactionGroupClientImpl {
    pub async fn get_compaction_group_id(
        &self,
        table_id: StateTableId,
    ) -> HummockResult<CompactionGroupId> {
        match self {
            CompactionGroupClientImpl::Meta(c) => c.get_compaction_group_id(table_id).await,
            CompactionGroupClientImpl::Dummy(c) => Ok(c.get_compaction_group_id()),
        }
    }

    pub fn update_by(&self, compaction_groups: Vec<CompactionGroup>) {
        match self {
            CompactionGroupClientImpl::Meta(c) => c.update_by(compaction_groups),
            CompactionGroupClientImpl::Dummy(_) => (),
        }
    }
}

/// `CompactionGroupClientImpl` maintains compaction group metadata cache.
pub struct MetaCompactionGroupClient {
    // Lock order: wait_queue before cache
    wait_queue: Mutex<Option<Vec<oneshot::Sender<bool>>>>,
    cache: RwLock<CompactionGroupClientInner>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl MetaCompactionGroupClient {
    /// TODO: cache is synced on need currently. We can refactor it to push based after #3679.
    async fn get_compaction_group_id(
        self: &Arc<Self>,
        table_id: StateTableId,
    ) -> HummockResult<CompactionGroupId> {
        // We wait for cache update for at most twice.
        // For the first time there may already be an inflight RPC when cache miss, whose response
        // may not contain wanted cache entry. For the second time the new RPC must contain
        // wanted cache entry, no matter the RPC is fired by this task or other. Otherwise,
        // the caller is trying to get an inexistent cache entry, which indicates a bug.
        let mut wait_counter = 0;
        while wait_counter <= 2 {
            // 1. Get from cache
            if let Some(id) = self.cache.read().get(&table_id) {
                return Ok(id);
            }
            // 2. Otherwise either update cache, or wait for previous update if any.
            let waiter = {
                let mut guard = self.wait_queue.lock();
                if let Some(id) = self.cache.read().get(&table_id) {
                    return Ok(id);
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
                // Wait for previous update
                if let Ok(success) = waiter.await && success {
                    wait_counter += 1;
                }
                continue;
            }
            // Update cache
            let this = self.clone();
            tokio::spawn(async move {
                let result = this.update().await;
                let mut guard = this.wait_queue.lock();
                let wait_queue = guard.deref_mut().take().unwrap();
                for notify in wait_queue {
                    let _ = notify.send(result.is_ok());
                }
                result
            })
            .await
            .unwrap()?;
            wait_counter += 1;
        }
        Err(HummockError::compaction_group_error(format!(
            "compaction group not found for table id {}",
            table_id
        )))
    }
}

impl MetaCompactionGroupClient {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>) -> Self {
        Self {
            wait_queue: Default::default(),
            cache: Default::default(),
            hummock_meta_client,
        }
    }

    async fn update(&self) -> HummockResult<()> {
        let compaction_groups = self
            .hummock_meta_client
            .get_compaction_groups()
            .await
            .map_err(HummockError::meta_error)?;
        let mut guard = self.cache.write();
        guard.supply_index(compaction_groups, true);
        Ok(())
    }

    fn update_by(&self, compaction_groups: Vec<CompactionGroup>) {
        let mut guard = self.cache.write();
        guard.supply_index(compaction_groups, false);
    }
}

#[derive(Default)]
struct CompactionGroupClientInner {
    index: HashMap<StateTableId, CompactionGroupId>,
}

impl CompactionGroupClientInner {
    fn get(&self, table_id: &StateTableId) -> Option<CompactionGroupId> {
        self.index.get(table_id).cloned()
    }

    fn update_member_ids(
        &mut self,
        member_ids: &[StateTableId],
        is_pull: bool,
        cg_id: CompactionGroupId,
    ) {
        for table_id in member_ids {
            match self.index.entry(*table_id) {
                Entry::Occupied(mut entry) => {
                    if !is_pull {
                        entry.insert(cg_id);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(cg_id);
                }
            }
        }
    }

    fn supply_index(&mut self, compaction_groups: Vec<CompactionGroup>, is_pull: bool) {
        for compaction_group in compaction_groups {
            let member_ids = compaction_group.get_member_table_ids();
            self.update_member_ids(member_ids, is_pull, compaction_group.get_id());
        }
    }
}

pub struct DummyCompactionGroupClient {
    /// Always return this `compaction_group_id`.
    compaction_group_id: CompactionGroupId,
}

impl DummyCompactionGroupClient {
    pub fn new(compaction_group_id: CompactionGroupId) -> Self {
        Self {
            compaction_group_id,
        }
    }
}

impl DummyCompactionGroupClient {
    fn get_compaction_group_id(&self) -> CompactionGroupId {
        self.compaction_group_id
    }
}
