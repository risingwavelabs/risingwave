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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Notify;

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
}

/// `CompactionGroupClientImpl` maintains compaction group metadata cache.
pub struct MetaCompactionGroupClient {
    // Lock order: update_notifier before cache
    update_notifier: parking_lot::Mutex<Option<Arc<Notify>>>,
    cache: RwLock<CompactionGroupClientInner>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl MetaCompactionGroupClient {
    /// TODO: cache is synced on need currently. We can refactor it to push based after #3679.
    async fn get_compaction_group_id(
        self: &Arc<Self>,
        table_id: StateTableId,
    ) -> HummockResult<CompactionGroupId> {
        // The loop executes at most twice.
        // For the first time there may already be an inflight RPC when cache miss, whose response
        // may not contain wanted cache entry. For the second time the new RPC must contain
        // wanted cache entry, no matter the RPC is fired by this task or other. Otherwise,
        // the caller is trying to get an inexistent cache entry, which indicates a bug.
        for _ in 0..2 {
            // 1. Get from cache
            if let Some(id) = self.cache.read().get(&table_id) {
                return Ok(id);
            }
            // 2. Otherwise either update cache, or wait for previous update if any.
            let (notify, update) = {
                let mut guard = self.update_notifier.lock();
                if let Some(id) = self.cache.read().get(&table_id) {
                    return Ok(id);
                }
                match guard.deref() {
                    None => {
                        let notify = Arc::new(Notify::new());
                        *guard = Some(notify.clone());
                        (notify, true)
                    }
                    Some(notify) => (notify.clone(), false),
                }
            };
            if !update {
                // Wait for previous update
                notify.notified().await;
                notify.notify_one();
                continue;
            }
            // Update cache
            let this = self.clone();
            tokio::spawn(async move {
                let result = this.update().await;
                this.update_notifier.lock().take().unwrap().notify_one();
                result
            })
            .await
            .unwrap()?;
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
            cache: Default::default(),
            hummock_meta_client,
            update_notifier: parking_lot::Mutex::new(None),
        }
    }

    async fn update(&self) -> HummockResult<()> {
        let compaction_groups = self
            .hummock_meta_client
            .get_compaction_groups()
            .await
            .map_err(HummockError::meta_error)?;
        let mut guard = self.cache.write();
        guard.set_index(compaction_groups);
        Ok(())
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

    fn set_index(&mut self, compaction_groups: Vec<CompactionGroup>) {
        self.index.clear();
        let new_entries = compaction_groups
            .into_iter()
            .flat_map(|cg| {
                cg.member_table_ids
                    .into_iter()
                    .map(|table_id| (cg.id, table_id))
                    .collect_vec()
            })
            .collect_vec();
        for (cg_id, table_id) in new_entries {
            self.index.insert(table_id, cg_id);
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
