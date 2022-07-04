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
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::{HummockError, HummockResult};

#[async_trait::async_trait]
pub trait CompactionGroupClient: Send + Sync + 'static {
    /// Updates local cache
    async fn update(&self) -> HummockResult<()>;
    /// Tries to get from local cache
    fn get_compaction_group_id(&self, table_id: StateTableId) -> Option<CompactionGroupId>;
}

/// `CompactionGroupClientImpl` maintains compaction group metadata cache.
pub struct CompactionGroupClientImpl {
    inner: RwLock<CompactionGroupClientInner>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl CompactionGroupClientImpl {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>) -> Self {
        Self {
            inner: Default::default(),
            hummock_meta_client,
        }
    }
}

#[async_trait::async_trait]
impl CompactionGroupClient for CompactionGroupClientImpl {
    async fn update(&self) -> HummockResult<()> {
        let compaction_groups = self
            .hummock_meta_client
            .get_compaction_groups()
            .await
            .map_err(HummockError::meta_error)?;
        let mut guard = self.inner.write();
        guard.set_index(compaction_groups);
        Ok(())
    }

    fn get_compaction_group_id(&self, table_id: StateTableId) -> Option<CompactionGroupId> {
        self.inner.read().get(&table_id)
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

#[async_trait::async_trait]
impl CompactionGroupClient for DummyCompactionGroupClient {
    async fn update(&self) -> HummockResult<()> {
        Ok(())
    }

    fn get_compaction_group_id(&self, _table_id: StateTableId) -> Option<CompactionGroupId> {
        Some(self.compaction_group_id)
    }
}
