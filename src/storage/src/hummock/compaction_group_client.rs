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
use risingwave_hummock_sdk::compaction_group::Prefix;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionGroup;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::RwLock;

use crate::hummock::{HummockError, HummockResult};

/// `CompactionGroupClient` maintains compaction group metadata cache.
pub struct CompactionGroupClient {
    inner: RwLock<CompactionGroupClientInner>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl CompactionGroupClient {
    pub fn new(hummock_meta_client: Arc<dyn HummockMetaClient>) -> Self {
        Self {
            inner: Default::default(),
            hummock_meta_client,
        }
    }

    /// Tries to get from local cache
    pub async fn try_ger_compaction_group_id(
        &self,
        prefix: Prefix,
    ) -> HummockResult<Option<CompactionGroupId>> {
        if let Some(compaction_group_id) = self.inner.read().await.get(&prefix) {
            return Ok(Some(compaction_group_id));
        }
        Ok(None)
    }

    /// Tries to get from meta service
    pub async fn get_compaction_group_id(
        &self,
        prefix: Prefix,
    ) -> HummockResult<Option<CompactionGroupId>> {
        let mut guard = self.inner.write().await;
        if let Some(compaction_group_id) = guard.get(&prefix) {
            return Ok(Some(compaction_group_id));
        }
        let compaction_groups = self
            .hummock_meta_client
            .get_compaction_groups()
            .await
            .map_err(HummockError::meta_error)?;
        guard.set_index(compaction_groups);
        Ok(guard.get(&prefix))
    }
}

#[derive(Default)]
struct CompactionGroupClientInner {
    index: HashMap<Prefix, CompactionGroupId>,
}

impl CompactionGroupClientInner {
    fn get(&self, prefix: &Prefix) -> Option<CompactionGroupId> {
        self.index.get(prefix).cloned()
    }

    fn set_index(&mut self, compaction_groups: Vec<CompactionGroup>) {
        self.index.clear();
        let new_entries = compaction_groups
            .into_iter()
            .flat_map(|cg| {
                cg.member_prefixes
                    .into_iter()
                    .map(|prefix| (cg.id, prefix))
                    .collect_vec()
            })
            .collect_vec();
        for (cg_id, prefix) in new_entries {
            let prefix: [u8; 4] = prefix.try_into().expect("invalid prefix");
            self.index.insert(Prefix::from(prefix), cg_id);
        }
    }
}
