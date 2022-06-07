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

use std::collections::BTreeMap;
use std::ops::DerefMut;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;
use tokio::sync::RwLock;

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction_group::CompactionGroup;
use crate::hummock::error::Result;
use crate::manager::MetaSrvEnv;
use crate::model::{MetadataModel, ValTransaction, VarTransaction};
use crate::storage::{MetaStore, Transaction};

pub type CompactionGroupManagerRef<S> = Arc<CompactionGroupManager<S>>;

pub struct CompactionGroupManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    compaction_groups: RwLock<BTreeMap<CompactionGroupId, CompactionGroup>>,
    config: CompactionConfig,
}

impl<S: MetaStore> CompactionGroupManager<S> {
    pub async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let config = CompactionConfigBuilder::new().build();
        Self::new_with_config(env, config).await
    }

    pub async fn new_with_config(env: MetaSrvEnv<S>, config: CompactionConfig) -> Result<Self> {
        let instance = Self {
            env,
            compaction_groups: RwLock::new(Default::default()),
            config: config.clone(),
        };
        instance.init(config).await?;
        Ok(instance)
    }

    async fn init(&self, config: CompactionConfig) -> Result<()> {
        let mut guard = self.compaction_groups.write().await;
        let compaction_groups = guard.deref_mut();
        let loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup> =
            CompactionGroup::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|cg| (cg.group_id(), cg))
                .collect();
        if !loaded_compaction_groups.is_empty() {
            *compaction_groups = loaded_compaction_groups;
        } else {
            let mut new_compaction_groups = VarTransaction::new(compaction_groups);
            let static_compaction_groups = vec![
                CompactionGroup::new(StaticCompactionGroupId::StateDefault.into(), config.clone()),
                CompactionGroup::new(
                    StaticCompactionGroupId::MaterializedView.into(),
                    config.clone(),
                ),
            ];
            for static_compaction_group in static_compaction_groups {
                new_compaction_groups
                    .insert(static_compaction_group.group_id(), static_compaction_group);
            }
            let mut trx = Transaction::default();
            new_compaction_groups.apply_to_txn(&mut trx)?;
            self.env.meta_store().txn(trx).await?;
            new_compaction_groups.commit();
        }
        Ok(())
    }

    pub async fn compaction_groups(&self) -> Vec<CompactionGroup> {
        self.compaction_groups
            .read()
            .await
            .values()
            .cloned()
            .collect_vec()
    }

    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }
}
