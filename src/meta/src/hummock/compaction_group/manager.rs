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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::{Prefix, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;
use tokio::sync::RwLock;

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction_group::CompactionGroup;
use crate::hummock::error::{Error, Result};
use crate::manager::MetaSrvEnv;
use crate::model::{MetadataModel, TableFragments, ValTransaction, VarTransaction};
use crate::storage::{MetaStore, Transaction};

pub type CompactionGroupManagerRef<S> = Arc<CompactionGroupManager<S>>;

/// `CompactionGroupManager` manages `CompactionGroup`'s members.
pub struct CompactionGroupManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    inner: RwLock<CompactionGroupManagerInner>,
}

impl<S: MetaStore> CompactionGroupManager<S> {
    pub async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let config = CompactionConfigBuilder::new().build();
        Self::new_with_config(env, config).await
    }

    pub async fn new_with_config(env: MetaSrvEnv<S>, config: CompactionConfig) -> Result<Self> {
        let instance = Self {
            env,
            inner: RwLock::new(Default::default()),
        };
        instance
            .inner
            .write()
            .await
            .init(&config, instance.env.meta_store())
            .await?;
        Ok(instance)
    }

    pub async fn compaction_groups(&self) -> Vec<CompactionGroup> {
        self.inner
            .read()
            .await
            .compaction_groups
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn compaction_group(&self, id: CompactionGroupId) -> Option<CompactionGroup> {
        self.inner.read().await.compaction_groups.get(&id).cloned()
    }

    /// Registers `table_fragments` to compaction groups.
    pub async fn register_table_fragments(&self, table_fragments: &TableFragments) -> Result<()> {
        let mut pairs = vec![];
        // MV or source
        // existing_table_ids include the table_ref_id (source and materialized_view) +
        // internal_table_id (stateful executor)
        pairs.push((
            Prefix::from(table_fragments.table_id().table_id),
            // TODO: before compaction group write path is finished, all SSTs belongs to
            // `StateDefault`.
            CompactionGroupId::from(StaticCompactionGroupId::StateDefault),
            // CompactionGroupId::from(StaticCompactionGroupId::MaterializedView),
        ));
        // internal states
        for table_id in table_fragments.internal_table_ids() {
            assert_ne!(table_id, table_fragments.table_id().table_id);
            pairs.push((
                Prefix::from(table_id),
                CompactionGroupId::from(StaticCompactionGroupId::StateDefault),
            ));
        }
        self.inner
            .write()
            .await
            .register(&pairs, self.env.meta_store())
            .await
    }

    /// Unregisters `table_fragments` from compaction groups
    pub async fn unregister_table_fragments(&self, table_fragments: &TableFragments) -> Result<()> {
        let prefixes = table_fragments
            .internal_table_ids()
            .into_iter()
            .chain(std::iter::once(table_fragments.table_id().table_id))
            .into_iter()
            .map(Prefix::from)
            .collect_vec();
        self.inner
            .write()
            .await
            .unregister(&prefixes, self.env.meta_store())
            .await
    }

    /// Unregisters members that doesn't belong to `table_fragments_list` from compaction groups
    pub async fn purge_stale_members(
        &self,
        table_fragments_list: &[&TableFragments],
    ) -> Result<()> {
        let mut guard = self.inner.write().await;
        let registered_members = guard
            .compaction_groups
            .values()
            .flat_map(|cg| cg.member_prefixes.iter())
            .collect_vec();
        let valid_ids = table_fragments_list
            .iter()
            .flat_map(|table_fragments| {
                table_fragments
                    .internal_table_ids()
                    .iter()
                    .cloned()
                    .chain(std::iter::once(table_fragments.table_id().table_id))
                    .collect_vec()
            })
            .collect_vec();
        let to_unregister = registered_members
            .into_iter()
            .filter(|prefix| !valid_ids.contains(&u32::from(*prefix)))
            .cloned()
            .collect_vec();
        guard
            .unregister(&to_unregister, self.env.meta_store())
            .await
    }

    pub async fn internal_table_ids_by_compation_group_id(
        &self,
        compaction_group_id: u64,
    ) -> Result<HashSet<u32>> {
        let inner = self.inner.read().await;
        let prefix_set = inner.prefixs_by_compaction_group_id(compaction_group_id)?;

        Ok(prefix_set.into_iter().map(u32::from).collect())
    }
}

#[derive(Default)]
struct CompactionGroupManagerInner {
    compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup>,
    index: BTreeMap<Prefix, CompactionGroupId>,
}

impl CompactionGroupManagerInner {
    async fn init<S: MetaStore>(
        &mut self,
        config: &CompactionConfig,
        meta_store: &S,
    ) -> Result<()> {
        let loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup> =
            CompactionGroup::list(meta_store)
                .await?
                .into_iter()
                .map(|cg| (cg.group_id(), cg))
                .collect();
        if !loaded_compaction_groups.is_empty() {
            self.compaction_groups = loaded_compaction_groups;
        } else {
            let compaction_groups = &mut self.compaction_groups;
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
            meta_store.txn(trx).await?;
            new_compaction_groups.commit();
        }

        // Build in-memory index
        for (id, compaction_group) in &self.compaction_groups {
            for member in &compaction_group.member_prefixes {
                assert!(self.index.insert(*member, *id).is_none());
            }
        }

        Ok(())
    }

    async fn register<S: MetaStore>(
        &mut self,
        pairs: &[(Prefix, CompactionGroupId)],
        meta_store: &S,
    ) -> Result<()> {
        let mut compaction_groups = VarTransaction::new(&mut self.compaction_groups);
        for (prefix, compaction_group_id) in pairs {
            let compaction_group = compaction_groups
                .get_mut(compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(*compaction_group_id))?;
            compaction_group.member_prefixes.insert(*prefix);
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        meta_store.txn(trx).await?;
        compaction_groups.commit();

        // Update in-memory index
        for (prefix, compaction_group_id) in pairs {
            self.index.insert(*prefix, *compaction_group_id);
        }
        Ok(())
    }

    async fn unregister<S: MetaStore>(
        &mut self,
        prefixes: &[Prefix],
        meta_store: &S,
    ) -> Result<()> {
        let mut compaction_groups = VarTransaction::new(&mut self.compaction_groups);
        for prefix in prefixes {
            let compaction_group_id = self
                .index
                .get(prefix)
                .cloned()
                .ok_or(Error::InvalidCompactionGroupMember(*prefix))?;
            let compaction_group = compaction_groups
                .get_mut(&compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?;
            compaction_group.member_prefixes.remove(prefix);
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        meta_store.txn(trx).await?;
        compaction_groups.commit();

        // Update in-memory index
        for prefix in prefixes {
            self.index.remove(prefix);
        }
        Ok(())
    }

    fn prefixs_by_compaction_group_id(&self, compaction_group_id: u64) -> Result<HashSet<Prefix>> {
        match self.compaction_groups.get(&compaction_group_id) {
            Some(compaction_group) => Ok(compaction_group.member_prefixes.clone()),

            None => Err(Error::InvalidCompactionGroup(compaction_group_id)),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Deref;

    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::compaction_group::{Prefix, StaticCompactionGroupId};

    use crate::hummock::compaction_group::manager::{
        CompactionGroupManager, CompactionGroupManagerInner,
    };
    use crate::hummock::test_utils::setup_compute_env;
    use crate::model::TableFragments;

    #[tokio::test]
    async fn test_inner() {
        let (env, ..) = setup_compute_env(8080).await;
        let compaction_group_manager = CompactionGroupManager::new(env.clone()).await.unwrap();
        let inner = compaction_group_manager.inner;

        let registered_number = |inner: &CompactionGroupManagerInner| {
            inner
                .compaction_groups
                .iter()
                .map(|(_, cg)| cg.member_prefixes.len())
                .sum::<usize>()
        };

        assert!(inner.read().await.index.is_empty());
        assert_eq!(registered_number(inner.read().await.deref()), 0);

        // Test register
        inner
            .write()
            .await
            .register(
                &[(
                    Prefix::from(1u32),
                    StaticCompactionGroupId::StateDefault.into(),
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        inner
            .write()
            .await
            .register(
                &[(
                    Prefix::from(2u32),
                    StaticCompactionGroupId::MaterializedView.into(),
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);

        // Test init
        let compaction_group_manager = CompactionGroupManager::new(env.clone()).await.unwrap();
        let inner = compaction_group_manager.inner;
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);

        // Test unregister
        inner
            .write()
            .await
            .unregister(&[Prefix::from(2u32)], env.meta_store())
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);

        // Test init
        let compaction_group_manager = CompactionGroupManager::new(env.clone()).await.unwrap();
        let inner = compaction_group_manager.inner;
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);
    }

    #[tokio::test]
    async fn test_manager() {
        let (env, ..) = setup_compute_env(8080).await;
        let compaction_group_manager = CompactionGroupManager::new(env.clone()).await.unwrap();
        let table_fragment_1 =
            TableFragments::new(TableId::new(10), Default::default(), [11, 12, 13].into());
        let table_fragment_2 =
            TableFragments::new(TableId::new(20), Default::default(), [21, 22, 23].into());

        // Test register_stream_nodes
        let registered_number = || async {
            compaction_group_manager
                .compaction_groups()
                .await
                .iter()
                .map(|cg| cg.member_prefixes.len())
                .sum::<usize>()
        };
        assert_eq!(registered_number().await, 0);
        compaction_group_manager
            .register_table_fragments(&table_fragment_1)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .register_table_fragments(&table_fragment_2)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 8);

        // Test unregister_stream_nodes
        compaction_group_manager
            .unregister_table_fragments(&table_fragment_1)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);

        // Test purge_stale_members
        compaction_group_manager
            .purge_stale_members(&[&table_fragment_2])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .purge_stale_members(&[])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 0);
    }
}
