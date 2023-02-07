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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::DerefMut;

use function_name::named;
use itertools::Itertools;
use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::CompactionConfig;
use tokio::sync::{OnceCell, RwLock};

use super::versioning::Versioning;
use super::write_lock;
use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction_group::CompactionGroup;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::HummockManager;
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{BTreeMapTransaction, MetadataModel, TableFragments, ValTransaction};
use crate::storage::{MetaStore, Transaction};

impl<S: MetaStore> HummockManager<S> {
    pub(super) async fn build_compaction_group_manager(
        env: &MetaSrvEnv<S>,
    ) -> Result<RwLock<CompactionGroupManagerInner<S>>> {
        let config = CompactionConfigBuilder::new().build();
        Self::build_compaction_group_manager_with_config(env, config).await
    }

    pub(super) async fn build_compaction_group_manager_with_config(
        env: &MetaSrvEnv<S>,
        config: CompactionConfig,
    ) -> Result<RwLock<CompactionGroupManagerInner<S>>> {
        let id_generator_ref = env.id_gen_manager_ref();
        let compaction_group_manager = RwLock::new(CompactionGroupManagerInner {
            id_generator_ref,
            compaction_groups: BTreeMap::new(),
            index: BTreeMap::new(),
        });
        compaction_group_manager
            .write()
            .await
            .init(&config, env.meta_store())
            .await?;
        Ok(compaction_group_manager)
    }

    pub async fn compaction_groups(&self) -> Vec<CompactionGroup> {
        self.compaction_group_manager
            .read()
            .await
            .compaction_groups
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn compaction_groups_and_index(
        &self,
    ) -> (
        Vec<CompactionGroup>,
        BTreeMap<StateTableId, CompactionGroupId>,
    ) {
        let compaction_group_manager = self.compaction_group_manager.read().await;
        (
            compaction_group_manager
                .compaction_groups
                .values()
                .cloned()
                .collect_vec(),
            compaction_group_manager.index.clone(),
        )
    }

    pub async fn compaction_group_ids(&self) -> Vec<CompactionGroupId> {
        self.compaction_group_manager
            .read()
            .await
            .compaction_groups
            .values()
            .map(|cg| cg.group_id)
            .collect_vec()
    }

    pub async fn compaction_group(&self, id: CompactionGroupId) -> Option<CompactionGroup> {
        self.compaction_group_manager
            .read()
            .await
            .compaction_groups
            .get(&id)
            .cloned()
    }

    /// Registers `table_fragments` to compaction groups.
    pub async fn register_table_fragments(
        &self,
        table_fragments: &TableFragments,
        table_properties: &HashMap<String, String>,
    ) -> Result<Vec<StateTableId>> {
        let is_independent_compaction_group = table_properties
            .get("independent_compaction_group")
            .map(|s| s == "1")
            == Some(true);
        let table_option = TableOption::build_table_option(table_properties);
        let mut pairs = vec![];
        // materialized_view
        pairs.push((
            table_fragments.table_id().table_id,
            if is_independent_compaction_group {
                CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
            } else {
                CompactionGroupId::from(StaticCompactionGroupId::MaterializedView)
            },
            table_option,
        ));
        // internal states
        for table_id in table_fragments.internal_table_ids() {
            assert_ne!(table_id, table_fragments.table_id().table_id);
            pairs.push((
                table_id,
                if is_independent_compaction_group {
                    CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
                } else {
                    CompactionGroupId::from(StaticCompactionGroupId::StateDefault)
                },
                table_option,
            ));
        }
        self.register_table_ids(&mut pairs).await
    }

    /// Unregisters `table_fragments` from compaction groups
    pub async fn unregister_table_fragments(&self, table_fragments: &TableFragments) -> Result<()> {
        self.unregister_table_ids(&table_fragments.all_table_ids().collect_vec())
            .await
    }

    /// Unregisters stale members and groups
    pub async fn purge_stale(&self, table_fragments_list: &[TableFragments]) -> Result<()> {
        {
            let mut guard = self.compaction_group_manager.write().await;
            let registered_members = guard
                .compaction_groups
                .values()
                .flat_map(|cg| cg.member_table_ids.iter())
                .cloned()
                .collect_vec();
            let valid_ids = table_fragments_list
                .iter()
                .flat_map(|table_fragments| table_fragments.all_table_ids())
                .collect_vec();
            let to_unregister = registered_members
                .into_iter()
                .filter(|table_id| !valid_ids.contains(table_id))
                .collect_vec();
            guard
                .unregister(None, &to_unregister, self.env.meta_store())
                .await?;
        }
        self.purge_stale_groups().await
    }

    pub async fn internal_table_ids_by_compaction_group_id(
        &self,
        compaction_group_id: u64,
    ) -> Result<HashSet<StateTableId>> {
        let compaction_group_manager = self.compaction_group_manager.read().await;
        let table_id_set =
            compaction_group_manager.table_ids_by_compaction_group_id(compaction_group_id)?;
        Ok(table_id_set)
    }

    #[named]
    pub async fn register_table_ids(
        &self,
        pairs: &mut [(StateTableId, CompactionGroupId, TableOption)],
    ) -> Result<Vec<StateTableId>> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        self.compaction_group_manager
            .write()
            .await
            .register(self, versioning, pairs, self.env.meta_store())
            .await
    }

    #[named]
    pub async fn register_new_group(
        &self,
        group_id: CompactionGroupId,
        group_config: CompactionConfig,
        tables: &[(StateTableId, TableOption)],
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        self.compaction_group_manager
            .write()
            .await
            .register_new_group(
                self,
                versioning,
                group_id,
                group_config,
                tables,
                self.env.meta_store(),
            )
            .await
    }

    #[named]
    pub async fn remove_group_by_id(&self, group_id: CompactionGroupId) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        self.compaction_group_manager
            .write()
            .await
            .remove_group_by_id(self, versioning, group_id, self.env.meta_store())
            .await
    }

    #[named]
    pub async fn unregister_table_ids(&self, table_ids: &[StateTableId]) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        self.compaction_group_manager
            .write()
            .await
            .unregister(Some((self, versioning)), table_ids, self.env.meta_store())
            .await
    }

    #[named]
    async fn purge_stale_groups(&self) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let mut guard = self.compaction_group_manager.write().await;
        guard
            .purge_stale_groups(self, versioning, self.env.meta_store())
            .await
    }

    pub async fn get_table_option(
        &self,
        id: CompactionGroupId,
        table_id: u32,
    ) -> Result<TableOption> {
        let compaction_group_manager = self.compaction_group_manager.read().await;
        compaction_group_manager.table_option_by_table_id(id, table_id)
    }

    pub async fn all_table_ids(&self) -> HashSet<StateTableId> {
        let compaction_group_manager = self.compaction_group_manager.read().await;
        compaction_group_manager.all_table_ids()
    }

    pub async fn update_compaction_config(
        &self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
    ) -> Result<()> {
        self.compaction_group_manager
            .write()
            .await
            .update_compaction_config(
                compaction_group_ids,
                config_to_update,
                self.env.meta_store(),
            )
            .await
    }
}

pub(super) struct CompactionGroupManagerInner<S: MetaStore> {
    id_generator_ref: IdGeneratorManagerRef<S>,
    compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup>,
    index: BTreeMap<StateTableId, CompactionGroupId>,
}

impl<S: MetaStore> CompactionGroupManagerInner<S> {
    async fn init(&mut self, config: &CompactionConfig, meta_store: &S) -> Result<()> {
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
            let mut new_compaction_groups = BTreeMapTransaction::new(compaction_groups);
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
            for member in &compaction_group.member_table_ids {
                assert!(self.index.insert(*member, *id).is_none());
            }
        }

        Ok(())
    }

    fn gen_compaction_group_snapshot(
        compaction_groups: &BTreeMapTransaction<'_, CompactionGroupId, CompactionGroup>,
        compaction_group_id_set: Vec<CompactionGroupId>,
    ) -> HashMap<CompactionGroupId, CompactionGroup> {
        compaction_group_id_set
            .into_iter()
            .filter_map(|group_id| {
                compaction_groups
                    .get(&group_id)
                    .map(|group| (group_id, group.clone()))
            })
            .collect()
    }

    async fn register(
        &mut self,
        hummock_manager: &HummockManager<S>,
        versioning: &mut Versioning,
        pairs: &mut [(StateTableId, CompactionGroupId, TableOption)],
        meta_store: &S,
    ) -> Result<Vec<StateTableId>> {
        for (table_id, new_compaction_group_id, _) in pairs.iter() {
            if let Some(old_compaction_group_id) = self.index.get(table_id) {
                if old_compaction_group_id != new_compaction_group_id {
                    return Err(Error::InvalidCompactionGroupMember(*table_id));
                }
            }
        }
        // All NewCompactionGroup pairs are mapped to one new compaction group.
        let new_compaction_group_id: OnceCell<CompactionGroupId> = OnceCell::new();
        let mut compaction_group_id_set = self.compaction_groups.keys().cloned().collect_vec();
        let old_id_cnt = compaction_group_id_set.len();
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        for (table_id, compaction_group_id, table_option) in pairs.iter_mut() {
            let mut compaction_group =
                if *compaction_group_id == StaticCompactionGroupId::NewCompactionGroup as u64 {
                    *compaction_group_id = *new_compaction_group_id
                        .get_or_try_init(|| async {
                            self.id_generator_ref
                                .generate::<{ IdCategory::CompactionGroup }>()
                                .await
                                .map(|new_id| {
                                    compaction_group_id_set.push(new_id);
                                    compaction_groups.insert(
                                        new_id,
                                        CompactionGroup::new(
                                            new_id,
                                            CompactionConfigBuilder::new().build(),
                                        ),
                                    );
                                    new_id
                                })
                        })
                        .await?;
                    compaction_groups.get_mut(*compaction_group_id).unwrap()
                } else {
                    compaction_groups
                        .get_mut(*compaction_group_id)
                        .ok_or(Error::InvalidCompactionGroup(*compaction_group_id))?
                };
            compaction_group.member_table_ids.insert(*table_id);
            compaction_group
                .table_id_to_options
                .insert(*table_id, *table_option);
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        let mut trx_wrapper = Some(trx);
        if compaction_group_id_set.len() > old_id_cnt {
            hummock_manager
                .sync_group(
                    None,
                    versioning,
                    &Self::gen_compaction_group_snapshot(
                        &compaction_groups,
                        compaction_group_id_set,
                    ),
                    &mut trx_wrapper,
                )
                .await?;
        }
        if let Some(trx) = trx_wrapper.take() {
            meta_store.txn(trx).await?;
        }
        compaction_groups.commit();

        // Update in-memory index
        for (table_id, compaction_group_id, _) in pairs.iter() {
            self.index.insert(*table_id, *compaction_group_id);
        }
        Ok(pairs.iter().map(|(table_id, ..)| *table_id).collect_vec())
    }

    async fn unregister(
        &mut self,
        empty_group_vacuum_context: Option<(&HummockManager<S>, &mut Versioning)>,
        table_ids: &[StateTableId],
        meta_store: &S,
    ) -> Result<()> {
        let compaction_group_id_set = self.compaction_groups.keys().cloned().collect_vec();
        let mut remove_some_group = false;
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        for table_id in table_ids {
            let compaction_group_id = match self.index.get(table_id).cloned() {
                None => {
                    continue;
                }
                Some(id) => id,
            };
            let mut compaction_group = compaction_groups
                .get_mut(compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?;
            compaction_group.member_table_ids.remove(table_id);
            compaction_group.table_id_to_options.remove(table_id);
            if empty_group_vacuum_context.is_some()
                && compaction_group_id > StaticCompactionGroupId::End as CompactionGroupId
                && compaction_group.member_table_ids.is_empty()
            {
                remove_some_group = true;
                compaction_groups.remove(compaction_group_id);
            }
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        let mut trx_wrapper = Some(trx);
        if remove_some_group {
            let (hummock_manager, versioning) = empty_group_vacuum_context.unwrap();
            hummock_manager
                .sync_group(
                    None,
                    versioning,
                    &Self::gen_compaction_group_snapshot(
                        &compaction_groups,
                        compaction_group_id_set,
                    ),
                    &mut trx_wrapper,
                )
                .await?;
        }
        if let Some(trx) = trx_wrapper.take() {
            meta_store.txn(trx).await?;
        }
        compaction_groups.commit();

        // Update in-memory index
        for table_id in table_ids {
            self.index.remove(table_id);
        }
        Ok(())
    }

    async fn register_new_group(
        &mut self,
        hummock_manager: &HummockManager<S>,
        versioning: &mut Versioning,
        group_id: CompactionGroupId,
        group_config: CompactionConfig,
        tables: &[(StateTableId, TableOption)],
        meta_store: &S,
    ) -> Result<()> {
        let mut compaction_group_id_set = self.compaction_groups.keys().cloned().collect_vec();
        let old_id_cnt = compaction_group_id_set.len();
        let is_exist = self.compaction_groups.contains_key(&group_id);
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        let compaction_group_id = if is_exist {
            group_id
        } else {
            let id = if group_id == StaticCompactionGroupId::NewCompactionGroup as u64 {
                self.id_generator_ref
                    .generate::<{ IdCategory::CompactionGroup }>()
                    .await?
            } else {
                group_id
            };
            compaction_group_id_set.push(id);
            compaction_groups.insert(
                id,
                CompactionGroup::new(
                    id,
                    CompactionConfigBuilder::with_config(group_config).build(),
                ),
            );
            id
        };

        let mut compaction_group = compaction_groups.get_mut(compaction_group_id).unwrap();
        for (table_id, table_option) in tables.iter() {
            compaction_group.member_table_ids.insert(*table_id);
            compaction_group
                .table_id_to_options
                .insert(*table_id, *table_option);
        }

        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        let mut trx_wrapper = Some(trx);
        if compaction_group_id_set.len() > old_id_cnt {
            hummock_manager
                .sync_group(
                    None,
                    versioning,
                    &Self::gen_compaction_group_snapshot(
                        &compaction_groups,
                        compaction_group_id_set,
                    ),
                    &mut trx_wrapper,
                )
                .await?;
        }
        if let Some(trx) = trx_wrapper.take() {
            meta_store.txn(trx).await?;
        }
        compaction_groups.commit();

        // Update in-memory index
        for (table_id, _) in tables.iter() {
            self.index.insert(*table_id, compaction_group_id);
        }

        let table_ids = tables
            .iter()
            .map(|(table_id, _)| table_id)
            .cloned()
            .collect_vec();
        tracing::info!(
            "Compaction group {}, registered table ids {:?}",
            compaction_group_id,
            table_ids
        );
        Ok(())
    }

    async fn remove_group_by_id(
        &mut self,
        hummock_manager: &HummockManager<S>,
        versioning: &mut Versioning,
        group_id: CompactionGroupId,
        meta_store: &S,
    ) -> Result<()> {
        let compaction_group_id_set = self.compaction_groups.keys().cloned().collect_vec();
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        let removed_group = compaction_groups.remove(group_id);
        if let Some(group) = removed_group {
            let mut trx = Transaction::default();
            compaction_groups.apply_to_txn(&mut trx)?;
            let mut trx_wrapper = Some(trx);
            hummock_manager
                .sync_group(
                    None,
                    versioning,
                    &Self::gen_compaction_group_snapshot(
                        &compaction_groups,
                        compaction_group_id_set,
                    ),
                    &mut trx_wrapper,
                )
                .await?;
            if let Some(trx) = trx_wrapper.take() {
                meta_store.txn(trx).await?;
            }
            compaction_groups.commit();
            // Update index
            for table_id in group.member_table_ids() {
                self.index.remove(table_id);
            }
        }
        Ok(())
    }

    async fn purge_stale_groups(
        &mut self,
        hummock_manager: &HummockManager<S>,
        versioning: &mut Versioning,
        meta_store: &S,
    ) -> Result<()> {
        let mut remove_some_group = false;
        let compaction_group_ids = self.compaction_groups.keys().cloned().collect_vec();
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        for compaction_group_id in &compaction_group_ids {
            let compaction_group = compaction_groups
                .get_mut(*compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(*compaction_group_id))?;
            if *compaction_group_id > StaticCompactionGroupId::End as CompactionGroupId
                && compaction_group.member_table_ids.is_empty()
            {
                remove_some_group = true;
                compaction_groups.remove(*compaction_group_id);
            }
        }
        if remove_some_group {
            let mut trx = Transaction::default();
            compaction_groups.apply_to_txn(&mut trx)?;
            let mut trx_wrapper = Some(trx);
            hummock_manager
                .sync_group(
                    None,
                    versioning,
                    &Self::gen_compaction_group_snapshot(&compaction_groups, compaction_group_ids),
                    &mut trx_wrapper,
                )
                .await?;
            if let Some(trx) = trx_wrapper.take() {
                meta_store.txn(trx).await?;
            }
            compaction_groups.commit();
        }
        Ok(())
    }

    fn compaction_group(&self, compaction_group_id: u64) -> Result<&CompactionGroup> {
        match self.compaction_groups.get(&compaction_group_id) {
            Some(compaction_group) => Ok(compaction_group),

            None => Err(Error::InvalidCompactionGroup(compaction_group_id)),
        }
    }

    pub fn table_ids_by_compaction_group_id(
        &self,
        compaction_group_id: u64,
    ) -> Result<HashSet<StateTableId>> {
        let compaction_group = self.compaction_group(compaction_group_id)?;
        Ok(compaction_group.member_table_ids.clone())
    }

    pub fn table_option_by_table_id(
        &self,
        compaction_group_id: u64,
        table_id: u32,
    ) -> Result<TableOption> {
        let compaction_group = self.compaction_group(compaction_group_id)?;
        match compaction_group.table_id_to_options().get(&table_id) {
            Some(table_option) => Ok(*table_option),

            None => Ok(TableOption::default()),
        }
    }

    fn all_table_ids(&self) -> HashSet<StateTableId> {
        self.index.keys().cloned().collect()
    }

    async fn update_compaction_config(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
        meta_store: &S,
    ) -> Result<()> {
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        for compaction_group_id in compaction_group_ids {
            if let Some(mut group) = compaction_groups.get_mut(*compaction_group_id) {
                let config = &mut group.compaction_config;
                update_compaction_config(config, config_to_update);
            }
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        meta_store.txn(trx).await?;
        compaction_groups.commit();
        Ok(())
    }
}

fn update_compaction_config(target: &mut CompactionConfig, items: &[MutableConfig]) {
    for item in items {
        match item {
            MutableConfig::MaxBytesForLevelBase(c) => {
                target.max_bytes_for_level_base = *c;
            }
            MutableConfig::MaxBytesForLevelMultiplier(c) => {
                target.max_bytes_for_level_multiplier = *c;
            }
            MutableConfig::MaxCompactionBytes(c) => {
                target.max_compaction_bytes = *c;
            }
            MutableConfig::SubLevelMaxCompactionBytes(c) => {
                target.sub_level_max_compaction_bytes = *c;
            }
            MutableConfig::Level0TierCompactFileNumber(c) => {
                target.level0_tier_compact_file_number = *c;
            }
            MutableConfig::TargetFileSizeBase(c) => {
                target.target_file_size_base = *c;
            }
            MutableConfig::CompactionFilterMask(c) => {
                target.compaction_filter_mask = *c;
            }
            MutableConfig::MaxSubCompaction(c) => {
                target.max_sub_compaction = *c;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::ops::Deref;

    use risingwave_common::catalog::{TableId, TableOption};
    use risingwave_common::constants::hummock::PROPERTIES_RETENTION_SECOND_KEY;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::StreamEnvironment;

    use crate::hummock::manager::compaction_group_manager::CompactionGroupManagerInner;
    use crate::hummock::manager::versioning::Versioning;
    use crate::hummock::test_utils::setup_compute_env;
    use crate::hummock::HummockManager;
    use crate::model::TableFragments;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_inner() {
        let (env, hummock_manager_ref, ..) = setup_compute_env(8080).await;
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();

        let registered_number = |inner: &CompactionGroupManagerInner<MemStore>| {
            inner
                .compaction_groups
                .values()
                .map(|cg| cg.member_table_ids.len())
                .sum::<usize>()
        };

        let table_option_number = |inner: &CompactionGroupManagerInner<MemStore>| {
            inner
                .compaction_groups
                .values()
                .map(|cg| cg.table_id_to_options().len())
                .sum::<usize>()
        };

        assert!(inner.read().await.index.is_empty());
        assert_eq!(registered_number(inner.read().await.deref()), 0);

        let table_properties = HashMap::from([(
            String::from(PROPERTIES_RETENTION_SECOND_KEY),
            String::from("300"),
        )]);
        let table_option = TableOption::build_table_option(&table_properties);

        // Test register
        inner
            .write()
            .await
            .register(
                &hummock_manager_ref,
                &mut Versioning::default(),
                &mut [(
                    1u32,
                    StaticCompactionGroupId::StateDefault.into(),
                    table_option,
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        inner
            .write()
            .await
            .register(
                &hummock_manager_ref,
                &mut Versioning::default(),
                &mut [(
                    2u32,
                    StaticCompactionGroupId::MaterializedView.into(),
                    table_option,
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);

        // Test init
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);
        assert_eq!(table_option_number(inner.read().await.deref()), 2);

        // Test unregister
        inner
            .write()
            .await
            .unregister(
                Some((&hummock_manager_ref, &mut Versioning::default())),
                &[2u32],
                env.meta_store(),
            )
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);
        assert_eq!(table_option_number(inner.read().await.deref()), 1);

        // Test init
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);
        assert_eq!(table_option_number(inner.read().await.deref()), 1);

        // Test table_option_by_table_id
        {
            let table_option = inner
                .read()
                .await
                .table_option_by_table_id(StaticCompactionGroupId::StateDefault.into(), 1u32)
                .unwrap();
            assert_eq!(300, table_option.retention_seconds.unwrap());
        }

        {
            // unregistered table_id
            let table_option_default = inner
                .read()
                .await
                .table_option_by_table_id(StaticCompactionGroupId::StateDefault.into(), 2u32);
            assert!(table_option_default.is_ok());
            assert_eq!(None, table_option_default.unwrap().retention_seconds);
        }
    }

    #[tokio::test]
    async fn test_manager() {
        let (_, compaction_group_manager, ..) = setup_compute_env(8080).await;
        let table_fragment_1 = TableFragments::new(
            TableId::new(10),
            BTreeMap::from([(
                1,
                Fragment {
                    fragment_id: 1,
                    state_table_ids: vec![10, 11, 12, 13],
                    ..Default::default()
                },
            )]),
            StreamEnvironment::default(),
        );
        let table_fragment_2 = TableFragments::new(
            TableId::new(20),
            BTreeMap::from([(
                2,
                Fragment {
                    fragment_id: 2,
                    state_table_ids: vec![20, 21, 22, 23],
                    ..Default::default()
                },
            )]),
            StreamEnvironment::default(),
        );

        // Test register_table_fragments
        let registered_number = || async {
            compaction_group_manager
                .compaction_groups()
                .await
                .iter()
                .map(|cg| cg.member_table_ids.len())
                .sum::<usize>()
        };
        let group_number = || async { compaction_group_manager.compaction_groups().await.len() };
        assert_eq!(registered_number().await, 0);
        let mut table_properties = HashMap::from([(
            String::from(PROPERTIES_RETENTION_SECOND_KEY),
            String::from("300"),
        )]);

        compaction_group_manager
            .register_table_fragments(&table_fragment_1, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .register_table_fragments(&table_fragment_2, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 8);

        // Test unregister_table_fragments
        compaction_group_manager
            .unregister_table_fragments(&table_fragment_1)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);

        // Test purge_stale_members: table fragments
        compaction_group_manager
            .purge_stale(&[table_fragment_2])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager.purge_stale(&[]).await.unwrap();
        assert_eq!(registered_number().await, 0);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `register_table_fragments`
        assert_eq!(group_number().await, 2);
        table_properties.insert(
            String::from("independent_compaction_group"),
            String::from("1"),
        );
        compaction_group_manager
            .register_table_fragments(&table_fragment_1, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        assert_eq!(group_number().await, 3);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `unregister_table_fragments`
        compaction_group_manager
            .unregister_table_fragments(&table_fragment_1)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 0);
        assert_eq!(group_number().await, 2);
    }
}
