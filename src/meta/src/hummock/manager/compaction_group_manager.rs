// Copyright 2024 RisingWave Labs
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
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compact_task::ReportTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    get_compaction_group_ids, split_table_ids, TableGroupInfo,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::version::{GroupDelta, GroupDeltas};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_meta_model_v2::compaction_config;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{
    compact_task, CompactionConfig, CompactionGroupInfo, CompatibilityVersion, PbGroupConstruct,
    PbGroupDestroy, PbGroupMerge, PbStateTableInfoDelta,
};
use tokio::sync::OnceCell;

use crate::hummock::compaction::compaction_config::{
    validate_compaction_config, CompactionConfigBuilder,
};
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::HummockVersionTransaction;
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::manager::{commit_multi_var, HummockManager};
use crate::hummock::metrics_utils::remove_compaction_group_in_sst_stat;
use crate::hummock::model::CompactionGroup;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_object_id};
use crate::manager::{MetaSrvEnv, MetaStoreImpl};
use crate::model::{
    BTreeMapTransaction, BTreeMapTransactionInner, DerefMutForward, MetadataModel,
    MetadataModelError,
};

type CompactionGroupTransaction<'a> = BTreeMapTransaction<'a, CompactionGroupId, CompactionGroup>;

impl CompactionGroupManager {
    pub(super) async fn new(env: &MetaSrvEnv) -> Result<CompactionGroupManager> {
        let default_config = match env.opts.compaction_config.as_ref() {
            None => CompactionConfigBuilder::new().build(),
            Some(opt) => CompactionConfigBuilder::with_opt(opt).build(),
        };
        Self::new_with_config(env, default_config).await
    }

    pub(super) async fn new_with_config(
        env: &MetaSrvEnv,
        default_config: CompactionConfig,
    ) -> Result<CompactionGroupManager> {
        let mut compaction_group_manager = CompactionGroupManager {
            compaction_groups: BTreeMap::new(),
            default_config: Arc::new(default_config),
            write_limit: Default::default(),
        };

        let loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup> =
            match env.meta_store_ref() {
                MetaStoreImpl::Kv(meta_store) => CompactionGroup::list(meta_store)
                    .await?
                    .into_iter()
                    .map(|cg| (cg.group_id(), cg))
                    .collect(),
                MetaStoreImpl::Sql(sql_meta_store) => {
                    use sea_orm::EntityTrait;
                    compaction_config::Entity::find()
                        .all(&sql_meta_store.conn)
                        .await
                        .map_err(MetadataModelError::from)?
                        .into_iter()
                        .map(|m| (m.compaction_group_id as CompactionGroupId, m.into()))
                        .collect()
                }
            };

        compaction_group_manager.init(loaded_compaction_groups);
        Ok(compaction_group_manager)
    }

    fn init(&mut self, loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup>) {
        if !loaded_compaction_groups.is_empty() {
            self.compaction_groups = loaded_compaction_groups;
        }
    }
}

impl HummockManager {
    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    /// The implementation acquires `versioning` lock.
    pub async fn compaction_group_ids(&self) -> Vec<CompactionGroupId> {
        get_compaction_group_ids(&self.versioning.read().await.current_version).collect_vec()
    }

    /// The implementation acquires `compaction_group_manager` lock.
    pub async fn get_compaction_group_map(&self) -> BTreeMap<CompactionGroupId, CompactionGroup> {
        self.compaction_group_manager
            .read()
            .await
            .compaction_groups
            .clone()
    }

    #[cfg(test)]
    /// Registers `table_fragments` to compaction groups.
    pub async fn register_table_fragments(
        &self,
        mv_table: Option<u32>,
        mut internal_tables: Vec<u32>,
    ) -> Result<Vec<StateTableId>> {
        let mut pairs = vec![];
        if let Some(mv_table) = mv_table {
            if internal_tables.extract_if(|t| *t == mv_table).count() > 0 {
                tracing::warn!("`mv_table` {} found in `internal_tables`", mv_table);
            }
            // materialized_view
            pairs.push((
                mv_table,
                CompactionGroupId::from(StaticCompactionGroupId::MaterializedView),
            ));
        }
        // internal states
        for table_id in internal_tables {
            pairs.push((
                table_id,
                CompactionGroupId::from(StaticCompactionGroupId::StateDefault),
            ));
        }
        self.register_table_ids_for_test(&pairs).await?;
        Ok(pairs.iter().map(|(table_id, ..)| *table_id).collect_vec())
    }

    #[cfg(test)]
    /// Unregisters `table_fragments` from compaction groups
    pub async fn unregister_table_fragments_vec(
        &self,
        table_fragments: &[crate::model::TableFragments],
    ) {
        self.unregister_table_ids(
            table_fragments
                .iter()
                .flat_map(|t| t.all_table_ids().map(TableId::new)),
        )
        .await
        .unwrap();
    }

    /// Unregisters stale members and groups
    /// The caller should ensure `table_fragments_list` remain unchanged during `purge`.
    /// Currently `purge` is only called during meta service start ups.
    pub async fn purge(&self, valid_ids: &HashSet<TableId>) -> Result<()> {
        let to_unregister = self
            .versioning
            .read()
            .await
            .current_version
            .state_table_info
            .info()
            .keys()
            .cloned()
            .filter(|table_id| !valid_ids.contains(table_id))
            .collect_vec();
        // As we have released versioning lock, the version that `to_unregister` is calculated from
        // may not be the same as the one used in unregister_table_ids. It is OK.
        self.unregister_table_ids(to_unregister).await
    }

    /// The implementation acquires `versioning` lock.
    ///
    /// The method name is temporarily added with a `_for_test` prefix to mark
    /// that it's currently only used in test.
    pub async fn register_table_ids_for_test(
        &self,
        pairs: &[(StateTableId, CompactionGroupId)],
    ) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let mut compaction_group_manager = self.compaction_group_manager.write().await;
        let current_version = &versioning.current_version;
        let default_config = compaction_group_manager.default_compaction_config();
        let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();

        for (table_id, _) in pairs {
            if let Some(info) = current_version
                .state_table_info
                .info()
                .get(&TableId::new(*table_id))
            {
                return Err(Error::CompactionGroup(format!(
                    "table {} already {:?}",
                    *table_id, info
                )));
            }
        }
        // All NewCompactionGroup pairs are mapped to one new compaction group.
        let new_compaction_group_id: OnceCell<CompactionGroupId> = OnceCell::new();
        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();
        let epoch = new_version_delta.latest_version().max_committed_epoch;

        for (table_id, raw_group_id) in pairs {
            let mut group_id = *raw_group_id;
            if group_id == StaticCompactionGroupId::NewCompactionGroup as u64 {
                let mut is_group_init = false;
                group_id = *new_compaction_group_id
                    .get_or_try_init(|| async {
                        next_compaction_group_id(&self.env)
                            .await
                            .map(|new_group_id| {
                                is_group_init = true;
                                new_group_id
                            })
                    })
                    .await?;
                if is_group_init {
                    let group_deltas = &mut new_version_delta
                        .group_deltas
                        .entry(group_id)
                        .or_default()
                        .group_deltas;

                    let config =
                        match compaction_groups_txn.try_get_compaction_group_config(group_id) {
                            Some(config) => config.compaction_config.as_ref().clone(),
                            None => {
                                compaction_groups_txn
                                    .create_compaction_groups(group_id, default_config.clone());
                                default_config.as_ref().clone()
                            }
                        };

                    let group_delta = GroupDelta::GroupConstruct(PbGroupConstruct {
                        group_config: Some(config),
                        group_id,
                        ..Default::default()
                    });

                    group_deltas.push(group_delta);
                }
            }
            assert!(new_version_delta
                .state_table_info_delta
                .insert(
                    TableId::new(*table_id),
                    PbStateTableInfoDelta {
                        committed_epoch: epoch,
                        safe_epoch: epoch,
                        compaction_group_id: *raw_group_id,
                    }
                )
                .is_none());
        }
        new_version_delta.pre_apply();
        commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;

        Ok(())
    }

    pub async fn unregister_table_ids(
        &self,
        table_ids: impl IntoIterator<Item = TableId> + Send,
    ) -> Result<()> {
        let mut table_ids = table_ids.into_iter().peekable();
        if table_ids.peek().is_none() {
            return Ok(());
        }
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();
        let mut modified_groups: HashMap<CompactionGroupId, /* #member table */ u64> =
            HashMap::new();
        // Remove member tables
        for table_id in table_ids.unique() {
            let version = new_version_delta.latest_version();
            let Some(info) = version.state_table_info.info().get(&table_id) else {
                continue;
            };

            modified_groups
                .entry(info.compaction_group_id)
                .and_modify(|count| *count -= 1)
                .or_insert(
                    version
                        .state_table_info
                        .compaction_group_member_tables()
                        .get(&info.compaction_group_id)
                        .expect("should exist")
                        .len() as u64
                        - 1,
                );
            new_version_delta.removed_table_ids.insert(table_id);
        }

        let groups_to_remove = modified_groups
            .into_iter()
            .filter_map(|(group_id, member_count)| {
                if member_count == 0 && group_id > StaticCompactionGroupId::End as CompactionGroupId
                {
                    return Some((
                        group_id,
                        new_version_delta
                            .latest_version()
                            .get_compaction_group_levels(group_id)
                            .levels
                            .len(),
                    ));
                }
                None
            })
            .collect_vec();
        for (group_id, _) in &groups_to_remove {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(*group_id)
                .or_default()
                .group_deltas;

            let group_delta = GroupDelta::GroupDestroy(PbGroupDestroy {});
            group_deltas.push(group_delta);
        }

        for (group_id, max_level) in groups_to_remove {
            remove_compaction_group_in_sst_stat(&self.metrics, group_id, max_level);
        }

        new_version_delta.pre_apply();

        // Purge may cause write to meta store. If it hurts performance while holding versioning
        // lock, consider to make it in batch.
        let mut compaction_group_manager = self.compaction_group_manager.write().await;
        let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();

        compaction_groups_txn.purge(HashSet::from_iter(get_compaction_group_ids(
            version.latest_version(),
        )));
        commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        // No need to handle DeltaType::GroupDestroy during time travel.
        Ok(())
    }

    pub async fn update_compaction_config(
        &self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
    ) -> Result<()> {
        {
            // Avoid lock conflicts with `try_update_write_limits``
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .update_compaction_config(compaction_group_ids, config_to_update)?;
            commit_multi_var!(self.meta_store_ref(), compaction_groups_txn)?;
        }

        if config_to_update
            .iter()
            .any(|c| matches!(c, MutableConfig::Level0StopWriteThresholdSubLevelNumber(_)))
        {
            // Update write limits with lock
            self.try_update_write_limits(compaction_group_ids).await;
        }

        Ok(())
    }

    /// Gets complete compaction group info.
    /// It is the aggregate of `HummockVersion` and `CompactionGroupConfig`
    pub async fn list_compaction_group(&self) -> Vec<CompactionGroupInfo> {
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;
        let mut results = vec![];
        let compaction_group_manager = self.compaction_group_manager.read().await;

        for levels in current_version.levels.values() {
            let compaction_config = compaction_group_manager
                .try_get_compaction_group_config(levels.group_id)
                .unwrap()
                .compaction_config
                .as_ref()
                .clone();
            let group = CompactionGroupInfo {
                id: levels.group_id,
                parent_id: levels.parent_group_id,
                member_table_ids: current_version
                    .state_table_info
                    .compaction_group_member_table_ids(levels.group_id)
                    .iter()
                    .map(|table_id| table_id.table_id)
                    .collect_vec(),
                compaction_config: Some(compaction_config),
            };
            results.push(group);
        }
        results
    }

    /// Splits a compaction group into two. The new one will contain `table_ids`.
    /// Returns the newly created compaction group id.
    pub async fn split_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
    ) -> Result<CompactionGroupId> {
        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_string(),
            ));
        }

        let result = self
            .move_state_tables_to_dedicated_compaction_group(parent_group_id, table_ids)
            .await?;

        Ok(result)
    }

    /// move some table to another compaction-group. Create a new compaction group if it does not
    /// exist.
    #[expect(deprecated)]
    pub async fn move_state_table_to_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
        partition_vnode_count: u32,
    ) -> Result<CompactionGroupId> {
        if table_ids.is_empty() {
            return Ok(parent_group_id);
        }
        let table_ids = table_ids.iter().cloned().unique().collect_vec();
        let compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning
            .current_version
            .levels
            .contains_key(&parent_group_id)
        {
            return Err(Error::CompactionGroup(format!(
                "invalid group {}",
                parent_group_id
            )));
        }

        for table_id in &table_ids {
            if !versioning
                .current_version
                .state_table_info
                .compaction_group_member_table_ids(parent_group_id)
                .contains(&TableId::new(*table_id))
            {
                return Err(Error::CompactionGroup(format!(
                    "table {} doesn't in group {}",
                    table_id, parent_group_id
                )));
            }
        }

        if table_ids.len()
            == versioning
                .current_version
                .state_table_info
                .compaction_group_member_table_ids(parent_group_id)
                .len()
        {
            return Err(Error::CompactionGroup(format!(
                "invalid split attempt for group {}: all member tables are moved",
                parent_group_id
            )));
        }
        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let new_sst_start_id = next_sstable_object_id(
            &self.env,
            new_version_delta
                .latest_version()
                .count_new_ssts_in_group_split(
                    parent_group_id,
                    HashSet::from_iter(table_ids.clone()),
                ),
        )
        .await?;
        let (new_group, target_compaction_group_id) = {
            {
                // All NewCompactionGroup pairs are mapped to one new compaction group.
                let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
                // The new config will be persisted later.
                let mut config = self
                    .compaction_group_manager
                    .read()
                    .await
                    .default_compaction_config()
                    .as_ref()
                    .clone();
                config.split_weight_by_vnode = partition_vnode_count;

                #[expect(deprecated)]
                // fill the deprecated field with default value
                new_version_delta.group_deltas.insert(
                    new_compaction_group_id,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta::GroupConstruct(PbGroupConstruct {
                            group_config: Some(config.clone()),
                            group_id: new_compaction_group_id,
                            parent_group_id,
                            new_sst_start_id,
                            table_ids: vec![],
                            version: CompatibilityVersion::NoMemberTableIds as i32,
                            split_key: None,
                        })],
                    },
                );
                ((new_compaction_group_id, config), new_compaction_group_id)
            }
        };

        let (new_compaction_group_id, config) = new_group;
        new_version_delta.with_latest_version(|version, new_version_delta| {
            for table_id in &table_ids {
                let table_id = TableId::new(*table_id);
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(new_version_delta
                    .state_table_info_delta
                    .insert(
                        table_id,
                        PbStateTableInfoDelta {
                            committed_epoch: info.committed_epoch,
                            safe_epoch: info.safe_epoch,
                            compaction_group_id: new_compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });
        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }
        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();
        let mut canceled_tasks = vec![];
        for task_assignment in compaction_guard.compact_task_assignment.values() {
            if let Some(task) = task_assignment.compact_task.as_ref() {
                let need_cancel = HummockManager::is_compact_task_expired(
                    &task.into(),
                    &versioning.current_version,
                );
                if need_cancel {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            }
        }

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            self.try_send_compaction_request(parent_group_id, compact_task::TaskType::SpaceReclaim);
            self.try_send_compaction_request(
                target_compaction_group_id,
                compact_task::TaskType::SpaceReclaim,
            );
        }

        self.metrics
            .move_state_table_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok(target_compaction_group_id)
    }

    pub async fn split_compaction_group_v2(
        &self,
        parent_group_id: CompactionGroupId,
        table_id: StateTableId,
        vnode: VirtualNode,
    ) -> Result<CompactionGroupId> {
        let compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning
            .current_version
            .levels
            .contains_key(&parent_group_id)
        {
            return Err(Error::CompactionGroup(format!(
                "invalid group {}",
                parent_group_id
            )));
        }

        let member_table_ids = versioning
            .current_version
            .state_table_info
            .compaction_group_member_table_ids(parent_group_id);

        if !member_table_ids.contains(&TableId::new(table_id)) {
            return Err(Error::CompactionGroup(format!(
                "table {} doesn't in group {}",
                table_id, parent_group_id
            )));
        }

        let table_ids = member_table_ids
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec();

        // not need to split
        if table_id == *table_ids.first().unwrap() && group_split::is_vnode_split_to_right(vnode) {
            return Ok(parent_group_id);
        }

        if table_id == *table_ids.last().unwrap() && group_split::is_vnode_split_to_left(vnode) {
            return Ok(parent_group_id);
        }

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let split_key = group_split::build_split_key(table_id, vnode, &table_ids);

        let (_, table_ids_right) = split_table_ids(&table_ids, split_key.clone());

        let split_sst_count = new_version_delta
            .latest_version()
            .count_new_ssts_in_group_split_v2(parent_group_id, split_key.clone());

        tracing::info!(
            "LI)K parent_group_id: {}, split_sst_count: {}",
            parent_group_id,
            split_sst_count
        );

        let new_sst_start_id = next_sstable_object_id(&self.env, split_sst_count).await?;
        let (new_group, target_compaction_group_id) = {
            {
                // All NewCompactionGroup pairs are mapped to one new compaction group.
                let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
                // The new config will be persisted later.
                let config = self
                    .compaction_group_manager
                    .read()
                    .await
                    .default_compaction_config()
                    .as_ref()
                    .clone();

                #[expect(deprecated)]
                // fill the deprecated field with default value
                new_version_delta.group_deltas.insert(
                    new_compaction_group_id,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta::GroupConstruct(PbGroupConstruct {
                            group_config: Some(config.clone()),
                            group_id: new_compaction_group_id,
                            parent_group_id,
                            new_sst_start_id,
                            table_ids: vec![],
                            version: CompatibilityVersion::NoMemberTableIds as i32,
                            split_key: Some(split_key.into()),
                        })],
                    },
                );
                ((new_compaction_group_id, config), new_compaction_group_id)
            }
        };

        let (new_compaction_group_id, config) = new_group;
        new_version_delta.with_latest_version(|version, new_version_delta| {
            for table_id in &table_ids_right {
                let table_id = TableId::new(*table_id);
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(new_version_delta
                    .state_table_info_delta
                    .insert(
                        table_id,
                        PbStateTableInfoDelta {
                            committed_epoch: info.committed_epoch,
                            safe_epoch: info.safe_epoch,
                            compaction_group_id: new_compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });
        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
            compaction_groups_txn
                .create_compaction_groups(new_compaction_group_id, Arc::new(config));

            new_version_delta.pre_apply();
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }
        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();
        let mut canceled_tasks = vec![];
        for task_assignment in compaction_guard.compact_task_assignment.values() {
            if let Some(task) = task_assignment.compact_task.as_ref() {
                let need_cancel = HummockManager::is_compact_task_expired(
                    &task.into(),
                    &versioning.current_version,
                );
                if need_cancel {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            }
        }

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            self.try_send_compaction_request(parent_group_id, compact_task::TaskType::SpaceReclaim);
            self.try_send_compaction_request(
                target_compaction_group_id,
                compact_task::TaskType::SpaceReclaim,
            );
        }

        self.metrics
            .move_state_table_count
            .with_label_values(&[&parent_group_id.to_string()])
            .inc();

        Ok(target_compaction_group_id)
    }

    pub async fn move_state_tables_to_dedicated_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
    ) -> Result<CompactionGroupId> {
        if table_ids.is_empty() {
            return Ok(parent_group_id);
        }

        if !table_ids.is_sorted() {
            return Err(Error::CompactionGroup(
                "table_ids must be sorted".to_string(),
            ));
        }

        // move [3,4,5,6]
        // [1,2,3,4,5,6,7,8,9,10] -> [1,2] [3,4,5,6] [7,8,9,10]
        // split key
        // 1. table_id = 3, vnode = 0, epoch = 0
        // 2. table_id = 7, vnode = 0, epoch = 0
        let table_ids = table_ids.iter().cloned().unique().collect_vec();

        // The new compaction group id is always generate on the right side
        // Hence, we return the first compaction group id as the result

        // split 1
        let target_compaction_group_id = self
            .split_compaction_group_v2(
                parent_group_id,
                *table_ids.first().unwrap(),
                group_split::VNODE_SPLIT_TO_RIGHT,
            )
            .await?;

        // split 2
        self.split_compaction_group_v2(
            target_compaction_group_id,
            *table_ids.last().unwrap(),
            group_split::VNODE_SPLIT_TO_LEFT,
        )
        .await?;

        Ok(target_compaction_group_id)
    }

    pub async fn merge_compaction_group_v2(
        &self,
        group_1: CompactionGroupId,
        group_2: CompactionGroupId,
    ) -> Result<()> {
        let compaction_guard = self.compaction.write().await;
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        // Validate parameters.
        if !versioning.current_version.levels.contains_key(&group_1) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_1)));
        }

        if !versioning.current_version.levels.contains_key(&group_2) {
            return Err(Error::CompactionGroup(format!("invalid group {}", group_2)));
        }

        let state_table_info = versioning.current_version.state_table_info.clone();
        let member_table_ids_1 = state_table_info
            .compaction_group_member_table_ids(group_1)
            .iter()
            .cloned()
            .collect_vec();

        let member_table_ids_2 = state_table_info
            .compaction_group_member_table_ids(group_2)
            .iter()
            .cloned()
            .collect_vec();

        let mut combine_1 = member_table_ids_1.clone();
        combine_1.extend_from_slice(&member_table_ids_2);

        let mut combine_2 = member_table_ids_2;
        combine_2.extend_from_slice(&member_table_ids_1);

        if !combine_1.is_sorted() && !combine_2.is_sorted() {
            return Err(Error::CompactionGroup(format!(
                "invalid merge group_1 {} group_2 {}",
                group_1, group_2
            )));
        }

        let mut left_group_id = group_1;
        let mut right_group_id = group_2;
        let combine_member_table_ids = if combine_1.is_sorted() {
            combine_1
        } else {
            std::mem::swap(&mut left_group_id, &mut right_group_id);
            combine_2
        };

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        let target_compaction_group_id = {
            let config = self
                .compaction_group_manager
                .read()
                .await
                .try_get_compaction_group_config(group_1)
                .unwrap()
                .compaction_config();

            // merge right_group_id to left_group_id and remove right_group_id
            new_version_delta.group_deltas.insert(
                left_group_id,
                GroupDeltas {
                    group_deltas: vec![GroupDelta::GroupMerge(PbGroupMerge {
                        group_config: Some(config.as_ref().clone()),
                        left_group_id,
                        right_group_id,
                    })],
                },
            );
            left_group_id
        };

        // TODO: remove compaciton group_id from state_table_info
        // rewrite compaction_group_id for all tables
        new_version_delta.with_latest_version(|version, new_version_delta| {
            for table_id in combine_member_table_ids {
                let table_id = TableId::new(table_id.table_id());
                let info = version
                    .state_table_info
                    .info()
                    .get(&table_id)
                    .expect("have check exist previously");
                assert!(new_version_delta
                    .state_table_info_delta
                    .insert(
                        table_id,
                        PbStateTableInfoDelta {
                            committed_epoch: info.committed_epoch,
                            safe_epoch: info.safe_epoch,
                            compaction_group_id: target_compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();

            // for metrics reclaim
            {
                let right_group_max_level = new_version_delta
                    .latest_version()
                    .get_compaction_group_levels(right_group_id)
                    .levels
                    .len();

                remove_compaction_group_in_sst_stat(
                    &self.metrics,
                    right_group_id,
                    right_group_max_level,
                );
            }

            new_version_delta.pre_apply();

            // purge right_group_id
            compaction_groups_txn.purge(HashSet::from_iter(get_compaction_group_ids(
                version.latest_version(),
            )));
            commit_multi_var!(self.meta_store_ref(), version, compaction_groups_txn)?;
        }

        // Instead of handling DeltaType::GroupConstruct for time travel, simply enforce a version snapshot.
        versioning.mark_next_time_travel_version_snapshot();

        // cancel tasks
        let mut canceled_tasks = vec![];
        // after merge, all tasks in right_group_id should be canceled
        // otherwise, pending size calculation by level handler will make some mistake
        for task_assignment in compaction_guard.compact_task_assignment.values() {
            if let Some(task) = task_assignment.compact_task.as_ref() {
                let need_cancel = task.compaction_group_id == right_group_id;
                if need_cancel {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            }
        }

        drop(versioning_guard);
        drop(compaction_guard);
        self.report_compact_tasks(canceled_tasks).await?;

        Ok(())
    }

    pub async fn calculate_compaction_group_statistic(&self) -> Vec<TableGroupInfo> {
        let mut infos = vec![];
        {
            let versioning_guard = self.versioning.read().await;
            let version = &versioning_guard.current_version;
            for group_id in version.levels.keys() {
                let mut group_info = TableGroupInfo {
                    group_id: *group_id,
                    ..Default::default()
                };
                for table_id in version
                    .state_table_info
                    .compaction_group_member_table_ids(*group_id)
                {
                    let stats_size = versioning_guard
                        .version_stats
                        .table_stats
                        .get(&table_id.table_id)
                        .map(|stats| stats.total_key_size + stats.total_value_size)
                        .unwrap_or(0);
                    let table_size = std::cmp::max(stats_size, 0) as u64;
                    group_info.group_size += table_size;
                    group_info
                        .table_statistic
                        .insert(table_id.table_id, table_size);
                }
                infos.push(group_info);
            }
        };
        let manager = self.compaction_group_manager.read().await;
        for info in &mut infos {
            if let Some(group) = manager.compaction_groups.get(&info.group_id) {
                info.split_by_table = group.compaction_config.split_by_state_table;
            }
        }
        infos
    }

    pub(super) async fn initial_compaction_group_config_after_load(
        &self,
        versioning_guard: &Versioning,
        compaction_group_manager: &mut CompactionGroupManager,
    ) -> Result<()> {
        // 1. Due to version compatibility, we fix some of the configuration of older versions after hummock starts.
        let current_version = &versioning_guard.current_version;
        let all_group_ids = get_compaction_group_ids(current_version).collect_vec();
        let default_config = compaction_group_manager.default_compaction_config();
        let mut compaction_groups_txn = compaction_group_manager.start_compaction_groups_txn();
        compaction_groups_txn.try_create_compaction_groups(&all_group_ids, default_config);
        commit_multi_var!(self.meta_store_ref(), compaction_groups_txn)?;

        Ok(())
    }
}

/// We muse ensure there is an entry exists in [`CompactionGroupManager`] for any
/// compaction group found in current hummock version. That's done by invoking
/// `get_or_insert_compaction_group_config` or `get_or_insert_compaction_group_configs` before
/// adding any group in current hummock version:
/// 1. initialize default static compaction group.
/// 2. register new table to new compaction group.
/// 3. move existent table to new compaction group.
pub(super) struct CompactionGroupManager {
    compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup>,
    default_config: Arc<CompactionConfig>,
    /// Tables that write limit is trigger for.
    pub write_limit: HashMap<CompactionGroupId, WriteLimit>,
}

impl CompactionGroupManager {
    /// Starts a transaction to update compaction group configs.
    pub fn start_compaction_groups_txn(&mut self) -> CompactionGroupTransaction<'_> {
        CompactionGroupTransaction::new(&mut self.compaction_groups)
    }

    pub fn start_owned_compaction_groups_txn<P: DerefMut<Target = Self>>(
        inner: P,
    ) -> BTreeMapTransactionInner<
        CompactionGroupId,
        CompactionGroup,
        DerefMutForward<
            Self,
            BTreeMap<CompactionGroupId, CompactionGroup>,
            P,
            impl Fn(&Self) -> &BTreeMap<CompactionGroupId, CompactionGroup>,
            impl Fn(&mut Self) -> &mut BTreeMap<CompactionGroupId, CompactionGroup>,
        >,
    > {
        BTreeMapTransactionInner::new(DerefMutForward::new(
            inner,
            |mgr| &mgr.compaction_groups,
            |mgr| &mut mgr.compaction_groups,
        ))
    }

    /// Tries to get compaction group config for `compaction_group_id`.
    pub(super) fn try_get_compaction_group_config(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Option<CompactionGroup> {
        self.compaction_groups.get(&compaction_group_id).cloned()
    }

    /// Tries to get compaction group config for `compaction_group_id`.
    pub(super) fn default_compaction_config(&self) -> Arc<CompactionConfig> {
        self.default_config.clone()
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
            MutableConfig::Level0StopWriteThresholdSubLevelNumber(c) => {
                target.level0_stop_write_threshold_sub_level_number = *c;
            }
            MutableConfig::Level0SubLevelCompactLevelCount(c) => {
                target.level0_sub_level_compact_level_count = *c;
            }
            MutableConfig::Level0OverlappingSubLevelCompactLevelCount(c) => {
                target.level0_overlapping_sub_level_compact_level_count = *c;
            }
            MutableConfig::MaxSpaceReclaimBytes(c) => {
                target.max_space_reclaim_bytes = *c;
            }
            MutableConfig::Level0MaxCompactFileNumber(c) => {
                target.level0_max_compact_file_number = *c;
            }
            MutableConfig::EnableEmergencyPicker(c) => {
                target.enable_emergency_picker = *c;
            }
            MutableConfig::TombstoneReclaimRatio(c) => {
                target.tombstone_reclaim_ratio = *c;
            }
            MutableConfig::CompressionAlgorithm(c) => {
                target.compression_algorithm[c.get_level() as usize]
                    .clone_from(&c.compression_algorithm);
            }
            MutableConfig::MaxL0CompactLevelCount(c) => {
                target.max_l0_compact_level_count = Some(*c);
            }
            MutableConfig::SplitWeightByVnode(c) => {
                target.split_weight_by_vnode = *c;
            }
        }
    }
}

impl<'a> CompactionGroupTransaction<'a> {
    /// Inserts compaction group configs if they do not exist.
    pub fn try_create_compaction_groups(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        config: Arc<CompactionConfig>,
    ) -> bool {
        let mut trivial = true;
        for id in compaction_group_ids {
            if self.contains_key(id) {
                continue;
            }
            let new_entry = CompactionGroup::new(*id, config.as_ref().clone());
            self.insert(*id, new_entry);

            trivial = false;
        }

        !trivial
    }

    pub fn create_compaction_groups(
        &mut self,
        compaction_group_id: CompactionGroupId,
        config: Arc<CompactionConfig>,
    ) {
        self.try_create_compaction_groups(&[compaction_group_id], config);
    }

    /// Tries to get compaction group config for `compaction_group_id`.
    pub(super) fn try_get_compaction_group_config(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Option<&CompactionGroup> {
        self.get(&compaction_group_id)
    }

    /// Removes stale group configs.
    fn purge(&mut self, existing_groups: HashSet<CompactionGroupId>) {
        let stale_group = self
            .tree_ref()
            .keys()
            .cloned()
            .filter(|k| !existing_groups.contains(k))
            .collect_vec();
        if stale_group.is_empty() {
            return;
        }
        for group in stale_group {
            self.remove(group);
        }
    }

    pub(super) fn update_compaction_config(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
    ) -> Result<HashMap<CompactionGroupId, CompactionGroup>> {
        let mut results = HashMap::default();
        for compaction_group_id in compaction_group_ids.iter().unique() {
            let group = self.get(compaction_group_id).ok_or_else(|| {
                Error::CompactionGroup(format!("invalid group {}", *compaction_group_id))
            })?;
            let mut config = group.compaction_config.as_ref().clone();
            update_compaction_config(&mut config, config_to_update);
            if let Err(reason) = validate_compaction_config(&config) {
                return Err(Error::CompactionGroup(reason));
            }
            let mut new_group = group.clone();
            new_group.compaction_config = Arc::new(config);
            self.insert(*compaction_group_id, new_group.clone());
            results.insert(new_group.group_id(), new_group);
        }

        Ok(results)
    }
}

pub mod group_split {
    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_hummock_sdk::compaction_group::StateTableId;
    use risingwave_hummock_sdk::key::{FullKey, TableKey};

    pub const VNODE_SPLIT_TO_RIGHT: VirtualNode = VirtualNode::ZERO;
    pub const VNODE_SPLIT_TO_LEFT: VirtualNode = VirtualNode::MAX;

    pub fn is_vnode_split_to_right(vnode: VirtualNode) -> bool {
        vnode == VNODE_SPLIT_TO_RIGHT
    }

    pub fn is_vnode_split_to_left(vnode: VirtualNode) -> bool {
        vnode == VNODE_SPLIT_TO_LEFT
    }

    // By default, the split key is constructed with vnode = 0 and epoch = 0, so that we can split table_id to the right group
    pub fn build_split_key(
        mut table_id: StateTableId,
        mut vnode: VirtualNode,
        table_ids: &Vec<u32>,
    ) -> Bytes {
        if is_vnode_split_to_left(vnode) {
            // Modify `table_id` to `next_table_id` to satisfy the `split_to_right`` rule, so that the `table_id`` originally passed in will be split to left.
            table_id = table_ids[table_ids.partition_point(|x| *x <= table_id)]; // use next table_id
            vnode = VNODE_SPLIT_TO_RIGHT;
        }

        Bytes::from(
            FullKey::new(TableId::from(table_id), TableKey(vnode.to_be_bytes()), 0).encode(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet};

    use risingwave_common::catalog::TableId;
    use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
    use risingwave_pb::meta::table_fragments::Fragment;

    use crate::hummock::commit_multi_var;
    use crate::hummock::error::Result;
    use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
    use crate::hummock::test_utils::setup_compute_env;
    use crate::manager::MetaStoreImpl;
    use crate::model::TableFragments;

    #[tokio::test]
    async fn test_inner() {
        let (env, ..) = setup_compute_env(8080).await;
        let mut inner = CompactionGroupManager::new(&env).await.unwrap();
        assert_eq!(inner.compaction_groups.len(), 2);

        async fn update_compaction_config(
            meta: &MetaStoreImpl,
            inner: &mut CompactionGroupManager,
            cg_ids: &[u64],
            config_to_update: &[MutableConfig],
        ) -> Result<()> {
            let mut compaction_groups_txn = inner.start_compaction_groups_txn();
            compaction_groups_txn.update_compaction_config(cg_ids, config_to_update)?;
            commit_multi_var!(meta, compaction_groups_txn)
        }

        async fn insert_compaction_group_configs(
            meta: &MetaStoreImpl,
            inner: &mut CompactionGroupManager,
            cg_ids: &[u64],
        ) {
            let default_config = inner.default_compaction_config();
            let mut compaction_groups_txn = inner.start_compaction_groups_txn();
            if compaction_groups_txn.try_create_compaction_groups(cg_ids, default_config) {
                commit_multi_var!(meta, compaction_groups_txn).unwrap();
            }
        }

        update_compaction_config(env.meta_store_ref(), &mut inner, &[100, 200], &[])
            .await
            .unwrap_err();
        insert_compaction_group_configs(env.meta_store_ref(), &mut inner, &[100, 200]).await;
        assert_eq!(inner.compaction_groups.len(), 4);
        let mut inner = CompactionGroupManager::new(&env).await.unwrap();
        assert_eq!(inner.compaction_groups.len(), 4);

        update_compaction_config(
            env.meta_store_ref(),
            &mut inner,
            &[100, 200],
            &[MutableConfig::MaxSubCompaction(123)],
        )
        .await
        .unwrap();
        assert_eq!(inner.compaction_groups.len(), 4);
        assert_eq!(
            inner
                .try_get_compaction_group_config(100)
                .unwrap()
                .compaction_config
                .max_sub_compaction,
            123
        );
        assert_eq!(
            inner
                .try_get_compaction_group_config(200)
                .unwrap()
                .compaction_config
                .max_sub_compaction,
            123
        );
    }

    #[tokio::test]
    async fn test_manager() {
        let (_, compaction_group_manager, ..) = setup_compute_env(8080).await;
        let table_fragment_1 = TableFragments::for_test(
            TableId::new(10),
            BTreeMap::from([(
                1,
                Fragment {
                    fragment_id: 1,
                    state_table_ids: vec![10, 11, 12, 13],
                    ..Default::default()
                },
            )]),
        );
        let table_fragment_2 = TableFragments::for_test(
            TableId::new(20),
            BTreeMap::from([(
                2,
                Fragment {
                    fragment_id: 2,
                    state_table_ids: vec![20, 21, 22, 23],
                    ..Default::default()
                },
            )]),
        );

        // Test register_table_fragments
        let registered_number = || async {
            compaction_group_manager
                .list_compaction_group()
                .await
                .iter()
                .map(|cg| cg.member_table_ids.len())
                .sum::<usize>()
        };
        let group_number =
            || async { compaction_group_manager.list_compaction_group().await.len() };
        assert_eq!(registered_number().await, 0);

        compaction_group_manager
            .register_table_fragments(
                Some(table_fragment_1.table_id().table_id),
                table_fragment_1.internal_table_ids(),
            )
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .register_table_fragments(
                Some(table_fragment_2.table_id().table_id),
                table_fragment_2.internal_table_ids(),
            )
            .await
            .unwrap();
        assert_eq!(registered_number().await, 8);

        // Test unregister_table_fragments
        compaction_group_manager
            .unregister_table_fragments_vec(&[table_fragment_1.clone()])
            .await;
        assert_eq!(registered_number().await, 4);

        // Test purge_stale_members: table fragments
        compaction_group_manager
            .purge(&table_fragment_2.all_table_ids().map(TableId::new).collect())
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .purge(&HashSet::new())
            .await
            .unwrap();
        assert_eq!(registered_number().await, 0);

        assert_eq!(group_number().await, 2);

        compaction_group_manager
            .register_table_fragments(
                Some(table_fragment_1.table_id().table_id),
                table_fragment_1.internal_table_ids(),
            )
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        assert_eq!(group_number().await, 2);

        compaction_group_manager
            .unregister_table_fragments_vec(&[table_fragment_1])
            .await;
        assert_eq!(registered_number().await, 0);
        assert_eq!(group_number().await, 2);
    }
}
