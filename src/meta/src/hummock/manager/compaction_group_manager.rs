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

use function_name::named;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    build_version_delta_after_version, get_compaction_group_ids, get_compaction_group_ssts,
    get_member_table_ids, try_get_compaction_group_id_by_table_id, TableGroupInfo,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_meta_model_v2::compaction_config;
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version_delta::GroupDeltas;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::subscribe_compaction_event_request::ReportTask;
use risingwave_pb::hummock::{
    compact_task, CompactionConfig, CompactionGroupInfo, CompatibilityVersion, GroupConstruct,
    GroupDelta, GroupDestroy, GroupMetaChange, GroupTableChange,
};
use thiserror_ext::AsReport;
use tokio::sync::{OnceCell, RwLock};

use super::write_lock;
use crate::controller::SqlMetaStore;
use crate::hummock::compaction::compaction_config::{
    validate_compaction_config, CompactionConfigBuilder,
};
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::{
    commit_multi_var, create_trx_wrapper, drop_sst, read_lock, HummockManager,
};
use crate::hummock::metrics_utils::remove_compaction_group_in_sst_stat;
use crate::hummock::model::CompactionGroup;
use crate::hummock::sequence::{next_compaction_group_id, next_sstable_object_id};
use crate::manager::MetaSrvEnv;
use crate::model::{
    BTreeMapEntryTransaction, BTreeMapEntryTransactionWrapper, BTreeMapTransaction,
    BTreeMapTransactionWrapper, MetadataModel, MetadataModelError, ValTransaction,
};
use crate::storage::MetaStore;
use crate::stream::CreateStreamingJobOption;

impl HummockManager {
    pub(super) async fn build_compaction_group_manager(
        env: &MetaSrvEnv,
    ) -> Result<RwLock<CompactionGroupManager>> {
        let default_config = match env.opts.compaction_config.as_ref() {
            None => CompactionConfigBuilder::new().build(),
            Some(opt) => CompactionConfigBuilder::with_opt(opt).build(),
        };
        Self::build_compaction_group_manager_with_config(env, default_config).await
    }

    pub(super) async fn build_compaction_group_manager_with_config(
        env: &MetaSrvEnv,
        default_config: CompactionConfig,
    ) -> Result<RwLock<CompactionGroupManager>> {
        let compaction_group_manager = RwLock::new(CompactionGroupManager {
            compaction_groups: BTreeMap::new(),
            default_config,
            sql_meta_store: env.sql_meta_store(),
        });
        compaction_group_manager
            .write()
            .await
            .init(env.meta_store())
            .await?;
        Ok(compaction_group_manager)
    }

    /// Should not be called inside [`HummockManager`], because it requests locks internally.
    /// The implementation acquires `versioning` lock.
    #[named]
    pub async fn compaction_group_ids(&self) -> Vec<CompactionGroupId> {
        get_compaction_group_ids(&read_lock!(self, versioning).await.current_version).collect_vec()
    }

    /// The implementation acquires `compaction_group_manager` lock.
    pub async fn get_compaction_group_map(&self) -> BTreeMap<CompactionGroupId, CompactionGroup> {
        self.compaction_group_manager
            .read()
            .await
            .compaction_groups
            .clone()
    }

    /// Registers `table_fragments` to compaction groups.
    pub async fn register_table_fragments(
        &self,
        mv_table: Option<u32>,
        mut internal_tables: Vec<u32>,
        create_stream_job_option: CreateStreamingJobOption,
    ) -> Result<Vec<StateTableId>> {
        let mut pairs = vec![];
        if let Some(mv_table) = mv_table {
            if internal_tables.extract_if(|t| *t == mv_table).count() > 0 {
                tracing::warn!("`mv_table` {} found in `internal_tables`", mv_table);
            }
            // materialized_view
            pairs.push((
                mv_table,
                if create_stream_job_option.new_independent_compaction_group {
                    CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
                } else {
                    CompactionGroupId::from(StaticCompactionGroupId::MaterializedView)
                },
            ));
        }
        // internal states
        for table_id in internal_tables {
            pairs.push((
                table_id,
                if create_stream_job_option.new_independent_compaction_group {
                    CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
                } else {
                    CompactionGroupId::from(StaticCompactionGroupId::StateDefault)
                },
            ));
        }
        self.register_table_ids(&pairs).await?;
        Ok(pairs.iter().map(|(table_id, ..)| *table_id).collect_vec())
    }

    #[cfg(test)]
    /// Unregisters `table_fragments` from compaction groups
    pub async fn unregister_table_fragments_vec(
        &self,
        table_fragments: &[crate::model::TableFragments],
    ) {
        self.unregister_table_ids_fail_fast(
            &table_fragments
                .iter()
                .flat_map(|t| t.all_table_ids())
                .collect_vec(),
        )
        .await;
    }

    /// Unregisters stale members and groups
    /// The caller should ensure `table_fragments_list` remain unchanged during `purge`.
    /// Currently `purge` is only called during meta service start ups.
    #[named]
    pub async fn purge(&self, valid_ids: &[u32]) {
        let registered_members =
            get_member_table_ids(&read_lock!(self, versioning).await.current_version);
        let to_unregister = registered_members
            .into_iter()
            .filter(|table_id| !valid_ids.contains(table_id))
            .collect_vec();
        // As we have released versioning lock, the version that `to_unregister` is calculated from
        // may not be the same as the one used in unregister_table_ids. It is OK.
        self.unregister_table_ids_fail_fast(&to_unregister).await;
    }

    /// The implementation acquires `versioning` lock.
    #[named]
    pub async fn register_table_ids(
        &self,
        pairs: &[(StateTableId, CompactionGroupId)],
    ) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;

        for (table_id, _) in pairs {
            if let Some(old_group) =
                try_get_compaction_group_id_by_table_id(current_version, *table_id)
            {
                return Err(Error::CompactionGroup(format!(
                    "table {} already in group {}",
                    *table_id, old_group
                )));
            }
        }
        // All NewCompactionGroup pairs are mapped to one new compaction group.
        let new_compaction_group_id: OnceCell<CompactionGroupId> = OnceCell::new();
        let mut new_version_delta = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapEntryTransactionWrapper,
            BTreeMapEntryTransaction::new_insert(
                &mut versioning.hummock_version_deltas,
                current_version.id + 1,
                build_version_delta_after_version(current_version),
            )
        );

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
                    let config = self
                        .compaction_group_manager
                        .write()
                        .await
                        .get_or_insert_compaction_group_config(group_id, self.env.meta_store())
                        .await?
                        .compaction_config
                        .as_ref()
                        .clone();
                    group_deltas.push(GroupDelta {
                        delta_type: Some(DeltaType::GroupConstruct(GroupConstruct {
                            group_config: Some(config),
                            group_id,
                            ..Default::default()
                        })),
                    });
                }
            }
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(group_id)
                .or_default()
                .group_deltas;
            group_deltas.push(GroupDelta {
                delta_type: Some(DeltaType::GroupMetaChange(GroupMetaChange {
                    table_ids_add: vec![*table_id],
                    ..Default::default()
                })),
            });
        }
        let mut current_version = versioning.current_version.clone();
        let sst_split_info = current_version.apply_version_delta(&new_version_delta);
        assert!(sst_split_info.is_empty());
        commit_multi_var!(
            self.env.meta_store(),
            self.sql_meta_store(),
            new_version_delta
        )?;
        versioning.current_version = current_version;

        self.notify_last_version_delta(versioning);

        Ok(())
    }

    #[named]
    async fn unregister_table_ids(&self, table_ids: &[StateTableId]) -> Result<()> {
        if table_ids.is_empty() {
            return Ok(());
        }
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;
        let mut new_version_delta = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapEntryTransactionWrapper,
            BTreeMapEntryTransaction::new_insert(
                &mut versioning.hummock_version_deltas,
                current_version.id + 1,
                build_version_delta_after_version(current_version),
            )
        );
        let mut modified_groups: HashMap<CompactionGroupId, /* #member table */ u64> =
            HashMap::new();
        // Remove member tables
        for table_id in table_ids.iter().unique() {
            let group_id = match try_get_compaction_group_id_by_table_id(current_version, *table_id)
            {
                Some(group_id) => group_id,
                None => continue,
            };
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(group_id)
                .or_default()
                .group_deltas;
            group_deltas.push(GroupDelta {
                delta_type: Some(DeltaType::GroupMetaChange(GroupMetaChange {
                    table_ids_remove: vec![*table_id],
                    ..Default::default()
                })),
            });
            modified_groups
                .entry(group_id)
                .and_modify(|count| *count -= 1)
                .or_insert(
                    current_version
                        .get_compaction_group_levels(group_id)
                        .member_table_ids
                        .len() as u64
                        - 1,
                );
            new_version_delta
                .removed_table_ids
                .push(TableId::new(*table_id));
        }

        // Remove empty group, GC SSTs and remove metric.
        let mut branched_ssts = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.branched_ssts)
        );
        let groups_to_remove = modified_groups
            .into_iter()
            .filter_map(|(group_id, member_count)| {
                if member_count == 0 && group_id > StaticCompactionGroupId::End as CompactionGroupId
                {
                    return Some(group_id);
                }
                None
            })
            .collect_vec();
        for group_id in &groups_to_remove {
            // We don't bother to add IntraLevelDelta to remove SSTs from group, because the entire
            // group is to be removed.
            // However, we need to take care of SST GC for the removed group.
            for (object_id, sst_id) in get_compaction_group_ssts(current_version, *group_id) {
                if drop_sst(&mut branched_ssts, *group_id, object_id, sst_id) {
                    new_version_delta.gc_object_ids.push(object_id);
                }
            }
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(*group_id)
                .or_default()
                .group_deltas;
            group_deltas.push(GroupDelta {
                delta_type: Some(DeltaType::GroupDestroy(GroupDestroy {})),
            });
        }
        let mut current_version = versioning.current_version.clone();
        let sst_split_info = current_version.apply_version_delta(&new_version_delta);
        assert!(sst_split_info.is_empty());
        commit_multi_var!(
            self.env.meta_store(),
            self.sql_meta_store(),
            new_version_delta
        )?;

        for group_id in &groups_to_remove {
            let max_level = versioning
                .current_version
                .get_compaction_group_levels(*group_id)
                .get_levels()
                .len();
            remove_compaction_group_in_sst_stat(&self.metrics, *group_id, max_level);
        }
        versioning.current_version = current_version;
        branched_ssts.commit_memory();

        self.notify_last_version_delta(versioning);

        // Purge may cause write to meta store. If it hurts performance while holding versioning
        // lock, consider to make it in batch.
        self.compaction_group_manager
            .write()
            .await
            .purge(
                HashSet::from_iter(get_compaction_group_ids(&versioning.current_version)),
                self.env.meta_store(),
            )
            .await?;
        Ok(())
    }

    /// The implementation acquires `versioning` lock and `compaction_group_manager` lock.
    pub async fn unregister_table_ids_fail_fast(&self, table_ids: &[StateTableId]) {
        self.unregister_table_ids(table_ids)
            .await
            .unwrap_or_else(|e| {
                panic!("unregister table ids fail: {table_ids:?} {}", e.as_report())
            });
    }

    pub async fn update_compaction_config(
        &self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
    ) -> Result<Vec<CompactionGroup>> {
        let result = self
            .compaction_group_manager
            .write()
            .await
            .update_compaction_config(
                compaction_group_ids,
                config_to_update,
                self.env.meta_store(),
            )
            .await?;
        if config_to_update
            .iter()
            .any(|c| matches!(c, MutableConfig::Level0StopWriteThresholdSubLevelNumber(_)))
        {
            self.try_update_write_limits(compaction_group_ids).await;
        }

        Ok(result)
    }

    /// Gets complete compaction group info.
    /// It is the aggregate of `HummockVersion` and `CompactionGroupConfig`
    #[named]
    pub async fn list_compaction_group(&self) -> Vec<CompactionGroupInfo> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;
        let mut compaction_groups = vec![];
        for levels in current_version.levels.values() {
            let config = self
                .compaction_group_manager
                .read()
                .await
                .try_get_compaction_group_config(levels.group_id)
                .unwrap()
                .compaction_config;
            let group = CompactionGroupInfo {
                id: levels.group_id,
                parent_id: levels.parent_group_id,
                member_table_ids: levels.member_table_ids.clone(),
                compaction_config: Some(config.as_ref().clone()),
            };
            compaction_groups.push(group);
        }
        compaction_groups
    }

    /// Splits a compaction group into two. The new one will contain `table_ids`.
    /// Returns the newly created compaction group id.
    pub async fn split_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
    ) -> Result<CompactionGroupId> {
        let result = self
            .move_state_table_to_compaction_group(parent_group_id, table_ids, None, 0)
            .await?;
        self.group_to_table_vnode_partition
            .write()
            .insert(result.0, result.1);

        Ok(result.0)
    }

    /// move some table to another compaction-group. Create a new compaction group if it does not
    /// exist.
    /// TODO: Move table_to_partition in result to compaction group
    #[named]
    pub async fn move_state_table_to_compaction_group(
        &self,
        parent_group_id: CompactionGroupId,
        table_ids: &[StateTableId],
        target_group_id: Option<CompactionGroupId>,
        partition_vnode_count: u32,
    ) -> Result<(CompactionGroupId, BTreeMap<StateTableId, u32>)> {
        let mut table_to_partition = BTreeMap::default();
        if table_ids.is_empty() {
            return Ok((parent_group_id, table_to_partition));
        }
        let table_ids = table_ids.iter().cloned().unique().collect_vec();
        let compaction_guard = write_lock!(self, compaction).await;
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;
        // Validate parameters.
        let parent_group = current_version
            .levels
            .get(&parent_group_id)
            .ok_or_else(|| Error::CompactionGroup(format!("invalid group {}", parent_group_id)))?;
        for table_id in &table_ids {
            if !parent_group.member_table_ids.contains(table_id) {
                return Err(Error::CompactionGroup(format!(
                    "table {} doesn't in group {}",
                    table_id, parent_group_id
                )));
            }
        }

        if table_ids.len() == parent_group.member_table_ids.len() {
            return Err(Error::CompactionGroup(format!(
                "invalid split attempt for group {}: all member tables are moved",
                parent_group_id
            )));
        }
        if let Some(compaction_group_id) = target_group_id {
            if !versioning.check_branched_sst_in_target_group(
                &table_ids,
                &parent_group_id,
                &compaction_group_id,
            ) {
                return Err(Error::CompactionGroup(format!(
                    "invalid split attempt for group {}: we shall wait some time for parent group and target group could compact stale sst files",
                    parent_group_id
                )));
            }
        }
        let mut new_version_delta = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapEntryTransactionWrapper,
            BTreeMapEntryTransaction::new_insert(
                &mut versioning.hummock_version_deltas,
                current_version.id + 1,
                build_version_delta_after_version(current_version),
            )
        );
        let new_sst_start_id = next_sstable_object_id(
            &self.env,
            current_version.count_new_ssts_in_group_split(
                parent_group_id,
                HashSet::from_iter(table_ids.clone()),
            ),
        )
        .await?;
        let mut new_group = None;
        let target_compaction_group_id = match target_group_id {
            Some(compaction_group_id) => {
                match current_version.levels.get(&compaction_group_id) {
                    Some(group) => {
                        for table_id in &table_ids {
                            if group.member_table_ids.contains(table_id) {
                                return Err(Error::CompactionGroup(format!(
                                    "table {} already exist in group {}",
                                    *table_id, compaction_group_id,
                                )));
                            }
                        }
                    }
                    None => {
                        return Err(Error::CompactionGroup(format!(
                            "target group {} does not exist",
                            compaction_group_id,
                        )));
                    }
                }
                let group_deltas = &mut new_version_delta
                    .group_deltas
                    .entry(compaction_group_id)
                    .or_default()
                    .group_deltas;
                group_deltas.push(GroupDelta {
                    delta_type: Some(DeltaType::GroupTableChange(GroupTableChange {
                        table_ids: table_ids.to_vec(),
                        origin_group_id: parent_group_id,
                        target_group_id: compaction_group_id,
                        new_sst_start_id,
                        version: CompatibilityVersion::NoTrivialSplit as i32,
                    })),
                });
                compaction_group_id
            }
            None => {
                // All NewCompactionGroup pairs are mapped to one new compaction group.
                let new_compaction_group_id = next_compaction_group_id(&self.env).await?;
                // The new config will be persisted later.
                let mut config = self
                    .compaction_group_manager
                    .read()
                    .await
                    .default_compaction_config();
                config.split_weight_by_vnode = partition_vnode_count;

                new_version_delta.group_deltas.insert(
                    new_compaction_group_id,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta {
                            delta_type: Some(DeltaType::GroupConstruct(GroupConstruct {
                                group_config: Some(config.clone()),
                                group_id: new_compaction_group_id,
                                parent_group_id,
                                new_sst_start_id,
                                table_ids: table_ids.to_vec(),
                                version: CompatibilityVersion::NoTrivialSplit as i32,
                            })),
                        }],
                    },
                );

                new_group = Some((new_compaction_group_id, config));
                new_version_delta.group_deltas.insert(
                    parent_group_id,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta {
                            delta_type: Some(DeltaType::GroupMetaChange(GroupMetaChange {
                                table_ids_remove: table_ids.to_vec(),
                                ..Default::default()
                            })),
                        }],
                    },
                );
                new_compaction_group_id
            }
        };

        let mut current_version = versioning.current_version.clone();
        let sst_split_info = current_version.apply_version_delta(&new_version_delta);

        // `branched_ssts` only commit in memory, so `TXN` make no difference.
        let mut branched_ssts = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.branched_ssts)
        );
        if let Some((new_compaction_group_id, config)) = new_group {
            let mut compaction_group_manager = self.compaction_group_manager.write().await;
            let insert = create_trx_wrapper!(
                self.sql_meta_store(),
                BTreeMapEntryTransactionWrapper,
                BTreeMapEntryTransaction::new_insert(
                    &mut compaction_group_manager.compaction_groups,
                    new_compaction_group_id,
                    CompactionGroup {
                        group_id: new_compaction_group_id,
                        compaction_config: Arc::new(config),
                    },
                )
            );
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                new_version_delta,
                insert
            )?;
            // Currently, only splitting out a single table_id is supported.
            for table_id in table_ids {
                table_to_partition.insert(table_id, partition_vnode_count);
            }
        } else {
            commit_multi_var!(
                self.env.meta_store(),
                self.sql_meta_store(),
                new_version_delta
            )?;
        }
        versioning.current_version = current_version;
        // Updates SST split info
        let mut changed_sst_ids: HashSet<u64> = HashSet::default();
        for (object_id, sst_id, parent_old_sst_id, parent_new_sst_id) in sst_split_info {
            changed_sst_ids.insert(parent_old_sst_id);
            match branched_ssts.get_mut(object_id) {
                Some(mut entry) => {
                    entry.insert(parent_group_id, parent_new_sst_id);
                    entry.insert(target_compaction_group_id, sst_id);
                }
                None => {
                    let mut groups = HashMap::from_iter([(target_compaction_group_id, sst_id)]);
                    groups.insert(parent_group_id, parent_new_sst_id);
                    branched_ssts.insert(object_id, groups);
                }
            }
        }
        branched_ssts.commit_memory();
        self.notify_last_version_delta(versioning);
        drop(versioning_guard);
        let mut canceled_tasks = vec![];
        for task_assignment in compaction_guard.compact_task_assignment.values() {
            let mut need_cancel = false;
            if let Some(task) = task_assignment.compact_task.as_ref() {
                for input_level in &task.input_ssts {
                    for sst in &input_level.table_infos {
                        if changed_sst_ids.contains(&sst.sst_id) {
                            need_cancel = true;
                            break;
                        }
                    }
                }
                if need_cancel {
                    canceled_tasks.push(ReportTask {
                        task_id: task.task_id,
                        task_status: TaskStatus::ManualCanceled as i32,
                        table_stats_change: HashMap::default(),
                        sorted_output_ssts: vec![],
                    });
                }
            }
        }

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

        Ok((target_compaction_group_id, table_to_partition))
    }

    #[named]
    pub async fn calculate_compaction_group_statistic(&self) -> Vec<TableGroupInfo> {
        let mut infos = vec![];
        {
            let versioning_guard = read_lock!(self, versioning).await;
            let version = &versioning_guard.current_version;
            for (group_id, group) in &version.levels {
                let mut group_info = TableGroupInfo {
                    group_id: *group_id,
                    ..Default::default()
                };
                for table_id in &group.member_table_ids {
                    let stats_size = versioning_guard
                        .version_stats
                        .table_stats
                        .get(table_id)
                        .map(|stats| stats.total_key_size + stats.total_value_size)
                        .unwrap_or(0);
                    let table_size = std::cmp::max(stats_size, 0) as u64;
                    group_info.group_size += table_size;
                    group_info.table_statistic.insert(*table_id, table_size);
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
    default_config: CompactionConfig,
    sql_meta_store: Option<SqlMetaStore>,
}

impl CompactionGroupManager {
    async fn init<S: MetaStore>(&mut self, meta_store: Option<&S>) -> Result<()> {
        let loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup> =
            match &self.sql_meta_store {
                None => CompactionGroup::list(meta_store.unwrap())
                    .await?
                    .into_iter()
                    .map(|cg| (cg.group_id(), cg))
                    .collect(),
                Some(sql_meta_store) => {
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
        if !loaded_compaction_groups.is_empty() {
            self.compaction_groups = loaded_compaction_groups;
        }
        Ok(())
    }

    /// Gets compaction group config for `compaction_group_id`, inserts default one if missing.
    pub(super) async fn get_or_insert_compaction_group_config<S: MetaStore>(
        &mut self,
        compaction_group_id: CompactionGroupId,
        meta_store: Option<&S>,
    ) -> Result<CompactionGroup> {
        let r = self
            .get_or_insert_compaction_group_configs(&[compaction_group_id], meta_store)
            .await?;
        Ok(r.into_values().next().unwrap())
    }

    /// Gets compaction group configs for `compaction_group_ids`, inserts default one if missing.
    pub(super) async fn get_or_insert_compaction_group_configs<S: MetaStore>(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        meta_store: Option<&S>,
    ) -> Result<HashMap<CompactionGroupId, CompactionGroup>> {
        let mut compaction_groups = create_trx_wrapper!(
            self.sql_meta_store,
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut self.compaction_groups,)
        );
        for id in compaction_group_ids {
            if compaction_groups.contains_key(id) {
                continue;
            }
            let new_entry = CompactionGroup::new(*id, self.default_config.clone());
            compaction_groups.insert(*id, new_entry);
        }
        commit_multi_var!(meta_store, self.sql_meta_store, compaction_groups)?;

        let r = compaction_group_ids
            .iter()
            .map(|id| (*id, self.compaction_groups[id].clone()))
            .collect();
        Ok(r)
    }

    /// Tries to get compaction group config for `compaction_group_id`.
    pub(super) fn try_get_compaction_group_config(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Option<CompactionGroup> {
        self.compaction_groups.get(&compaction_group_id).cloned()
    }

    pub(super) fn default_compaction_config(&self) -> CompactionConfig {
        self.default_config.clone()
    }

    pub async fn update_compaction_config<S: MetaStore>(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
        meta_store: Option<&S>,
    ) -> Result<Vec<CompactionGroup>> {
        let mut compaction_groups = create_trx_wrapper!(
            self.sql_meta_store,
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut self.compaction_groups,)
        );
        let mut result = Vec::with_capacity(compaction_group_ids.len());
        for compaction_group_id in compaction_group_ids.iter().unique() {
            let group = compaction_groups.get(compaction_group_id).ok_or_else(|| {
                Error::CompactionGroup(format!("invalid group {}", *compaction_group_id))
            })?;
            let mut config = group.compaction_config.as_ref().clone();
            update_compaction_config(&mut config, config_to_update);
            if let Err(reason) = validate_compaction_config(&config) {
                return Err(Error::CompactionGroup(reason));
            }
            let mut new_group = group.clone();
            new_group.compaction_config = Arc::new(config);
            compaction_groups.insert(*compaction_group_id, new_group.clone());
            result.push(new_group);
        }
        commit_multi_var!(meta_store, self.sql_meta_store, compaction_groups)?;
        Ok(result)
    }

    /// Initializes the config for a group.
    /// Should only be used by compaction test.
    pub async fn init_compaction_config_for_replay<S: MetaStore>(
        &mut self,
        group_id: CompactionGroupId,
        config: CompactionConfig,
        meta_store: Option<&S>,
    ) -> Result<()> {
        let insert = create_trx_wrapper!(
            self.sql_meta_store,
            BTreeMapEntryTransactionWrapper,
            BTreeMapEntryTransaction::new_insert(
                &mut self.compaction_groups,
                group_id,
                CompactionGroup {
                    group_id,
                    compaction_config: Arc::new(config),
                },
            )
        );
        commit_multi_var!(meta_store, self.sql_meta_store, insert)?;
        Ok(())
    }

    /// Removes stale group configs.
    async fn purge<S: MetaStore>(
        &mut self,
        existing_groups: HashSet<CompactionGroupId>,
        meta_store: Option<&S>,
    ) -> Result<()> {
        let mut compaction_groups = create_trx_wrapper!(
            self.sql_meta_store,
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut self.compaction_groups,)
        );
        let stale_group = compaction_groups
            .tree_ref()
            .keys()
            .cloned()
            .filter(|k| !existing_groups.contains(k))
            .collect_vec();
        if stale_group.is_empty() {
            return Ok(());
        }
        for group in stale_group {
            compaction_groups.remove(group);
        }
        commit_multi_var!(meta_store, self.sql_meta_store, compaction_groups)?;
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
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
    use risingwave_pb::meta::table_fragments::Fragment;

    use crate::hummock::test_utils::setup_compute_env;
    use crate::hummock::HummockManager;
    use crate::model::TableFragments;
    use crate::stream::CreateStreamingJobOption;

    #[tokio::test]
    async fn test_inner() {
        let (env, ..) = setup_compute_env(8080).await;
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.compaction_groups.len(), 2);
        inner
            .write()
            .await
            .update_compaction_config(&[100, 200], &[], env.meta_store())
            .await
            .unwrap_err();
        inner
            .write()
            .await
            .get_or_insert_compaction_group_configs(&[100, 200], env.meta_store())
            .await
            .unwrap();
        assert_eq!(inner.read().await.compaction_groups.len(), 4);
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.compaction_groups.len(), 4);
        inner
            .write()
            .await
            .update_compaction_config(
                &[100, 200],
                &[MutableConfig::MaxSubCompaction(123)],
                env.meta_store(),
            )
            .await
            .unwrap();
        assert_eq!(inner.read().await.compaction_groups.len(), 4);
        assert_eq!(
            inner
                .read()
                .await
                .try_get_compaction_group_config(100)
                .unwrap()
                .compaction_config
                .max_sub_compaction,
            123
        );
        assert_eq!(
            inner
                .read()
                .await
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
                CreateStreamingJobOption {
                    new_independent_compaction_group: false,
                },
            )
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .register_table_fragments(
                Some(table_fragment_2.table_id().table_id),
                table_fragment_2.internal_table_ids(),
                CreateStreamingJobOption {
                    new_independent_compaction_group: false,
                },
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
            .purge(&table_fragment_2.all_table_ids().collect_vec())
            .await;
        assert_eq!(registered_number().await, 4);
        compaction_group_manager.purge(&[]).await;
        assert_eq!(registered_number().await, 0);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `register_table_fragments`
        assert_eq!(group_number().await, 2);

        compaction_group_manager
            .register_table_fragments(
                Some(table_fragment_1.table_id().table_id),
                table_fragment_1.internal_table_ids(),
                CreateStreamingJobOption {
                    new_independent_compaction_group: true,
                },
            )
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        assert_eq!(group_number().await, 3);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `unregister_table_fragments`
        compaction_group_manager
            .unregister_table_fragments_vec(&[table_fragment_1])
            .await;
        assert_eq!(registered_number().await, 0);
        assert_eq!(group_number().await, 2);
    }
}
