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

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::split_sst;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_stats::{
    add_prost_table_stats_map, purge_prost_table_stats, to_prost_table_stats_map, PbTableStatsMap,
};
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockEpoch, HummockSstableObjectId, LocalSstableInfo,
};
use risingwave_pb::hummock::compact_task::{self};
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version_delta::ChangeLogDelta;
use risingwave_pb::hummock::{
    GroupDelta, HummockSnapshot, IntraLevelDelta, SstableInfo, StateTableInfoDelta,
};

use super::transaction::SingleDeltaTransaction;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::transaction::{
    HummockVersionStatsTransaction, HummockVersionTransaction,
};
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::manager::HISTORY_TABLE_INFO_STATISTIC_TIME;
use crate::hummock::metrics_utils::{
    get_or_create_local_table_stat, trigger_local_table_stat, trigger_sst_stat,
};
use crate::hummock::sequence::next_sstable_object_id;
use crate::hummock::{commit_multi_var, start_measure_real_process_timer, HummockManager};

#[derive(Debug, Clone)]
pub struct NewTableFragmentInfo {
    pub table_id: TableId,
    pub mv_table_id: Option<TableId>,
    pub internal_table_ids: Vec<TableId>,
}

pub struct CommitEpochInfo {
    pub sstables: Vec<LocalSstableInfo>,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    pub new_table_fragment_info: Option<NewTableFragmentInfo>,
    pub change_log_delta: HashMap<TableId, ChangeLogDelta>,
    pub table_committed_epoch: BTreeMap<HummockEpoch, HashSet<TableId>>,
    pub max_committed_epoch: HummockEpoch,
}

impl CommitEpochInfo {
    pub fn new(
        sstables: Vec<LocalSstableInfo>,
        new_table_watermarks: HashMap<TableId, TableWatermarks>,
        sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
        new_table_fragment_info: Option<NewTableFragmentInfo>,
        change_log_delta: HashMap<TableId, ChangeLogDelta>,
        table_committed_epoch: BTreeMap<HummockEpoch, HashSet<TableId>>,
        max_committed_epoch: HummockEpoch,
    ) -> Self {
        Self {
            sstables,
            new_table_watermarks,
            sst_to_context,
            new_table_fragment_info,
            change_log_delta,
            table_committed_epoch,
            max_committed_epoch,
        }
    }
}

impl HummockManager {
    #[cfg(any(test, feature = "test"))]
    pub async fn commit_epoch_for_test(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<impl Into<LocalSstableInfo>>,
        sst_to_context: HashMap<HummockSstableObjectId, HummockContextId>,
    ) -> Result<()> {
        let tables = self
            .versioning
            .read()
            .await
            .current_version
            .state_table_info
            .info()
            .keys()
            .cloned()
            .collect();
        let info = CommitEpochInfo::new(
            sstables.into_iter().map(Into::into).collect(),
            HashMap::new(),
            sst_to_context,
            None,
            HashMap::new(),
            BTreeMap::from_iter([(epoch, tables)]),
            epoch,
        );
        self.commit_epoch(info).await?;
        Ok(())
    }

    /// Caller should ensure `epoch` > `max_committed_epoch`
    pub async fn commit_epoch(
        &self,
        commit_info: CommitEpochInfo,
    ) -> Result<Option<HummockSnapshot>> {
        let CommitEpochInfo {
            mut sstables,
            new_table_watermarks,
            sst_to_context,
            new_table_fragment_info,
            change_log_delta,
            table_committed_epoch,
            max_committed_epoch: epoch,
        } = commit_info;
        let mut versioning_guard = self.versioning.write().await;
        let _timer = start_measure_real_process_timer!(self, "commit_epoch");
        // Prevent commit new epochs if this flag is set
        if versioning_guard.disable_commit_epochs {
            return Ok(None);
        }

        let versioning: &mut Versioning = &mut versioning_guard;
        self.commit_epoch_sanity_check(
            epoch,
            &sstables,
            &sst_to_context,
            &versioning.current_version,
        )
        .await?;

        // Consume and aggregate table stats.
        let mut table_stats_change = PbTableStatsMap::default();
        for s in &mut sstables {
            add_prost_table_stats_map(
                &mut table_stats_change,
                &to_prost_table_stats_map(std::mem::take(&mut s.table_stats)),
            );
        }

        let mut version = HummockVersionTransaction::new(
            &mut versioning.current_version,
            &mut versioning.hummock_version_deltas,
            self.env.notification_manager(),
            &self.metrics,
        );
        let mut new_version_delta = version.new_delta();

        new_version_delta.max_committed_epoch = epoch;
        new_version_delta.new_table_watermarks = new_table_watermarks;
        new_version_delta.change_log_delta = change_log_delta;

        let mut table_compaction_group_mapping = new_version_delta
            .latest_version()
            .state_table_info
            .build_table_compaction_group_id();

        let mut new_table_ids = HashMap::new();
        // Add new table
        if let Some(new_fragment_table_info) = new_table_fragment_info {
            if !new_fragment_table_info.internal_table_ids.is_empty() {
                let new_table_id_to_cg = self.on_handle_add_new_table(
                    &mut new_version_delta,
                    &new_fragment_table_info.internal_table_ids,
                    StaticCompactionGroupId::StateDefault as u64,
                    &mut table_compaction_group_mapping,
                )?;

                new_table_ids.extend(new_table_id_to_cg.into_iter());
            }

            if let Some(mv_table_id) = new_fragment_table_info.mv_table_id {
                let new_table_id_to_cg = self.on_handle_add_new_table(
                    &mut new_version_delta,
                    &vec![mv_table_id],
                    StaticCompactionGroupId::MaterializedView as u64,
                    &mut table_compaction_group_mapping,
                )?;

                new_table_ids.extend(new_table_id_to_cg.into_iter());
            }
        }

        let mut modified_compaction_groups = vec![];
        let commit_sstables = self
            .correct_commit_ssts(sstables, &table_compaction_group_mapping)
            .await?;

        self.pre_commit_ssts_to_delta(
            epoch,
            commit_sstables,
            new_table_ids,
            &mut new_version_delta,
            &mut modified_compaction_groups,
        );
        new_version_delta.pre_apply();

        // TODO: remove the sanity check when supporting partial checkpoint
        assert_eq!(1, table_committed_epoch.len());
        assert_eq!(
            table_committed_epoch.iter().next().expect("non-empty"),
            (
                &epoch,
                &version
                    .latest_version()
                    .state_table_info
                    .info()
                    .keys()
                    .cloned()
                    .collect()
            )
        );

        // Apply stats changes.
        let mut version_stats = HummockVersionStatsTransaction::new(
            &mut versioning.version_stats,
            self.env.notification_manager(),
        );
        add_prost_table_stats_map(&mut version_stats.table_stats, &table_stats_change);
        if purge_prost_table_stats(&mut version_stats.table_stats, version.latest_version()) {
            self.metrics.version_stats.reset();
            versioning.local_metrics.clear();
        }

        trigger_local_table_stat(
            &self.metrics,
            &mut versioning.local_metrics,
            &version_stats,
            &table_stats_change,
        );
        for (table_id, stats) in &table_stats_change {
            if stats.total_key_size == 0
                && stats.total_value_size == 0
                && stats.total_key_count == 0
            {
                continue;
            }
            let stats_value = std::cmp::max(0, stats.total_key_size + stats.total_value_size);
            let table_metrics = get_or_create_local_table_stat(
                &self.metrics,
                *table_id,
                &mut versioning.local_metrics,
            );
            table_metrics.inc_write_throughput(stats_value as u64);
        }

        commit_multi_var!(self.meta_store_ref(), version, version_stats)?;

        let snapshot = HummockSnapshot {
            committed_epoch: epoch,
            current_epoch: epoch,
        };
        let prev_snapshot = self.latest_snapshot.swap(snapshot.clone().into());
        assert!(prev_snapshot.committed_epoch < epoch);
        assert!(prev_snapshot.current_epoch < epoch);

        for compaction_group_id in &modified_compaction_groups {
            trigger_sst_stat(
                &self.metrics,
                None,
                &versioning.current_version,
                *compaction_group_id,
            );
        }

        drop(versioning_guard);
        tracing::trace!("new committed epoch {}", epoch);

        // Don't trigger compactions if we enable deterministic compaction
        if !self.env.opts.compaction_deterministic_test {
            // commit_epoch may contains SSTs from any compaction group
            for id in &modified_compaction_groups {
                self.try_send_compaction_request(*id, compact_task::TaskType::Dynamic);
            }
            if !table_stats_change.is_empty() {
                self.collect_table_write_throughput(table_stats_change);
            }
        }
        if !modified_compaction_groups.is_empty() {
            self.try_update_write_limits(&modified_compaction_groups)
                .await;
        }
        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }
        Ok(Some(snapshot))
    }

    fn collect_table_write_throughput(&self, table_stats: PbTableStatsMap) {
        let mut table_infos = self.history_table_throughput.write();
        for (table_id, stat) in table_stats {
            let throughput = (stat.total_value_size + stat.total_key_size) as u64;
            let entry = table_infos.entry(table_id).or_default();
            entry.push_back(throughput);
            if entry.len() > HISTORY_TABLE_INFO_STATISTIC_TIME {
                entry.pop_front();
            }
        }
    }

    fn on_handle_add_new_table(
        &self,
        new_version_delta: &mut SingleDeltaTransaction<'_, '_>,
        table_ids: &Vec<TableId>,
        compaction_group_id: CompactionGroupId,
        table_compaction_group_mapping: &mut HashMap<TableId, CompactionGroupId>,
    ) -> Result<HashMap<TableId, CompactionGroupId>> {
        if table_ids.is_empty() {
            return Err(Error::CompactionGroup("empty table ids".to_string()));
        }

        let mut new_table_ids = HashMap::new();

        for table_id in table_ids {
            if let Some(info) = new_version_delta
                .latest_version()
                .state_table_info
                .info()
                .get(table_id)
            {
                return Err(Error::CompactionGroup(format!(
                    "table {} already exist {:?}",
                    table_id.table_id, info,
                )));
            }
            table_compaction_group_mapping.insert(*table_id, compaction_group_id);
            new_table_ids.insert(*table_id, compaction_group_id);
        }

        Ok(new_table_ids)
    }

    async fn correct_commit_ssts(
        &self,
        sstables: Vec<LocalSstableInfo>,
        table_compaction_group_mapping: &HashMap<TableId, CompactionGroupId>,
    ) -> Result<BTreeMap<u64, Vec<SstableInfo>>> {
        let mut new_sst_id_number = 0;
        let mut sst_to_cg_vec = Vec::with_capacity(sstables.len());
        for commit_sst in sstables {
            let mut group_table_ids: BTreeMap<u64, Vec<u32>> = BTreeMap::new();
            for table_id in commit_sst.sst_info.get_table_ids() {
                match table_compaction_group_mapping.get(&TableId::new(*table_id)) {
                    Some(cg_id_from_meta) => {
                        group_table_ids
                            .entry(*cg_id_from_meta)
                            .or_default()
                            .push(*table_id);
                    }
                    None => {
                        tracing::warn!(
                            "table {} in SST {} doesn't belong to any compaction group",
                            table_id,
                            commit_sst.sst_info.get_object_id(),
                        );
                    }
                }
            }

            new_sst_id_number += group_table_ids.len();
            sst_to_cg_vec.push((commit_sst, group_table_ids));
        }
        let mut new_sst_id = next_sstable_object_id(&self.env, new_sst_id_number).await?;
        let mut commit_sstables: BTreeMap<u64, Vec<SstableInfo>> = BTreeMap::new();

        for (mut sst, group_table_ids) in sst_to_cg_vec {
            for (group_id, _match_ids) in group_table_ids {
                let branch_sst = split_sst(&mut sst.sst_info, &mut new_sst_id);
                commit_sstables
                    .entry(group_id)
                    .or_default()
                    .push(branch_sst);
            }
        }

        Ok(commit_sstables)
    }

    fn pre_commit_ssts_to_delta(
        &self,
        epoch: HummockEpoch,
        commit_sstables: BTreeMap<CompactionGroupId, Vec<SstableInfo>>,
        new_table_ids: HashMap<TableId, CompactionGroupId>,
        new_version_delta: &mut SingleDeltaTransaction<'_, '_>,
        modified_compaction_groups: &mut Vec<CompactionGroupId>,
    ) {
        // Append SSTs to a new version.
        for (compaction_group_id, inserted_table_infos) in commit_sstables {
            modified_compaction_groups.push(compaction_group_id);
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;
            let l0_sub_level_id = epoch;
            let group_delta = GroupDelta {
                delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
                    level_idx: 0,
                    inserted_table_infos,
                    l0_sub_level_id,
                    ..Default::default()
                })),
            };
            group_deltas.push(group_delta);
        }

        // update state table info
        new_version_delta.with_latest_version(|version, delta| {
            for (table_id, cg_id) in new_table_ids {
                delta.state_table_info_delta.insert(
                    table_id,
                    StateTableInfoDelta {
                        committed_epoch: epoch,
                        safe_epoch: epoch,
                        compaction_group_id: cg_id,
                    },
                );
            }

            for (table_id, info) in version.state_table_info.info() {
                assert!(
                    delta
                        .state_table_info_delta
                        .insert(
                            *table_id,
                            StateTableInfoDelta {
                                committed_epoch: epoch,
                                safe_epoch: info.safe_epoch,
                                compaction_group_id: info.compaction_group_id,
                            }
                        )
                        .is_none(),
                    "newly added table exists previously: {:?}",
                    table_id
                );
            }
        });
    }
}
