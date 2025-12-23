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
use std::ops::{Deref, DerefMut};

use parking_lot::Mutex;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::change_log::{ChangeLogDelta, TableChangeLog};
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::vector_index::VectorIndexDelta;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{CompactionGroupId, FrontendHummockVersionDelta, HummockVersionId};
use risingwave_meta_model::Epoch;
use risingwave_pb::hummock::{
    CompatibilityVersion, GroupConstruct, HummockVersionDeltas, HummockVersionStats,
    StateTableInfoDelta,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use sea_orm::{ConnectionTrait, EntityTrait};

use super::TableCommittedEpochNotifiers;
use crate::hummock::model::CompactionGroup;
use crate::hummock::model::ext::to_table_change_log_meta_store_model;
use crate::manager::NotificationManager;
use crate::model::{
    InMemValTransaction, MetadataModelResult, Transactional, ValTransaction, VarTransaction,
};
use crate::rpc::metrics::MetaMetrics;

fn trigger_delta_log_stats(metrics: &MetaMetrics, total_number: usize) {
    metrics.delta_log_count.set(total_number as _);
}

fn trigger_version_stat(metrics: &MetaMetrics, current_version: &HummockVersion) {
    metrics
        .version_size
        .set(current_version.estimated_encode_len() as i64);
    metrics
        .current_version_id
        .set(current_version.id.to_u64() as i64);
}

pub(super) struct HummockVersionTransaction<'a> {
    orig_version: &'a mut HummockVersion,
    orig_deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
    orig_table_change_log: &'a mut HashMap<TableId, TableChangeLog>,
    notification_manager: &'a NotificationManager,
    table_committed_epoch_notifiers: Option<&'a Mutex<TableCommittedEpochNotifiers>>,
    meta_metrics: &'a MetaMetrics,

    pre_applied_version: Option<(
        HummockVersion,
        Vec<HummockVersionDelta>,
        HashMap<TableId, TableChangeLog>,
    )>,
    disable_apply_to_txn: bool,
}

impl<'a> HummockVersionTransaction<'a> {
    pub(super) fn new(
        version: &'a mut HummockVersion,
        deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
        table_change_log: &'a mut HashMap<TableId, TableChangeLog>,
        notification_manager: &'a NotificationManager,
        table_committed_epoch_notifiers: Option<&'a Mutex<TableCommittedEpochNotifiers>>,
        meta_metrics: &'a MetaMetrics,
    ) -> Self {
        Self {
            orig_version: version,
            orig_deltas: deltas,
            orig_table_change_log: table_change_log,
            pre_applied_version: None,
            disable_apply_to_txn: false,
            notification_manager,
            table_committed_epoch_notifiers,
            meta_metrics,
        }
    }

    pub(super) fn disable_apply_to_txn(&mut self) {
        assert!(
            self.pre_applied_version.is_none(),
            "should only call disable at the beginning of txn"
        );
        self.disable_apply_to_txn = true;
    }

    pub(super) fn latest_version(&self) -> &HummockVersion {
        if let Some((version, _, _)) = &self.pre_applied_version {
            version
        } else {
            self.orig_version
        }
    }

    pub(super) fn new_delta<'b>(&'b mut self) -> SingleDeltaTransaction<'a, 'b> {
        let delta = self.latest_version().version_delta_after();
        SingleDeltaTransaction {
            version_txn: self,
            delta: Some(delta),
        }
    }

    fn pre_apply(&mut self, delta: HummockVersionDelta) {
        let (version, deltas, table_change_log) =
            self.pre_applied_version.get_or_insert_with(|| {
                (
                    self.orig_version.clone(),
                    Vec::with_capacity(1),
                    self.orig_table_change_log.clone(),
                )
            });
        let changed_table_info = version.apply_version_delta(&delta);
        HummockVersion::apply_change_log_delta(
            table_change_log,
            &delta.change_log_delta,
            &delta.removed_table_ids,
            &delta.state_table_info_delta,
            &changed_table_info,
        );
        deltas.push(delta);
    }

    /// Returns a duplicate delta, used by time travel.
    pub(super) fn pre_commit_epoch(
        &mut self,
        tables_to_commit: &HashMap<TableId, u64>,
        new_compaction_groups: Vec<CompactionGroup>,
        group_id_to_sub_levels: BTreeMap<CompactionGroupId, Vec<Vec<SstableInfo>>>,
        new_table_ids: &HashMap<TableId, CompactionGroupId>,
        new_table_watermarks: HashMap<TableId, TableWatermarks>,
        change_log_delta: HashMap<TableId, ChangeLogDelta>,
        vector_index_delta: HashMap<TableId, VectorIndexDelta>,
        group_id_to_truncate_tables: HashMap<CompactionGroupId, HashSet<TableId>>,
    ) -> HummockVersionDelta {
        let mut new_version_delta = self.new_delta();
        new_version_delta.new_table_watermarks = new_table_watermarks;
        new_version_delta.change_log_delta = change_log_delta;
        new_version_delta.vector_index_delta = vector_index_delta;

        for compaction_group in &new_compaction_groups {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group.group_id())
                .or_default()
                .group_deltas;

            #[expect(deprecated)]
            group_deltas.push(GroupDelta::GroupConstruct(Box::new(GroupConstruct {
                group_config: Some(compaction_group.compaction_config().as_ref().clone()),
                group_id: compaction_group.group_id(),
                parent_group_id: StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId,
                new_sst_start_id: 0, // No need to set it when `NewCompactionGroup`
                table_ids: vec![],
                version: CompatibilityVersion::LATEST as _,
                split_key: None,
            })));
        }

        // Append SSTs to a new version.
        for (compaction_group_id, sub_levels) in group_id_to_sub_levels {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;

            for sub_level in sub_levels {
                group_deltas.push(GroupDelta::NewL0SubLevel(sub_level));
            }
        }

        for (compaction_group_id, table_ids) in group_id_to_truncate_tables {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;

            group_deltas.push(GroupDelta::TruncateTables(table_ids.into_iter().collect()));
        }

        // update state table info
        new_version_delta.with_latest_version(|version, delta| {
            for (table_id, cg_id) in new_table_ids {
                assert!(
                    !version.state_table_info.info().contains_key(table_id),
                    "newly added table exists previously: {:?}",
                    table_id
                );
                let committed_epoch = *tables_to_commit.get(table_id).expect("newly added table must exist in tables_to_commit");
                delta.state_table_info_delta.insert(
                    *table_id,
                    StateTableInfoDelta {
                        committed_epoch,
                        compaction_group_id: *cg_id,
                    },
                );
            }

            for (table_id, committed_epoch) in tables_to_commit {
                if new_table_ids.contains_key(table_id) {
                    continue;
                }
                let info = version.state_table_info.info().get(table_id).unwrap_or_else(|| {
                    panic!("tables_to_commit {:?} contains table_id {} that is not newly added but not exists previously", tables_to_commit, table_id);
                });
                assert!(delta
                    .state_table_info_delta
                    .insert(
                        *table_id,
                        StateTableInfoDelta {
                            committed_epoch: *committed_epoch,
                            compaction_group_id: info.compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        let time_travel_delta = (*new_version_delta).clone();
        new_version_delta.pre_apply();
        time_travel_delta
    }
}

impl InMemValTransaction for HummockVersionTransaction<'_> {
    fn commit(self) {
        if let Some((version, deltas, table_change_log)) = self.pre_applied_version {
            *self.orig_version = version;
            *self.orig_table_change_log = table_change_log;
            if !self.disable_apply_to_txn {
                let pb_deltas = deltas.iter().map(|delta| delta.to_protobuf()).collect();
                self.notification_manager.notify_hummock_without_version(
                    Operation::Add,
                    Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                        version_deltas: pb_deltas,
                    }),
                );
                self.notification_manager.notify_frontend_without_version(
                    Operation::Update,
                    Info::HummockVersionDeltas(HummockVersionDeltas {
                        version_deltas: deltas
                            .iter()
                            .map(|delta| {
                                FrontendHummockVersionDelta::from_delta(delta).to_protobuf()
                            })
                            .collect(),
                    }),
                );
                if let Some(table_committed_epoch_notifiers) = self.table_committed_epoch_notifiers
                {
                    table_committed_epoch_notifiers
                        .lock()
                        .notify_deltas(&deltas);
                }
            }
            for delta in deltas {
                assert!(self.orig_deltas.insert(delta.id, delta.clone()).is_none());
            }

            trigger_delta_log_stats(self.meta_metrics, self.orig_deltas.len());
            trigger_version_stat(self.meta_metrics, self.orig_version);
        }
    }
}

impl<TXN> ValTransaction<TXN> for HummockVersionTransaction<'_>
where
    TXN: ConnectionTrait,
    HummockVersionDelta: Transactional<TXN>,
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if self.disable_apply_to_txn {
            return Ok(());
        }
        if let Some((_, deltas, modified_table_change_log)) = &self.pre_applied_version {
            // These upsert_in_transaction can be batched. However, we know len(deltas) is always 1 currently.
            for delta in deltas {
                delta.upsert_in_transaction(txn).await?;
            }

            let change_log_to_insert = modified_table_change_log
                .iter()
                .flat_map(|(table_id, change_logs)| {
                    change_logs.iter().map(|change_log| (*table_id, change_log))
                })
                .filter(|(table_id, change_log)| {
                    let Some(existing_logs) = self.orig_table_change_log.get(table_id) else {
                        // Insert to meta store if no previous change log found for this table.
                        return true;
                    };
                    // Insert to meta store if no previous change log of this checkpoint epoch found for this table.
                    existing_logs
                        .0
                        .binary_search_by_key(&change_log.checkpoint_epoch, |log| {
                            log.checkpoint_epoch
                        })
                        .is_err()
                });
            let change_log_to_delete = self
                .orig_table_change_log
                .iter()
                .flat_map(|(table_id, change_logs)| {
                    change_logs.iter().map(|change_log| (*table_id, change_log))
                })
                .filter(|(table_id, change_log)| {
                    let Some(modified_logs) = modified_table_change_log.get(table_id) else {
                        // Delete from meta store if no change log found for this table.
                        return true;
                    };
                    // Delete from meta store if no change log of this checkpoint epoch found for this table.
                    modified_logs
                        .0
                        .binary_search_by_key(&change_log.checkpoint_epoch, |log| {
                            log.checkpoint_epoch
                        })
                        .is_err()
                });

            // TODO(ZW): configurable
            let insert_batch_size = 100;
            use futures::stream::{self, StreamExt};
            use sea_orm::{ColumnTrait, Condition, QueryFilter};
            let mut stream = stream::iter(change_log_to_insert).chunks(insert_batch_size);
            while let Some(change_log_batch) = stream.next().await {
                let insert_many = change_log_batch
                    .into_iter()
                    .map(|(table_id, change_log)| {
                        to_table_change_log_meta_store_model(table_id, change_log)
                    })
                    .collect::<Vec<_>>();
                risingwave_meta_model::hummock_table_change_log::Entity::insert_many(insert_many)
                    .on_empty_do_nothing()
                    .exec(txn)
                    .await?;
            }

            // TODO(ZW): configurable
            let delete_batch_size = 100;
            let mut stream = stream::iter(change_log_to_delete).chunks(delete_batch_size);
            while let Some(change_log_batch) = stream.next().await {
                let mut condition = Condition::any();
                for (table_id, change_log) in change_log_batch {
                    condition = condition.add(
                        Condition::all()
                            .add(risingwave_meta_model::hummock_table_change_log::Column::TableId.eq(table_id))
                            .add(risingwave_meta_model::hummock_table_change_log::Column::CheckpointEpoch.eq(change_log.checkpoint_epoch as Epoch))
                    );
                }
                risingwave_meta_model::hummock_table_change_log::Entity::delete_many()
                    .filter(condition)
                    .exec(txn)
                    .await?;
            }
        }
        Ok(())
    }
}

pub(super) struct SingleDeltaTransaction<'a, 'b> {
    version_txn: &'b mut HummockVersionTransaction<'a>,
    delta: Option<HummockVersionDelta>,
}

impl SingleDeltaTransaction<'_, '_> {
    pub(super) fn latest_version(&self) -> &HummockVersion {
        self.version_txn.latest_version()
    }

    pub(super) fn pre_apply(mut self) {
        self.version_txn.pre_apply(self.delta.take().unwrap());
    }

    pub(super) fn with_latest_version(
        &mut self,
        f: impl FnOnce(&HummockVersion, &mut HummockVersionDelta),
    ) {
        f(
            self.version_txn.latest_version(),
            self.delta.as_mut().expect("should exist"),
        )
    }
}

impl Deref for SingleDeltaTransaction<'_, '_> {
    type Target = HummockVersionDelta;

    fn deref(&self) -> &Self::Target {
        self.delta.as_ref().expect("should exist")
    }
}

impl DerefMut for SingleDeltaTransaction<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.delta.as_mut().expect("should exist")
    }
}

impl Drop for SingleDeltaTransaction<'_, '_> {
    fn drop(&mut self) {
        if let Some(delta) = self.delta.take() {
            self.version_txn.pre_apply(delta);
        }
    }
}

pub(super) struct HummockVersionStatsTransaction<'a> {
    stats: VarTransaction<'a, HummockVersionStats>,
    notification_manager: &'a NotificationManager,
}

impl<'a> HummockVersionStatsTransaction<'a> {
    pub(super) fn new(
        stats: &'a mut HummockVersionStats,
        notification_manager: &'a NotificationManager,
    ) -> Self {
        Self {
            stats: VarTransaction::new(stats),
            notification_manager,
        }
    }
}

impl InMemValTransaction for HummockVersionStatsTransaction<'_> {
    fn commit(self) {
        if self.stats.has_new_value() {
            let stats = self.stats.clone();
            self.stats.commit();
            self.notification_manager
                .notify_frontend_without_version(Operation::Update, Info::HummockStats(stats));
        }
    }
}

impl<TXN> ValTransaction<TXN> for HummockVersionStatsTransaction<'_>
where
    TXN: ConnectionTrait,
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        self.stats.apply_to_txn(txn).await
    }
}

impl Deref for HummockVersionStatsTransaction<'_> {
    type Target = HummockVersionStats;

    fn deref(&self) -> &Self::Target {
        self.stats.deref()
    }
}

impl DerefMut for HummockVersionStatsTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stats.deref_mut()
    }
}
