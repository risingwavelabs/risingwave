// Copyright 2025 RisingWave Labs
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

use itertools::Itertools;
use risingwave_hummock_sdk::version::HummockVersionDelta;
use risingwave_meta_model::compaction_config::CompactionConfig;
use risingwave_meta_model::compaction_status::LevelHandlers;
use risingwave_meta_model::compaction_task::CompactionTask;
use risingwave_meta_model::hummock_version_delta::FullVersionDelta;
use risingwave_meta_model::hummock_version_stats::TableStats;
use risingwave_meta_model::{
    CompactionGroupId, CompactionTaskId, HummockVersionId, WorkerId, compaction_config,
    compaction_status, compaction_task, hummock_pinned_snapshot, hummock_pinned_version,
    hummock_version_delta, hummock_version_stats,
};
use risingwave_pb::hummock::{
    CompactTaskAssignment, HummockPinnedSnapshot, HummockPinnedVersion, HummockVersionStats,
};
use sea_orm::ActiveValue::Set;
use sea_orm::EntityTrait;
use sea_orm::sea_query::OnConflict;

use crate::hummock::compaction::CompactStatus;
use crate::hummock::model::CompactionGroup;
use crate::model::{MetadataModelError, MetadataModelResult, Transactional};
use crate::storage::MetaStoreError;

pub type Transaction = sea_orm::DatabaseTransaction;

impl From<sea_orm::DbErr> for MetadataModelError {
    fn from(err: sea_orm::DbErr) -> Self {
        MetadataModelError::MetaStoreError(MetaStoreError::Internal(err.into()))
    }
}

// TODO: reduce boilerplate code

#[async_trait::async_trait]
impl Transactional<Transaction> for CompactionGroup {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = compaction_config::ActiveModel {
            compaction_group_id: Set(self.group_id.try_into().unwrap()),
            config: Set(CompactionConfig::from(&(*self.compaction_config))),
        };
        compaction_config::Entity::insert(m)
            .on_conflict(
                OnConflict::column(compaction_config::Column::CompactionGroupId)
                    .update_columns([compaction_config::Column::Config])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        compaction_config::Entity::delete_by_id(
            CompactionGroupId::try_from(self.group_id).unwrap(),
        )
        .exec(trx)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for CompactStatus {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = compaction_status::ActiveModel {
            compaction_group_id: Set(self.compaction_group_id.try_into().unwrap()),
            status: Set(LevelHandlers::from(
                self.level_handlers.iter().map_into().collect_vec(),
            )),
        };
        compaction_status::Entity::insert(m)
            .on_conflict(
                OnConflict::column(compaction_status::Column::CompactionGroupId)
                    .update_columns([compaction_status::Column::Status])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        compaction_status::Entity::delete_by_id(
            CompactionGroupId::try_from(self.compaction_group_id).unwrap(),
        )
        .exec(trx)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for CompactTaskAssignment {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let task = self.compact_task.to_owned().unwrap();
        let m = compaction_task::ActiveModel {
            id: Set(task.task_id.try_into().unwrap()),
            context_id: Set(self.context_id.try_into().unwrap()),
            task: Set(CompactionTask::from(&task)),
        };
        compaction_task::Entity::insert(m)
            .on_conflict(
                OnConflict::column(compaction_task::Column::Id)
                    .update_columns([
                        compaction_task::Column::ContextId,
                        compaction_task::Column::Task,
                    ])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        compaction_task::Entity::delete_by_id(
            CompactionTaskId::try_from(self.compact_task.as_ref().unwrap().task_id).unwrap(),
        )
        .exec(trx)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockPinnedVersion {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_pinned_version::ActiveModel {
            context_id: Set(self.context_id.try_into().unwrap()),
            min_pinned_id: Set(self.min_pinned_id.try_into().unwrap()),
        };
        hummock_pinned_version::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_pinned_version::Column::ContextId)
                    .update_columns([hummock_pinned_version::Column::MinPinnedId])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        hummock_pinned_version::Entity::delete_by_id(WorkerId::try_from(self.context_id).unwrap())
            .exec(trx)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockPinnedSnapshot {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_pinned_snapshot::ActiveModel {
            context_id: Set(self.context_id.try_into().unwrap()),
            min_pinned_snapshot: Set(self.minimal_pinned_snapshot.try_into().unwrap()),
        };
        hummock_pinned_snapshot::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_pinned_snapshot::Column::ContextId)
                    .update_columns([hummock_pinned_snapshot::Column::MinPinnedSnapshot])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        hummock_pinned_snapshot::Entity::delete_by_id(WorkerId::try_from(self.context_id).unwrap())
            .exec(trx)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockVersionStats {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_version_stats::ActiveModel {
            id: Set(self.hummock_version_id.try_into().unwrap()),
            stats: Set(TableStats(self.table_stats.clone())),
        };
        hummock_version_stats::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_version_stats::Column::Id)
                    .update_columns([hummock_version_stats::Column::Stats])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        hummock_version_stats::Entity::delete_by_id(
            HummockVersionId::try_from(self.hummock_version_id).unwrap(),
        )
        .exec(trx)
        .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockVersionDelta {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_version_delta::ActiveModel {
            id: Set(self.id.to_u64().try_into().unwrap()),
            prev_id: Set(self.prev_id.to_u64().try_into().unwrap()),
            max_committed_epoch: Set(0.into()),
            safe_epoch: Set(0.into()),
            trivial_move: Set(self.trivial_move),
            full_version_delta: Set(FullVersionDelta::from(&self.into())),
        };
        hummock_version_delta::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_version_delta::Column::Id)
                    .update_columns([
                        hummock_version_delta::Column::PrevId,
                        hummock_version_delta::Column::MaxCommittedEpoch,
                        hummock_version_delta::Column::SafeEpoch,
                        hummock_version_delta::Column::TrivialMove,
                        hummock_version_delta::Column::FullVersionDelta,
                    ])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        hummock_version_delta::Entity::delete_by_id(
            HummockVersionId::try_from(self.id.to_u64()).unwrap(),
        )
        .exec(trx)
        .await?;
        Ok(())
    }
}

impl From<compaction_config::Model> for CompactionGroup {
    fn from(value: compaction_config::Model) -> Self {
        Self::new(
            value.compaction_group_id.try_into().unwrap(),
            value.config.to_protobuf(),
        )
    }
}

impl From<compaction_status::Model> for CompactStatus {
    fn from(value: compaction_status::Model) -> Self {
        Self {
            compaction_group_id: value.compaction_group_id.try_into().unwrap(),
            level_handlers: value.status.to_protobuf().iter().map_into().collect(),
        }
    }
}
