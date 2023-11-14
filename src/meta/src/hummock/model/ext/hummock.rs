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

use itertools::Itertools;
use risingwave_meta_model_v2::compaction_config::CompactionConfig;
use risingwave_meta_model_v2::compaction_status::LevelHandlers;
use risingwave_meta_model_v2::compaction_task::CompactionTask;
use risingwave_meta_model_v2::hummock_version_delta::GroupDeltas;
use risingwave_meta_model_v2::{
    compaction_config, compaction_status, compaction_task, hummock_pinned_snapshot,
    hummock_pinned_version, hummock_version_delta, CompactionGroupId, CompactionTaskId,
    HummockVersionId, WorkerId,
};
use risingwave_pb::hummock::{
    CompactTaskAssignment, HummockPinnedSnapshot, HummockPinnedVersion, HummockVersionDelta,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::EntityTrait;

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
            compaction_group_id: Set(self.group_id as _),
            config: Set(CompactionConfig((*self.compaction_config).to_owned())),
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
        compaction_config::Entity::delete_by_id(self.group_id as CompactionGroupId)
            .exec(trx)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for CompactStatus {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = compaction_status::ActiveModel {
            compaction_group_id: Set(self.compaction_group_id as _),
            status: Set(LevelHandlers(
                self.level_handlers.iter().map_into().collect(),
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
        compaction_status::Entity::delete_by_id(self.compaction_group_id as CompactionGroupId)
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
            id: Set(task.task_id as _),
            context_id: Set(self.context_id as _),
            task: Set(CompactionTask(task)),
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
            self.compact_task.as_ref().unwrap().task_id as CompactionTaskId,
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
            context_id: Set(self.context_id as _),
            min_pinned_id: Set(self.min_pinned_id as _),
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
        hummock_pinned_version::Entity::delete_by_id(self.context_id as WorkerId)
            .exec(trx)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockPinnedSnapshot {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_pinned_snapshot::ActiveModel {
            context_id: Set(self.context_id as _),
            min_pinned_snapshot: Set(self.minimal_pinned_snapshot as _),
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
        hummock_pinned_snapshot::Entity::delete_by_id(self.context_id as i32)
            .exec(trx)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Transactional<Transaction> for HummockVersionDelta {
    async fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        let m = hummock_version_delta::ActiveModel {
            id: Set(self.id as _),
            prev_id: Set(self.prev_id as _),
            group_deltas: Set(GroupDeltas(
                self.group_deltas
                    .iter()
                    .map(|(k, v)| (*k as _, v.group_deltas.to_owned()))
                    .collect(),
            )),
            max_committed_epoch: Set(self.max_committed_epoch as _),
            safe_epoch: Set(self.safe_epoch as _),
            trivial_move: Set(self.trivial_move),
            gc_object_ids: Set(self.gc_object_ids.to_owned().into()),
        };
        hummock_version_delta::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_version_delta::Column::Id)
                    .update_columns([
                        hummock_version_delta::Column::PrevId,
                        hummock_version_delta::Column::GroupDeltas,
                        hummock_version_delta::Column::MaxCommittedEpoch,
                        hummock_version_delta::Column::SafeEpoch,
                        hummock_version_delta::Column::TrivialMove,
                        hummock_version_delta::Column::GcObjectIds,
                    ])
                    .to_owned(),
            )
            .exec(trx)
            .await?;
        Ok(())
    }

    async fn delete_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        hummock_version_delta::Entity::delete_by_id(self.id as HummockVersionId)
            .exec(trx)
            .await?;
        Ok(())
    }
}

impl From<compaction_config::Model> for CompactionGroup {
    fn from(value: compaction_config::Model) -> Self {
        Self::new(value.compaction_group_id as _, value.config.0)
    }
}

impl From<compaction_status::Model> for CompactStatus {
    fn from(value: compaction_status::Model) -> Self {
        Self {
            compaction_group_id: value.compaction_group_id as _,
            level_handlers: value.status.0.iter().map_into().collect(),
        }
    }
}
