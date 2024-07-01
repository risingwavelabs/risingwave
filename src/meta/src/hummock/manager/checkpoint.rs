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

use std::collections::{HashMap, VecDeque};
use std::ops::Bound::{Excluded, Included};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::Ordering;

use anyhow::anyhow;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    object_size_map, summarize_group_deltas,
};
use risingwave_hummock_sdk::time_travel::{
    refill_version, IncompleteHummockVersion, IncompleteHummockVersionDelta,
};
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{HummockEpoch, HummockVersionId};
use risingwave_meta_model_v2::{
    hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_archive,
};
use risingwave_pb::hummock::hummock_version_checkpoint::{
    StaleObjects as PbStaleObjects, StaleObjects,
};
use risingwave_pb::hummock::{
    PbHummockVersion, PbHummockVersionArchive, PbHummockVersionCheckpoint, PbHummockVersionDelta,
    PbSstableInfo,
};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, TransactionTrait};
use thiserror_ext::AsReport;
use tracing::warn;

use crate::controller::SqlMetaStore;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::versioning::Versioning;
use crate::hummock::metrics_utils::{trigger_gc_stat, trigger_split_stat};
use crate::hummock::HummockManager;
use crate::manager::MetaStoreImpl;

#[derive(Default)]
pub struct HummockVersionCheckpoint {
    pub version: HummockVersion,

    /// stale objects of versions before the current checkpoint.
    ///
    /// Previously we stored the stale object of each single version.
    /// Currently we will merge the stale object between two checkpoints, and only the
    /// id of the checkpointed hummock version are included in the map.
    pub stale_objects: HashMap<HummockVersionId, PbStaleObjects>,
}

impl HummockVersionCheckpoint {
    pub fn from_protobuf(checkpoint: &PbHummockVersionCheckpoint) -> Self {
        Self {
            version: HummockVersion::from_persisted_protobuf(checkpoint.version.as_ref().unwrap()),
            stale_objects: checkpoint
                .stale_objects
                .iter()
                .map(|(version_id, objects)| (*version_id as HummockVersionId, objects.clone()))
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionCheckpoint {
        PbHummockVersionCheckpoint {
            version: Some(self.version.to_protobuf()),
            stale_objects: self.stale_objects.clone(),
        }
    }
}

/// A hummock version checkpoint compacts previous hummock version delta logs, and stores stale
/// objects from those delta logs.
impl HummockManager {
    /// Returns Ok(None) if not found.
    pub async fn try_read_checkpoint(&self) -> Result<Option<HummockVersionCheckpoint>> {
        use prost::Message;
        let data = match self
            .object_store
            .read(&self.version_checkpoint_path, ..)
            .await
        {
            Ok(data) => data,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let ckpt = PbHummockVersionCheckpoint::decode(data).map_err(|e| anyhow::anyhow!(e))?;
        Ok(Some(HummockVersionCheckpoint::from_protobuf(&ckpt)))
    }

    pub(super) async fn write_checkpoint(
        &self,
        checkpoint: &HummockVersionCheckpoint,
    ) -> Result<()> {
        use prost::Message;
        let buf = checkpoint.to_protobuf().encode_to_vec();
        self.object_store
            .upload(&self.version_checkpoint_path, buf.into())
            .await?;
        Ok(())
    }

    pub(super) async fn write_version_archive(
        &self,
        archive: &PbHummockVersionArchive,
    ) -> Result<()> {
        use prost::Message;
        let buf = archive.encode_to_vec();
        let archive_path = format!(
            "{}/{}",
            self.version_archive_dir,
            archive.version.as_ref().unwrap().id
        );
        self.object_store.upload(&archive_path, buf.into()).await?;
        Ok(())
    }

    /// Creates a hummock version checkpoint.
    /// Returns the diff between new and old checkpoint id.
    /// Note that this method must not be called concurrently, because internally it doesn't hold
    /// lock throughout the method.
    pub async fn create_version_checkpoint(&self, min_delta_log_num: u64) -> Result<u64> {
        let timer = self.metrics.version_checkpoint_latency.start_timer();
        // 1. hold read lock and create new checkpoint
        let versioning_guard = self.versioning.read().await;
        let versioning: &Versioning = versioning_guard.deref();
        let current_version: &HummockVersion = &versioning.current_version;
        let old_checkpoint: &HummockVersionCheckpoint = &versioning.checkpoint;
        let new_checkpoint_id = current_version.id;
        let old_checkpoint_id = old_checkpoint.version.id;
        if new_checkpoint_id < old_checkpoint_id + min_delta_log_num {
            return Ok(0);
        }
        if cfg!(test) && new_checkpoint_id == old_checkpoint_id {
            drop(versioning_guard);
            let versioning = self.versioning.read().await;
            let context_info = self.context_info.read().await;
            versioning.mark_objects_for_deletion(&context_info, &self.delete_object_tracker);
            let min_pinned_version_id = context_info.min_pinned_version_id();
            trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
            return Ok(0);
        }
        assert!(new_checkpoint_id > old_checkpoint_id);
        let mut archive: Option<PbHummockVersionArchive> = None;
        let mut time_travel_archive: Option<PbHummockVersionArchive> = None;
        let mut stale_objects = old_checkpoint.stale_objects.clone();
        // `object_sizes` is used to calculate size of stale objects.
        let mut object_sizes = object_size_map(&old_checkpoint.version);
        // The set of object ids that once exist in any hummock version
        let mut versions_object_ids = old_checkpoint.version.get_object_ids();
        for (_, version_delta) in versioning
            .hummock_version_deltas
            .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
        {
            for group_deltas in version_delta.group_deltas.values() {
                let summary = summarize_group_deltas(group_deltas);
                object_sizes.extend(
                    summary
                        .insert_table_infos
                        .iter()
                        .map(|t| (t.object_id, t.file_size))
                        .chain(
                            version_delta
                                .change_log_delta
                                .values()
                                .flat_map(|change_log| {
                                    let new_log = change_log.new_log.as_ref().unwrap();
                                    new_log
                                        .new_value
                                        .iter()
                                        .chain(new_log.old_value.iter())
                                        .map(|t| (t.object_id, t.file_size))
                                }),
                        ),
                );
            }
            versions_object_ids.extend(version_delta.newly_added_object_ids());
        }

        // Object ids that once exist in any hummock version but not exist in the latest hummock version
        let removed_object_ids = &versions_object_ids - &current_version.get_object_ids();
        let total_file_size = removed_object_ids
            .iter()
            .map(|t| {
                object_sizes.get(t).copied().unwrap_or_else(|| {
                    warn!(object_id = t, "unable to get size of removed object id");
                    0
                })
            })
            .sum::<u64>();
        stale_objects.insert(
            current_version.id,
            StaleObjects {
                id: removed_object_ids.into_iter().collect(),
                total_file_size,
            },
        );
        if self.env.opts.enable_hummock_time_travel {
            time_travel_archive = Some(create_time_travel_archive(
                &old_checkpoint.version.to_protobuf(),
                &versioning
                    .hummock_version_deltas
                    .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
                    .map(|(_, version_delta)| version_delta.to_protobuf())
                    .collect::<Vec<_>>(),
            ));
        }
        if self.env.opts.enable_hummock_data_archive {
            archive = Some(PbHummockVersionArchive {
                version: Some(old_checkpoint.version.to_protobuf()),
                version_deltas: versioning
                    .hummock_version_deltas
                    .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
                    .map(|(_, version_delta)| version_delta.to_protobuf())
                    .collect(),
            });
        }
        let new_checkpoint = HummockVersionCheckpoint {
            version: current_version.clone(),
            stale_objects,
        };
        drop(versioning_guard);
        // 2. persist the new checkpoint without holding lock
        if let Some(time_travel_archive) = time_travel_archive {
            // write_time_travel_archive must precede write_checkpoint.
            self.write_time_travel_archive(time_travel_archive).await?;
        }
        self.write_checkpoint(&new_checkpoint).await?;
        if let Some(archive) = archive {
            if let Err(e) = self.write_version_archive(&archive).await {
                tracing::warn!(
                    error = %e.as_report(),
                    "failed to write version archive {}",
                    archive.version.as_ref().unwrap().id
                );
            }
        }
        // 3. hold write lock and update in memory state
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let context_info = self.context_info.read().await;
        assert!(new_checkpoint.version.id > versioning.checkpoint.version.id);
        versioning.checkpoint = new_checkpoint;
        // Not delete stale objects when archive or time travel is enabled
        if !self.env.opts.enable_hummock_data_archive && !self.env.opts.enable_hummock_time_travel {
            versioning.mark_objects_for_deletion(&context_info, &self.delete_object_tracker);
        }

        let min_pinned_version_id = context_info.min_pinned_version_id();
        trigger_gc_stat(&self.metrics, &versioning.checkpoint, min_pinned_version_id);
        trigger_split_stat(&self.metrics, &versioning.current_version);
        drop(versioning_guard);
        timer.observe_duration();
        self.metrics
            .checkpoint_version_id
            .set(new_checkpoint_id as i64);

        Ok(new_checkpoint_id - old_checkpoint_id)
    }

    pub fn pause_version_checkpoint(&self) {
        self.pause_version_checkpoint.store(true, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is paused.");
    }

    pub fn resume_version_checkpoint(&self) {
        self.pause_version_checkpoint
            .store(false, Ordering::Relaxed);
        tracing::info!("hummock version checkpoint is resumed.");
    }

    pub fn is_version_checkpoint_paused(&self) -> bool {
        self.pause_version_checkpoint.load(Ordering::Relaxed)
    }

    pub async fn get_checkpoint_version(&self) -> HummockVersion {
        let versioning_guard = self.versioning.read().await;
        versioning_guard.checkpoint.version.clone()
    }
}

/// Time travel.
impl HummockManager {
    fn sql_store(&self) -> Result<SqlMetaStore> {
        match self.env.meta_store() {
            MetaStoreImpl::Sql(sql_store) => Ok(sql_store),
            _ => Err(anyhow!("time travel requires SQL meta store").into()),
        }
    }

    async fn write_time_travel_archive(&self, archive: PbHummockVersionArchive) -> Result<()> {
        let sql_store = self.sql_store()?;
        let first_version_id = archive.version.as_ref().unwrap().id.try_into().unwrap();
        let last_version_id = archive
            .version_deltas
            .last()
            .map(|d| d.id.try_into().unwrap())
            .unwrap_or(first_version_id);
        let m = hummock_time_travel_archive::ActiveModel {
            first_version_id: Set(first_version_id),
            last_version_id: Set(last_version_id),
            first_version: Set(archive.version.as_ref().unwrap().into()),
            version_deltas: Set(archive.version_deltas.into()),
        };
        hummock_time_travel_archive::Entity::insert(m)
            .on_conflict(
                OnConflict::column(hummock_time_travel_archive::Column::FirstVersionId)
                    .update_columns([
                        hummock_time_travel_archive::Column::LastVersionId,
                        hummock_time_travel_archive::Column::FirstVersion,
                        hummock_time_travel_archive::Column::VersionDeltas,
                    ])
                    .to_owned(),
            )
            .exec(&sql_store.conn)
            .await?;
        Ok(())
    }

    pub(crate) async fn write_sstable_infos(
        &self,
        sst_infos: impl Iterator<Item = &PbSstableInfo>,
    ) -> Result<()> {
        let sql_store = self.sql_store()?;
        let txn = sql_store.conn.begin().await?;
        for sst_info in sst_infos {
            let m = hummock_sstable_info::ActiveModel {
                sst_id: Set(sst_info.sst_id.try_into().unwrap()),
                object_id: Set(sst_info.object_id.try_into().unwrap()),
                sstable_info: Set(sst_info.into()),
            };
            hummock_sstable_info::Entity::insert(m).exec(&txn).await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn write_epoch_to_version(
        &self,
        epoch: HummockEpoch,
        version_id: HummockVersionId,
    ) -> Result<()> {
        let sql_store = self.sql_store()?;
        let m = hummock_epoch_to_version::ActiveModel {
            epoch: Set(epoch.try_into().unwrap()),
            version_id: Set(version_id.try_into().unwrap()),
        };
        hummock_epoch_to_version::Entity::insert(m)
            .exec(&sql_store.conn)
            .await?;
        Ok(())
    }

    pub(crate) async fn truncate_time_travel_metadata(
        &self,
        epoch_watermark: HummockEpoch,
    ) -> Result<()> {
        let sql_store = self.sql_store()?;
        let txn = sql_store.conn.begin().await?;

        let version_watermark = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model_v2::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&txn)
            .await?;
        let Some(version_watermark) = version_watermark else {
            return Ok(());
        };
        let version_id_watermark = version_watermark.version_id;
        let res = hummock_epoch_to_version::Entity::delete_many()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model_v2::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark,
            "delete {} rows from hummock_epoch_to_version",
            res.rows_affected
        );

        // TODO: maybe index LastVersionId
        let stale_archives = hummock_time_travel_archive::Entity::find()
            .filter(hummock_time_travel_archive::Column::LastVersionId.lte(
                risingwave_meta_model_v2::HummockVersionId::try_from(version_id_watermark).unwrap(),
            ))
            .order_by_asc(hummock_time_travel_archive::Column::FirstVersionId)
            .all(&txn)
            .await?;
        let res = hummock_time_travel_archive::Entity::delete_many()
            .filter(
                hummock_time_travel_archive::Column::FirstVersionId
                    .is_in(stale_archives.iter().map(|m| m.first_version_id)),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            version_id_watermark,
            "delete {} rows from hummock_time_travel_archive",
            res.rows_affected
        );

        let earliest_valid_archive = hummock_time_travel_archive::Entity::find()
            .filter(hummock_time_travel_archive::Column::LastVersionId.gt(
                risingwave_meta_model_v2::HummockVersionId::try_from(version_id_watermark).unwrap(),
            ))
            .order_by_asc(hummock_time_travel_archive::Column::FirstVersionId)
            .one(&txn)
            .await?;
        let Some(earliest_valid_archive) = earliest_valid_archive else {
            return Ok(());
        };
        let earliest_valid_version_sst_ids = HummockVersion::from_persisted_protobuf(
            &earliest_valid_archive.first_version.to_protobuf(),
        )
        .get_sst_ids();
        for stale_archive in stale_archives {
            let last_version = replay_archive(&stale_archive, None);
            let removed_sst_ids = &last_version.get_sst_ids() - &earliest_valid_version_sst_ids;
            tracing::debug!(?removed_sst_ids, "remove SstableInfo");
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(removed_sst_ids))
                .exec(&txn)
                .await?;
            tracing::debug!(
                archive_first_version_id = stale_archive.first_version_id,
                archive_last_version_id = stale_archive.last_version_id,
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
        }

        txn.commit().await?;
        Ok(())
    }

    /// Attempt to locate the version corresponding to `query_epoch`.
    ///
    /// The version is retrieved from `hummock_epoch_to_version`, selecting the entry with the largest epoch that's lte `query_epoch`.
    ///
    /// The resulted version is complete, i.e. with correct `SstableInfo`.
    pub async fn epoch_to_version(&self, query_epoch: HummockEpoch) -> Result<HummockVersion> {
        let sql_store = self.sql_store()?;
        let epoch_to_version = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lte(risingwave_meta_model_v2::Epoch::try_from(query_epoch).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "version not found for epoch {}",
                    query_epoch
                )))
            })?;
        let version_id = epoch_to_version.version_id;
        // TODO: As archive is taken asynchronously, a very recent epoch may not have its archive available yet.
        let archive = hummock_time_travel_archive::Entity::find()
            .filter(hummock_time_travel_archive::Column::FirstVersionId.lte(version_id))
            .filter(hummock_time_travel_archive::Column::LastVersionId.gte(version_id))
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "archive not found for epoch {}, version {}",
                    query_epoch, version_id,
                )))
            })?;
        let mut version = replay_archive(
            &archive,
            Some(HummockVersionId::try_from(version_id).unwrap()),
        );

        let mut sst_ids = version.get_sst_ids().into_iter().collect::<VecDeque<_>>();
        let sst_count = sst_ids.len();
        let mut sst_id_to_info = HashMap::with_capacity(sst_count);
        let sst_info_fetch_batch_size = 500;
        while !sst_ids.is_empty() {
            let sst_infos = hummock_sstable_info::Entity::find()
                .filter(hummock_sstable_info::Column::SstId.is_in(
                    sst_ids.drain(..std::cmp::min(sst_info_fetch_batch_size, sst_ids.len())),
                ))
                .all(&sql_store.conn)
                .await?;
            for sst_info in sst_infos {
                let sst_info = sst_info.sstable_info.to_protobuf();
                sst_id_to_info.insert(sst_info.sst_id, sst_info);
            }
        }
        tracing::debug!(expected = sst_count, actual = sst_id_to_info.len(), "!!!");
        if sst_count != sst_id_to_info.len() {
            return Err(Error::TimeTravel(anyhow!(format!(
                "some SstableInfos not found for epoch {}, version {}",
                query_epoch, version_id,
            ))));
        }
        refill_version(&mut version, &sst_id_to_info);
        Ok(version)
    }
}

fn create_time_travel_archive(
    version: &PbHummockVersion,
    deltas: &[PbHummockVersionDelta],
) -> PbHummockVersionArchive {
    let mut current_mce = version.max_committed_epoch;
    PbHummockVersionArchive {
        version: Some(IncompleteHummockVersion::from_protobuf(version).to_protobuf()),
        version_deltas: deltas
            .iter()
            .filter(|d| {
                tracing::debug!(current_mce, delta_mce = d.max_committed_epoch, "!!!");
                let is_commit_epoch = d.max_committed_epoch > current_mce;
                current_mce = d.max_committed_epoch;
                is_commit_epoch
            })
            .map(|d| IncompleteHummockVersionDelta::from_protobuf(d).to_protobuf())
            .collect(),
    }
}

fn replay_archive(
    archive: &hummock_time_travel_archive::Model,
    to_version_id: Option<HummockVersionId>,
) -> HummockVersion {
    let mut last_version =
        HummockVersion::from_persisted_protobuf(&archive.first_version.to_protobuf());
    let version_id_watermark = to_version_id.unwrap_or(HummockVersionId::MAX);
    for d in archive.version_deltas.to_protobuf() {
        let d = HummockVersionDelta::from_persisted_protobuf(&d);
        // Need to work around the assertion in `apply_version_delta`.
        // Because compaction deltas are not included in time travel archive.
        while last_version.id < d.prev_id {
            if last_version.id >= version_id_watermark {
                return last_version;
            }
            last_version.id += 1;
        }
        if last_version.id >= version_id_watermark {
            return last_version;
        }
        last_version.apply_version_delta(&d);
    }
    last_version
}
