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

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::time_travel::{
    refill_version, IncompleteHummockVersion, IncompleteHummockVersionDelta,
};
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockEpoch, HummockSstableId, HummockSstableObjectId,
};
use risingwave_meta_model_v2::hummock_sstable_info::SstableInfoV2Backend;
use risingwave_meta_model_v2::{
    hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_delta,
    hummock_time_travel_version,
};
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, Condition, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder, QuerySelect,
    TransactionTrait,
};

use crate::controller::SqlMetaStore;
use crate::hummock::error::{Error, Result};
use crate::hummock::HummockManager;
use crate::manager::MetaStoreImpl;

/// Time travel.
impl HummockManager {
    pub(crate) fn sql_store(&self) -> Option<SqlMetaStore> {
        match self.env.meta_store() {
            MetaStoreImpl::Sql(sql_store) => Some(sql_store),
            _ => None,
        }
    }

    pub(crate) async fn time_travel_enabled(&self) -> bool {
        self.env
            .system_params_reader()
            .await
            .time_travel_retention_ms()
            > 0
            && self.sql_store().is_some()
    }

    pub(crate) async fn init_time_travel_state(&self) -> Result<()> {
        let Some(sql_store) = self.sql_store() else {
            return Ok(());
        };
        let mut gurad = self.versioning.write().await;
        gurad.mark_next_time_travel_version_snapshot();

        gurad.last_time_travel_snapshot_sst_ids = HashSet::new();
        let Some(version) = hummock_time_travel_version::Entity::find()
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .map(|v| HummockVersion::from_persisted_protobuf(&v.version.to_protobuf()))
        else {
            return Ok(());
        };
        gurad.last_time_travel_snapshot_sst_ids = version.get_sst_ids();
        Ok(())
    }

    pub(crate) async fn truncate_time_travel_metadata(
        &self,
        epoch_watermark: HummockEpoch,
    ) -> Result<()> {
        let sql_store = match self.sql_store() {
            Some(sql_store) => sql_store,
            None => {
                return Ok(());
            }
        };
        let txn = sql_store.conn.begin().await?;

        let version_watermark = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model_v2::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .order_by_asc(hummock_epoch_to_version::Column::VersionId)
            .one(&txn)
            .await?;
        let Some(version_watermark) = version_watermark else {
            txn.commit().await?;
            return Ok(());
        };
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
        let earliest_valid_version = hummock_time_travel_version::Entity::find()
            .filter(
                hummock_time_travel_version::Column::VersionId.lte(version_watermark.version_id),
            )
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&txn)
            .await?
            .map(|m| HummockVersion::from_persisted_protobuf(&m.version.to_protobuf()));
        let Some(earliest_valid_version) = earliest_valid_version else {
            txn.commit().await?;
            return Ok(());
        };
        let (earliest_valid_version_id, earliest_valid_version_sst_ids) = {
            (
                earliest_valid_version.id,
                earliest_valid_version.get_sst_ids(),
            )
        };
        let version_ids_to_delete: Vec<risingwave_meta_model_v2::HummockVersionId> =
            hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .filter(
                    hummock_time_travel_version::Column::VersionId
                        .lt(earliest_valid_version_id.to_u64()),
                )
                .order_by_desc(hummock_time_travel_version::Column::VersionId)
                .into_tuple()
                .all(&txn)
                .await?;
        let delta_ids_to_delete: Vec<risingwave_meta_model_v2::HummockVersionId> =
            hummock_time_travel_delta::Entity::find()
                .select_only()
                .column(hummock_time_travel_delta::Column::VersionId)
                .filter(
                    hummock_time_travel_delta::Column::VersionId
                        .lt(earliest_valid_version_id.to_u64()),
                )
                .into_tuple()
                .all(&txn)
                .await?;
        for delta_id_to_delete in delta_ids_to_delete {
            let delta_to_delete = hummock_time_travel_delta::Entity::find_by_id(delta_id_to_delete)
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    Error::TimeTravel(anyhow!(format!(
                        "version delta {} not found",
                        delta_id_to_delete
                    )))
                })?;
            let new_sst_ids = HummockVersionDelta::from_persisted_protobuf(
                &delta_to_delete.version_delta.to_protobuf(),
            )
            .newly_added_sst_ids();
            // The SST ids added and then deleted by compaction between the 2 versions.
            let sst_ids_to_delete = &new_sst_ids - &earliest_valid_version_sst_ids;
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            tracing::debug!(
                delta_id = delta_to_delete.version_id,
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
        }
        let mut next_version_sst_ids = earliest_valid_version_sst_ids;
        for prev_version_id in version_ids_to_delete {
            let sst_ids = {
                let prev_version = hummock_time_travel_version::Entity::find_by_id(prev_version_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "prev_version {} not found",
                            prev_version_id
                        )))
                    })?;
                HummockVersion::from_persisted_protobuf(&prev_version.version.to_protobuf())
                    .get_sst_ids()
            };
            // The SST ids deleted by compaction between the 2 versions.
            let sst_ids_to_delete = &sst_ids - &next_version_sst_ids;
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            tracing::debug!(
                prev_version_id,
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
            next_version_sst_ids = sst_ids;
        }

        let res = hummock_time_travel_version::Entity::delete_many()
            .filter(
                hummock_time_travel_version::Column::VersionId
                    .lt(earliest_valid_version_id.to_u64()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = ?version_watermark.version_id,
            ?earliest_valid_version_id,
            "delete {} rows from hummock_time_travel_version",
            res.rows_affected
        );

        let res = hummock_time_travel_delta::Entity::delete_many()
            .filter(
                hummock_time_travel_delta::Column::VersionId.lt(earliest_valid_version_id.to_u64()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = ?version_watermark.version_id,
            ?earliest_valid_version_id,
            "delete {} rows from hummock_time_travel_delta",
            res.rows_affected
        );

        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn all_object_ids_in_time_travel(
        &self,
    ) -> Result<impl Iterator<Item = HummockSstableId>> {
        let object_ids: Vec<risingwave_meta_model_v2::HummockSstableObjectId> =
            match self.sql_store() {
                Some(sql_store) => {
                    hummock_sstable_info::Entity::find()
                        .select_only()
                        .column(hummock_sstable_info::Column::ObjectId)
                        .into_tuple()
                        .all(&sql_store.conn)
                        .await?
                }
                None => {
                    vec![]
                }
            };
        let object_ids = object_ids
            .into_iter()
            .unique()
            .map(|object_id| HummockSstableObjectId::try_from(object_id).unwrap());
        Ok(object_ids)
    }

    /// Attempt to locate the version corresponding to `query_epoch`.
    ///
    /// The version is retrieved from `hummock_epoch_to_version`, selecting the entry with the largest epoch that's lte `query_epoch`.
    ///
    /// The resulted version is complete, i.e. with correct `SstableInfo`.
    pub async fn epoch_to_version(
        &self,
        query_epoch: HummockEpoch,
        table_id: u32,
    ) -> Result<HummockVersion> {
        let sql_store = self.sql_store().ok_or_else(require_sql_meta_store_err)?;
        let epoch_to_version = hummock_epoch_to_version::Entity::find()
            .filter(
                Condition::any()
                    .add(hummock_epoch_to_version::Column::TableId.eq(i64::from(table_id)))
                    // for backward compatibility
                    .add(hummock_epoch_to_version::Column::TableId.eq(0)),
            )
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
        let actual_version_id = epoch_to_version.version_id;
        tracing::debug!(
            query_epoch,
            query_tz = ?(Epoch(query_epoch).as_timestamptz()),
            actual_epoch = epoch_to_version.epoch,
            actual_tz = ?(Epoch(u64::try_from(epoch_to_version.epoch).unwrap()).as_timestamptz()),
            actual_version_id,
            "convert query epoch"
        );

        let replay_version = hummock_time_travel_version::Entity::find()
            .filter(hummock_time_travel_version::Column::VersionId.lte(actual_version_id))
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "no replay version found for epoch {}, version {}",
                    query_epoch, actual_version_id,
                )))
            })?;
        let deltas = hummock_time_travel_delta::Entity::find()
            .filter(hummock_time_travel_delta::Column::VersionId.gt(replay_version.version_id))
            .filter(hummock_time_travel_delta::Column::VersionId.lte(actual_version_id))
            .order_by_asc(hummock_time_travel_delta::Column::VersionId)
            .all(&sql_store.conn)
            .await?;
        let mut actual_version = replay_archive(
            replay_version.version.to_protobuf(),
            deltas.into_iter().map(|d| d.version_delta.to_protobuf()),
        );

        let mut sst_ids = actual_version
            .get_sst_ids()
            .into_iter()
            .collect::<VecDeque<_>>();
        let sst_count = sst_ids.len();
        let mut sst_id_to_info = HashMap::with_capacity(sst_count);
        let sst_info_fetch_batch_size = std::env::var("RW_TIME_TRAVEL_SST_INFO_FETCH_BATCH_SIZE")
            .unwrap_or_else(|_| "100".into())
            .parse()
            .unwrap();
        while !sst_ids.is_empty() {
            let sst_infos = hummock_sstable_info::Entity::find()
                .filter(hummock_sstable_info::Column::SstId.is_in(
                    sst_ids.drain(..std::cmp::min(sst_info_fetch_batch_size, sst_ids.len())),
                ))
                .all(&sql_store.conn)
                .await?;
            for sst_info in sst_infos {
                let sst_info: SstableInfo = sst_info.sstable_info.to_protobuf().into();
                sst_id_to_info.insert(sst_info.sst_id, sst_info);
            }
        }
        if sst_count != sst_id_to_info.len() {
            return Err(Error::TimeTravel(anyhow!(format!(
                "some SstableInfos not found for epoch {}, version {}",
                query_epoch, actual_version_id,
            ))));
        }
        refill_version(&mut actual_version, &sst_id_to_info);
        Ok(actual_version)
    }

    pub(crate) async fn write_time_travel_metadata(
        &self,
        txn: &DatabaseTransaction,
        version: Option<&HummockVersion>,
        delta: HummockVersionDelta,
        group_parents: &HashMap<CompactionGroupId, CompactionGroupId>,
        skip_sst_ids: &HashSet<HummockSstableId>,
        tables_to_commit: impl Iterator<Item = (&TableId, &CompactionGroupId)>,
        committed_epoch: u64,
    ) -> Result<Option<HashSet<HummockSstableId>>> {
        let select_groups = group_parents
            .iter()
            .filter_map(|(cg_id, _)| {
                if should_ignore_group(find_root_group(*cg_id, group_parents)) {
                    None
                } else {
                    Some(*cg_id)
                }
            })
            .collect::<HashSet<_>>();
        async fn write_sstable_infos(
            sst_infos: impl Iterator<Item = &SstableInfo>,
            txn: &DatabaseTransaction,
        ) -> Result<usize> {
            let mut count = 0;
            for sst_info in sst_infos {
                let m = hummock_sstable_info::ActiveModel {
                    sst_id: Set(sst_info.sst_id.try_into().unwrap()),
                    object_id: Set(sst_info.object_id.try_into().unwrap()),
                    sstable_info: Set(SstableInfoV2Backend::from(&sst_info.to_protobuf())),
                };
                hummock_sstable_info::Entity::insert(m)
                    .on_conflict(
                        OnConflict::column(hummock_sstable_info::Column::SstId)
                            .do_nothing()
                            .to_owned(),
                    )
                    .do_nothing()
                    .exec(txn)
                    .await?;
                count += 1;
            }
            Ok(count)
        }

        for (table_id, cg_id) in tables_to_commit {
            if !select_groups.contains(cg_id) {
                continue;
            }
            let version_id: u64 = delta.id.to_u64();
            let m = hummock_epoch_to_version::ActiveModel {
                epoch: Set(committed_epoch.try_into().unwrap()),
                table_id: Set(table_id.table_id.into()),
                version_id: Set(version_id.try_into().unwrap()),
            };
            // There should be no conflict rows.
            hummock_epoch_to_version::Entity::insert(m)
                .exec(txn)
                .await?;
        }

        let mut version_sst_ids = None;
        if let Some(version) = version {
            version_sst_ids = Some(
                version
                    .get_sst_infos_from_groups(&select_groups)
                    .map(|s| s.sst_id)
                    .collect(),
            );
            write_sstable_infos(
                version
                    .get_sst_infos_from_groups(&select_groups)
                    .filter(|s| !skip_sst_ids.contains(&s.sst_id)),
                txn,
            )
            .await?;
            let m = hummock_time_travel_version::ActiveModel {
                version_id: Set(risingwave_meta_model_v2::HummockVersionId::try_from(
                    version.id.to_u64(),
                )
                .unwrap()),
                version: Set((&IncompleteHummockVersion::from((version, &select_groups))
                    .to_protobuf())
                    .into()),
            };
            hummock_time_travel_version::Entity::insert(m)
                .on_conflict(
                    OnConflict::column(hummock_time_travel_version::Column::VersionId)
                        .do_nothing()
                        .to_owned(),
                )
                .do_nothing()
                .exec(txn)
                .await?;
        }
        let written = write_sstable_infos(
            delta
                .newly_added_sst_infos(&select_groups)
                .filter(|s| !skip_sst_ids.contains(&s.sst_id)),
            txn,
        )
        .await?;
        // Ignore delta which adds no data.
        if written > 0 {
            let m = hummock_time_travel_delta::ActiveModel {
                version_id: Set(risingwave_meta_model_v2::HummockVersionId::try_from(
                    delta.id.to_u64(),
                )
                .unwrap()),
                version_delta: Set((&IncompleteHummockVersionDelta::from((
                    &delta,
                    &select_groups,
                ))
                .to_protobuf())
                    .into()),
            };
            hummock_time_travel_delta::Entity::insert(m)
                .on_conflict(
                    OnConflict::column(hummock_time_travel_delta::Column::VersionId)
                        .do_nothing()
                        .to_owned(),
                )
                .do_nothing()
                .exec(txn)
                .await?;
        }

        Ok(version_sst_ids)
    }
}

fn replay_archive(
    version: PbHummockVersion,
    deltas: impl Iterator<Item = PbHummockVersionDelta>,
) -> HummockVersion {
    let mut last_version = HummockVersion::from_persisted_protobuf(&version);
    for d in deltas {
        let d = HummockVersionDelta::from_persisted_protobuf(&d);
        // Need to work around the assertion in `apply_version_delta`.
        // Because compaction deltas are not included in time travel archive.
        while last_version.id < d.prev_id {
            last_version.id = last_version.id + 1;
        }
        last_version.apply_version_delta(&d);
    }
    last_version
}

fn find_root_group(
    group_id: CompactionGroupId,
    parents: &HashMap<CompactionGroupId, CompactionGroupId>,
) -> CompactionGroupId {
    let mut root = group_id;
    while let Some(parent) = parents.get(&root)
        && *parent != 0
    {
        root = *parent;
    }
    root
}

fn should_ignore_group(root_group_id: CompactionGroupId) -> bool {
    // It is possible some intermediate groups has been dropped,
    // so it's impossible to tell whether the root group is MaterializedView or not.
    // Just treat them as MaterializedView for correctness.
    root_group_id == StaticCompactionGroupId::StateDefault as CompactionGroupId
}

pub fn require_sql_meta_store_err() -> Error {
    Error::TimeTravel(anyhow!("require SQL meta store"))
}
