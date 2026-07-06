// Copyright 2022 RisingWave Labs
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

use std::collections::HashSet;
use std::future::Future;

use bytes::Bytes;
use futures::TryStreamExt;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot_v2::HUMMOCK_VERSION_ENCODING_INDEX;
use risingwave_backup::storage::MetaSnapshotStreamingUploader;
use risingwave_hummock_sdk::HummockRawObjectId;
use risingwave_hummock_sdk::change_log::EpochNewChangeLog;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_meta_model as model;
use risingwave_pb::hummock::PbHummockVersionDelta;
use sea_orm::{DbErr, EntityTrait, PaginatorTrait, QueryOrder, TransactionTrait};
use serde::Serialize;

use crate::controller::SqlMetaStore;
use crate::hummock::model::ext::to_table_change_log;

pub const VERSION: u32 = 2;

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

pub struct MetaSnapshotV2Builder {
    meta_store: SqlMetaStore,
}

pub struct StreamedMetaSnapshotInfo {
    pub hummock_version: HummockVersion,
    pub table_change_log_object_ids: HashSet<HummockRawObjectId>,
}

impl MetaSnapshotV2Builder {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self { meta_store }
    }

    pub async fn build_streaming(
        &self,
        writer: &mut MetaSnapshotStreamingUploader,
        hummock_version_builder: impl Future<Output = HummockVersion>,
    ) -> BackupResult<StreamedMetaSnapshotInfo> {
        // Get `hummock_version` before `meta_store_snapshot`.
        // We have ensure the required delta logs for replay is available, see
        // `HummockManager::delete_version_deltas`.
        let hummock_version = hummock_version_builder.await;
        let txn = self
            .meta_store
            .conn
            .begin_with_config(
                Some(sea_orm::IsolationLevel::Serializable),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await
            .map_err(map_db_err)?;
        let hummock_version = build_hummock_version_from_deltas(hummock_version, &txn).await?;
        stream_metadata(&hummock_version, writer, &txn).await?;
        let table_change_log_object_ids = collect_table_change_log_object_ids(&txn).await?;

        txn.commit().await.map_err(map_db_err)?;
        Ok(StreamedMetaSnapshotInfo {
            hummock_version,
            table_change_log_object_ids,
        })
    }
}

async fn build_hummock_version_from_deltas(
    hummock_version: HummockVersion,
    txn: &sea_orm::DatabaseTransaction,
) -> BackupResult<HummockVersion> {
    let mut redo_state = hummock_version;
    let mut max_log_id = None;
    {
        let mut version_delta_stream = model::prelude::HummockVersionDelta::find()
            .order_by_asc(model::hummock_version_delta::Column::Id)
            .stream(txn)
            .await
            .map_err(map_db_err)?;
        while let Some(model) = version_delta_stream.try_next().await.map_err(map_db_err)? {
            let version_delta = HummockVersionDelta::from_persisted_protobuf_owned(
                PbHummockVersionDelta::from(model),
            );
            if version_delta.prev_id == redo_state.id {
                redo_state.apply_version_delta(&version_delta);
            }
            max_log_id = Some(version_delta.id);
        }
    }

    if let Some(max_log_id) = max_log_id
        && max_log_id != redo_state.id
    {
        return Err(BackupError::Other(anyhow::anyhow!(format!(
            "inconsistent hummock version: expected {}, actual {}",
            max_log_id, redo_state.id
        ))));
    }
    Ok(redo_state)
}

macro_rules! define_stream_metadata {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        async fn stream_metadata(
            hummock_version: &HummockVersion,
            writer: &mut MetaSnapshotStreamingUploader,
            txn: &sea_orm::DatabaseTransaction,
        ) -> BackupResult<()> {
            let mut _idx = 0;
            $(
                if _idx == HUMMOCK_VERSION_ENCODING_INDEX {
                    let hummock_version_pb = hummock_version.to_protobuf();
                    put_1_streaming(writer, &hummock_version_pb).await?;
                }
                put_model_streaming::<$mod_path::$mod_name::Entity>(writer, txn).await?;
                _idx += 1;
            )*
            Ok(())
        }
    };
}

risingwave_backup::for_all_metadata_models_v2!(define_stream_metadata);

async fn put_model_streaming<E>(
    writer: &mut MetaSnapshotStreamingUploader,
    txn: &sea_orm::DatabaseTransaction,
) -> BackupResult<()>
where
    E: EntityTrait,
    E::Model: Serialize + Send + Sync + 'static,
{
    let count = E::find().count(txn).await.map_err(map_db_err)?;
    let count = u32::try_from(count)
        .map_err(|_| BackupError::Other(anyhow::anyhow!("too many metadata rows: {}", count)))?;
    writer.write_u32_le(count).await?;

    let mut stream = E::find().stream(txn).await.map_err(map_db_err)?;
    let mut actual_count = 0;
    while let Some(model) = stream.try_next().await.map_err(map_db_err)? {
        if actual_count == count {
            return Err(BackupError::Other(anyhow::anyhow!(
                "metadata row count changed while streaming {}: counted {}, streamed more",
                std::any::type_name::<E>(),
                count,
            )));
        }
        put_with_len_prefix_streaming(writer, &model).await?;
        actual_count += 1;
    }
    if actual_count != count {
        return Err(BackupError::Other(anyhow::anyhow!(
            "metadata row count changed while streaming {}: counted {}, streamed {}",
            std::any::type_name::<E>(),
            count,
            actual_count,
        )));
    }
    Ok(())
}

async fn put_1_streaming<T: Serialize>(
    writer: &mut MetaSnapshotStreamingUploader,
    data: &T,
) -> BackupResult<()> {
    writer.write_u32_le(1).await?;
    put_with_len_prefix_streaming(writer, data).await
}

async fn put_with_len_prefix_streaming<T: Serialize>(
    writer: &mut MetaSnapshotStreamingUploader,
    data: &T,
) -> BackupResult<()> {
    let bytes = serde_json::to_vec(data)?;
    assert!(!bytes.is_empty());
    if let Ok(len) = u32::try_from(bytes.len()) {
        writer.write_u32_le(len).await?;
    } else {
        writer.write_u32_le(0).await?;
        writer
            .write_u64_le(
                bytes
                    .len()
                    .try_into()
                    .unwrap_or_else(|_| panic!("cannot convert {} into u64", bytes.len())),
            )
            .await?;
    }
    writer.write_bytes(Bytes::from(bytes)).await
}

async fn collect_table_change_log_object_ids(
    txn: &sea_orm::DatabaseTransaction,
) -> BackupResult<HashSet<HummockRawObjectId>> {
    let mut object_ids = HashSet::new();
    let mut stream = model::hummock_table_change_log::Entity::find()
        .stream(txn)
        .await
        .map_err(map_db_err)?;
    while let Some(model) = stream.try_next().await.map_err(map_db_err)? {
        let EpochNewChangeLog {
            new_value,
            old_value,
            ..
        } = to_table_change_log(model);
        object_ids.extend(
            new_value
                .into_iter()
                .chain(old_value)
                .map(|t| t.object_id.as_raw()),
        );
    }
    Ok(object_ids)
}

#[cfg(test)]
mod tests {
    use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
    use risingwave_backup::storage::{MetaSnapshotStorage, unused};
    use risingwave_hummock_sdk::version::HummockVersion;
    use sea_orm::{ActiveModelTrait, EntityTrait, Set};

    use super::*;
    use crate::controller::SqlMetaStore;

    macro_rules! define_load_legacy_metadata {
        ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
            async fn load_legacy_metadata(
                meta_store: &SqlMetaStore,
                hummock_version: HummockVersion,
            ) -> BackupResult<MetadataV2> {
                let mut metadata = MetadataV2 {
                    hummock_version,
                    ..Default::default()
                };
                $(
                    metadata.$name = $mod_path::$mod_name::Entity::find()
                        .all(&meta_store.conn)
                        .await
                        .map_err(map_db_err)?;
                )*
                Ok(metadata)
            }
        };
    }

    risingwave_backup::for_all_metadata_models_v2!(define_load_legacy_metadata);

    #[tokio::test]
    async fn test_streaming_snapshot_export_matches_legacy_snapshot_with_non_empty_tables() {
        let meta_store = SqlMetaStore::for_test().await;
        risingwave_meta_model::hummock_sequence::ActiveModel {
            name: Set("streaming_snapshot_export_test".to_owned()),
            seq: Set(123),
        }
        .insert(&meta_store.conn)
        .await
        .unwrap();

        let store = unused().await;
        let snapshot_id = 45;
        let mut uploader = store
            .begin_snapshot_upload(snapshot_id, VERSION)
            .await
            .unwrap();
        let snapshot_info = MetaSnapshotV2Builder::new(meta_store.clone())
            .build_streaming(&mut uploader, std::future::ready(HummockVersion::default()))
            .await
            .unwrap();
        uploader.finish().await.unwrap();

        let expected_metadata =
            load_legacy_metadata(&meta_store, snapshot_info.hummock_version.clone())
                .await
                .unwrap();
        assert!(!expected_metadata.objects.is_empty());
        assert!(
            expected_metadata
                .hummock_sequences
                .iter()
                .any(|seq| seq.name == "streaming_snapshot_export_test" && seq.seq == 123)
        );

        let expected = MetaSnapshotV2 {
            format_version: VERSION,
            id: snapshot_id,
            metadata: expected_metadata,
        };
        let uploaded: MetaSnapshotV2 = store.get(snapshot_id).await.unwrap();
        assert_eq!(uploaded.encode().unwrap(), expected.encode().unwrap());
    }
}
