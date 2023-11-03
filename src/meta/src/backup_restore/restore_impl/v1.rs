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
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::meta_snapshot_v1::{ClusterMetadata, MetaSnapshotV1};
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_backup::MetaSnapshotId;

use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::backup_restore::utils::MetaStoreBackendImpl;
use crate::dispatch_meta_store;
use crate::hummock::model::CompactionGroup;
use crate::manager::model::SystemParamsModel;
use crate::model::{ClusterId, MetadataModel, TableFragments};
use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

pub struct LoaderV1 {
    backup_store: MetaSnapshotStorageRef,
}

impl LoaderV1 {
    pub fn new(backup_store: MetaSnapshotStorageRef) -> Self {
        Self { backup_store }
    }
}

#[async_trait::async_trait]
impl Loader<ClusterMetadata> for LoaderV1 {
    async fn load(&self, target_id: MetaSnapshotId) -> BackupResult<MetaSnapshot<ClusterMetadata>> {
        let backup_store = &self.backup_store;
        let snapshot_list = &backup_store.manifest().snapshot_metadata;
        let mut target_snapshot: MetaSnapshotV1 = backup_store.get(target_id).await?;
        tracing::info!(
            "snapshot {} before rewrite:\n{}",
            target_id,
            target_snapshot
        );
        let newest_id = snapshot_list
            .iter()
            .map(|m| m.id)
            .max()
            .expect("should exist");
        assert!(
            newest_id >= target_id,
            "newest_id={}, target_id={}",
            newest_id,
            target_id
        );
        // Always use newest snapshot's `default_cf` during restoring, in order not to corrupt shared
        // data of snapshots. Otherwise, for example if we restore a older SST id generator, an
        // existent SST in object store is at risk of being overwrote by the restored cluster.
        // All these risky metadata are in `default_cf`, e.g. id generator, epoch. They must satisfy:
        // - Value is monotonically non-decreasing.
        // - Value is memcomparable.
        // - Keys of newest_snapshot is a superset of that of target_snapshot.
        if newest_id > target_id {
            let newest_snapshot: MetaSnapshotV1 = backup_store.get(newest_id).await?;
            for (k, v) in &target_snapshot.metadata.default_cf {
                let newest_v = newest_snapshot
                    .metadata
                    .default_cf
                    .get(k)
                    .unwrap_or_else(|| panic!("violate superset requirement. key {:x?}", k));
                assert!(newest_v >= v, "violate monotonicity requirement");
            }
            target_snapshot.metadata.default_cf = newest_snapshot.metadata.default_cf;
            tracing::info!(
                "snapshot {} after rewrite by snapshot {}:\n{}",
                target_id,
                newest_id,
                target_snapshot,
            );
        }
        Ok(target_snapshot)
    }
}

pub struct WriterModelV1ToMetaStoreV1 {
    meta_store: MetaStoreBackendImpl,
}

impl WriterModelV1ToMetaStoreV1 {
    pub fn new(meta_store: MetaStoreBackendImpl) -> Self {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl Writer<ClusterMetadata> for WriterModelV1ToMetaStoreV1 {
    async fn write(&self, target_snapshot: MetaSnapshot<ClusterMetadata>) -> BackupResult<()> {
        dispatch_meta_store!(&self.meta_store, store, {
            restore_metadata(store.clone(), target_snapshot.clone()).await?;
        });
        Ok(())
    }
}

async fn restore_metadata_model<S: MetaStore, T: MetadataModel + Send + Sync>(
    meta_store: &S,
    metadata: &[T],
) -> BackupResult<()> {
    if !T::list(meta_store).await?.is_empty() {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for d in metadata {
        d.insert(meta_store).await?;
    }
    Ok(())
}

async fn restore_system_param_model<S: MetaStore, T: SystemParamsModel + Send + Sync>(
    meta_store: &S,
    metadata: &[T],
) -> BackupResult<()> {
    if T::get(meta_store).await?.is_some() {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for d in metadata {
        d.insert(meta_store).await?;
    }
    Ok(())
}

async fn restore_cluster_id<S: MetaStore>(
    meta_store: &S,
    cluster_id: ClusterId,
) -> BackupResult<()> {
    if ClusterId::from_meta_store(meta_store).await?.is_some() {
        return Err(BackupError::NonemptyMetaStorage);
    }
    cluster_id.put_at_meta_store(meta_store).await?;
    Ok(())
}

async fn restore_default_cf<S: MetaStore>(
    meta_store: &S,
    snapshot: &MetaSnapshotV1,
) -> BackupResult<()> {
    if !meta_store.list_cf(DEFAULT_COLUMN_FAMILY).await?.is_empty() {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for (k, v) in &snapshot.metadata.default_cf {
        meta_store
            .put_cf(DEFAULT_COLUMN_FAMILY, k.clone(), v.clone())
            .await?;
    }
    Ok(())
}

async fn restore_metadata<S: MetaStore>(
    meta_store: S,
    snapshot: MetaSnapshotV1,
) -> BackupResult<()> {
    restore_default_cf(&meta_store, &snapshot).await?;
    restore_metadata_model(&meta_store, &[snapshot.metadata.version_stats]).await?;
    restore_metadata_model(
        &meta_store,
        &snapshot
            .metadata
            .compaction_groups
            .into_iter()
            .map(CompactionGroup::from_protobuf)
            .collect_vec(),
    )
    .await?;
    restore_metadata_model(
        &meta_store,
        &snapshot
            .metadata
            .table_fragments
            .into_iter()
            .map(TableFragments::from_protobuf)
            .collect_vec(),
    )
    .await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.user_info).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.database).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.schema).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.table).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.index).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.sink).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.view).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.source).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.function).await?;
    restore_metadata_model(&meta_store, &snapshot.metadata.connection).await?;
    restore_system_param_model(&meta_store, &[snapshot.metadata.system_param]).await?;
    restore_cluster_id(&meta_store, snapshot.metadata.cluster_id.into()).await?;
    Ok(())
}
