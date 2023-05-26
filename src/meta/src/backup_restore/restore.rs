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

use std::sync::Arc;

use clap::Parser;
use itertools::Itertools;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::storage::MetaSnapshotStorageRef;
use risingwave_common::config::MetaBackend;
use risingwave_hummock_sdk::version_checkpoint_path;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::hummock::{HummockVersion, HummockVersionCheckpoint};

use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};
use crate::dispatch_meta_store;
use crate::hummock::model::CompactionGroup;
use crate::manager::model::SystemParamsModel;
use crate::model::{ClusterId, MetadataModel, TableFragments};
use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

/// Command-line arguments for restore.
#[derive(Parser, Debug, Clone)]
pub struct RestoreOpts {
    /// Id of snapshot used to restore. Available snapshots can be found in
    /// <storage_directory>/manifest.json.
    #[clap(long)]
    pub meta_snapshot_id: u64,
    /// Type of meta store to restore.
    #[clap(long, value_enum, default_value_t = MetaBackend::Etcd)]
    pub meta_store_type: MetaBackend,
    /// Etcd endpoints.
    #[clap(long, default_value_t = String::from(""))]
    pub etcd_endpoints: String,
    /// Whether etcd auth has been enabled.
    #[clap(long)]
    pub etcd_auth: bool,
    /// Username if etcd auth has been enabled.
    #[clap(long, default_value = "")]
    pub etcd_username: String,
    /// Password if etcd auth has been enabled.
    #[clap(long, default_value = "")]
    pub etcd_password: String,
    /// Url of storage to fetch meta snapshot from.
    #[clap(long)]
    pub backup_storage_url: String,
    /// Directory of storage to fetch meta snapshot from.
    #[clap(long, default_value_t = String::from("backup"))]
    pub backup_storage_directory: String,
    /// Url of storage to restore hummock version to.
    #[clap(long)]
    pub hummock_storage_url: String,
    /// Directory of storage to restore hummock version to.
    #[clap(long, default_value_t = String::from("hummock_001"))]
    pub hummock_storage_dir: String,
    /// Print the target snapshot, but won't restore to meta store.
    #[clap(long)]
    pub dry_run: bool,
}

async fn restore_hummock_version(
    hummock_storage_url: &str,
    hummock_storage_dir: &str,
    hummock_version: &HummockVersion,
) -> BackupResult<()> {
    let object_store = Arc::new(
        parse_remote_object_store(
            hummock_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            "Version Checkpoint",
        )
        .await,
    );
    let checkpoint_path = version_checkpoint_path(hummock_storage_dir);
    let checkpoint = HummockVersionCheckpoint {
        version: Some(hummock_version.clone()),
        // Ignore stale objects. Full GC will clear them.
        stale_objects: Default::default(),
    };
    use prost::Message;
    let buf = checkpoint.encode_to_vec();
    object_store
        .upload(&checkpoint_path, buf.into())
        .await
        .map_err(|e| BackupError::StateStorage(e.into()))?;
    Ok(())
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
    snapshot: &MetaSnapshot,
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

async fn restore_metadata<S: MetaStore>(meta_store: S, snapshot: MetaSnapshot) -> BackupResult<()> {
    restore_default_cf(&meta_store, &snapshot).await?;
    restore_metadata_model(&meta_store, &[snapshot.metadata.hummock_version]).await?;
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

/// Restores a meta store.
/// Uses `meta_store` and `backup_store` if provided.
/// Otherwise creates them based on `opts`.
async fn restore_impl(
    opts: RestoreOpts,
    meta_store: Option<MetaStoreBackendImpl>,
    backup_store: Option<MetaSnapshotStorageRef>,
) -> BackupResult<()> {
    if cfg!(not(test)) {
        assert!(meta_store.is_none());
        assert!(backup_store.is_none());
    }
    let meta_store = match meta_store {
        None => get_meta_store(opts.clone()).await?,
        Some(m) => m,
    };
    let backup_store = match backup_store {
        None => get_backup_store(opts.clone()).await?,
        Some(b) => b,
    };
    let target_id = opts.meta_snapshot_id;
    let snapshot_list = backup_store.manifest().snapshot_metadata.clone();
    if !snapshot_list.iter().any(|m| m.id == target_id) {
        return Err(BackupError::Other(anyhow::anyhow!(
            "snapshot id {} not found",
            target_id
        )));
    }
    let mut target_snapshot = backup_store.get(target_id).await?;
    tracing::info!(
        "snapshot {} before rewrite:\n{}",
        target_id,
        target_snapshot
    );
    let newest_id = snapshot_list
        .into_iter()
        .map(|m| m.id)
        .max()
        .expect("should exist");
    assert!(newest_id >= target_id);
    // Always use newest snapshot's `default_cf` during restoring, in order not to corrupt shared
    // data of snapshots. Otherwise, for example if we restore a older SST id generator, an
    // existent SST in object store is at risk of being overwrote by the restored cluster.
    // All these risky metadata are in `default_cf`, e.g. id generator, epoch. They must satisfy:
    // - Value is monotonically non-decreasing.
    // - Value is memcomparable.
    // - Keys of newest_snapshot is a superset of that of target_snapshot.
    if newest_id > target_id {
        let newest_snapshot = backup_store.get(newest_id).await?;
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
    if opts.dry_run {
        return Ok(());
    }
    restore_hummock_version(
        &opts.hummock_storage_url,
        &opts.hummock_storage_dir,
        &target_snapshot.metadata.hummock_version,
    )
    .await?;
    dispatch_meta_store!(meta_store.clone(), store, {
        restore_metadata(store.clone(), target_snapshot.clone()).await?;
    });
    Ok(())
}

pub async fn restore(opts: RestoreOpts) -> BackupResult<()> {
    tracing::info!("restore with opts: {:#?}", opts);
    let result = restore_impl(opts, None, None).await;
    match &result {
        Ok(_) => {
            tracing::info!("command succeeded");
        }
        Err(e) => {
            tracing::warn!("command failed: {}", e);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use clap::Parser;
    use itertools::Itertools;
    use risingwave_backup::meta_snapshot::{ClusterMetadata, MetaSnapshot};
    use risingwave_pb::hummock::HummockVersion;
    use risingwave_pb::meta::SystemParams;

    use crate::backup_restore::restore::restore_impl;
    use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};
    use crate::backup_restore::RestoreOpts;
    use crate::dispatch_meta_store;
    use crate::manager::model::SystemParamsModel;
    use crate::model::MetadataModel;
    use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

    fn get_restore_opts() -> RestoreOpts {
        RestoreOpts::parse_from([
            "restore",
            "--meta-snapshot-id",
            "1",
            "--meta-store-type",
            "mem",
            "--backup-storage-url",
            "memory",
            "--hummock-storage-url",
            "memory",
        ])
    }

    fn get_system_params() -> SystemParams {
        SystemParams {
            barrier_interval_ms: Some(101),
            checkpoint_frequency: Some(102),
            sstable_size_mb: Some(103),
            block_size_kb: Some(104),
            bloom_false_positive: Some(0.1),
            state_store: Some("state_store".to_string()),
            data_directory: Some("data_directory".to_string()),
            backup_storage_url: Some("backup_storage_url".to_string()),
            backup_storage_directory: Some("backup_storage_directory".to_string()),
            telemetry_enabled: Some(false),
        }
    }

    #[tokio::test]
    async fn test_restore_basic() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let nonempty_meta_store = get_meta_store(opts.clone()).await.unwrap();
        dispatch_meta_store!(nonempty_meta_store.clone(), store, {
            let hummock_version = HummockVersion::default();
            hummock_version.insert(&store).await.unwrap();
        });
        let empty_meta_store = get_meta_store(opts.clone()).await.unwrap();
        let system_param = get_system_params();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                hummock_version: HummockVersion {
                    id: 123,
                    ..Default::default()
                },
                system_param: system_param.clone(),
                ..Default::default()
            },
            ..Default::default()
        };

        // target snapshot not found
        restore_impl(opts.clone(), None, Some(backup_store.clone()))
            .await
            .unwrap_err();

        backup_store.create(&snapshot).await.unwrap();
        restore_impl(opts.clone(), None, Some(backup_store.clone()))
            .await
            .unwrap();

        // target meta store not empty
        restore_impl(
            opts.clone(),
            Some(nonempty_meta_store),
            Some(backup_store.clone()),
        )
        .await
        .unwrap_err();

        restore_impl(
            opts.clone(),
            Some(empty_meta_store.clone()),
            Some(backup_store.clone()),
        )
        .await
        .unwrap();

        dispatch_meta_store!(empty_meta_store, store, {
            let restored_hummock_version = HummockVersion::list(&store)
                .await
                .unwrap()
                .into_iter()
                .next()
                .unwrap();
            assert_eq!(restored_hummock_version.id, 123);
            let restored_system_param = SystemParams::get(&store).await.unwrap().unwrap();
            assert_eq!(restored_system_param, system_param);
        });
    }

    #[tokio::test]
    async fn test_restore_default_cf() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                default_cf: HashMap::from([(vec![1u8, 2u8], memcomparable::to_vec(&10).unwrap())]),
                system_param: get_system_params(),
                ..Default::default()
            },
            ..Default::default()
        };
        backup_store.create(&snapshot).await.unwrap();

        // `snapshot_2` is a superset of `snapshot`
        let mut snapshot_2 = MetaSnapshot {
            id: snapshot.id + 1,
            ..snapshot.clone()
        };
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![1u8, 2u8], memcomparable::to_vec(&10).unwrap());
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![10u8, 20u8], memcomparable::to_vec(&10).unwrap());
        backup_store.create(&snapshot_2).await.unwrap();
        let empty_meta_store = get_meta_store(opts.clone()).await.unwrap();
        restore_impl(
            opts.clone(),
            Some(empty_meta_store.clone()),
            Some(backup_store.clone()),
        )
        .await
        .unwrap();
        dispatch_meta_store!(empty_meta_store, store, {
            let mut kvs = store
                .list_cf(DEFAULT_COLUMN_FAMILY)
                .await
                .unwrap()
                .into_iter()
                .map(|(_, v)| v)
                .collect_vec();
            kvs.sort();
            assert_eq!(
                kvs,
                vec![
                    memcomparable::to_vec(&10).unwrap(),
                    memcomparable::to_vec(&10).unwrap()
                ]
            );
        });
    }

    #[tokio::test]
    #[should_panic]
    async fn test_sanity_check_superset_requirement() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                default_cf: HashMap::from([(vec![1u8, 2u8], memcomparable::to_vec(&10).unwrap())]),
                system_param: get_system_params(),
                ..Default::default()
            },
            ..Default::default()
        };
        backup_store.create(&snapshot).await.unwrap();

        // violate superset requirement
        let mut snapshot_2 = MetaSnapshot {
            id: snapshot.id + 1,
            ..Default::default()
        };
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![10u8, 20u8], memcomparable::to_vec(&1).unwrap());
        backup_store.create(&snapshot_2).await.unwrap();
        restore_impl(opts.clone(), None, Some(backup_store.clone()))
            .await
            .unwrap();
    }

    #[tokio::test]
    #[should_panic]
    async fn test_sanity_check_monotonicity_requirement() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                default_cf: HashMap::from([(vec![1u8, 2u8], memcomparable::to_vec(&10).unwrap())]),
                system_param: get_system_params(),
                ..Default::default()
            },
            ..Default::default()
        };
        backup_store.create(&snapshot).await.unwrap();

        // violate monotonicity requirement
        let mut snapshot_2 = MetaSnapshot {
            id: snapshot.id + 1,
            ..Default::default()
        };
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![1u8, 2u8], memcomparable::to_vec(&9).unwrap());
        backup_store.create(&snapshot_2).await.unwrap();
        restore_impl(opts.clone(), None, Some(backup_store.clone()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_dry_run() {
        let mut opts = get_restore_opts();
        assert!(!opts.dry_run);
        opts.dry_run = true;
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let empty_meta_store = get_meta_store(opts.clone()).await.unwrap();
        let system_param = get_system_params();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                default_cf: HashMap::from([
                    (
                        "some_key_1".as_bytes().to_vec(),
                        memcomparable::to_vec(&10).unwrap(),
                    ),
                    (
                        "some_key_2".as_bytes().to_vec(),
                        memcomparable::to_vec(&"some_value_2".to_string()).unwrap(),
                    ),
                ]),
                hummock_version: HummockVersion {
                    id: 123,
                    ..Default::default()
                },
                system_param: system_param.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        backup_store.create(&snapshot).await.unwrap();
        restore_impl(
            opts.clone(),
            Some(empty_meta_store.clone()),
            Some(backup_store.clone()),
        )
        .await
        .unwrap();

        dispatch_meta_store!(empty_meta_store, store, {
            assert!(HummockVersion::list(&store).await.unwrap().is_empty());
            assert!(SystemParams::get(&store).await.unwrap().is_none());
        });
    }
}
