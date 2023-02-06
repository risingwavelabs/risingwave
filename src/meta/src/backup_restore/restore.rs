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

use clap::Parser;
use itertools::Itertools;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::storage::MetaSnapshotStorageRef;
use risingwave_common::config::MetaBackend;

use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};
use crate::dispatch_meta_store;
use crate::hummock::compaction_group::CompactionGroup;
use crate::model::{MetadataModel, TableFragments};
use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

/// Command-line arguments for restore.
#[derive(Parser, Debug, Clone)]
pub struct RestoreOpts {
    /// Id of snapshot used to restore. Available snapshots can be found in
    /// <storage_directory>/manifest.json.
    #[clap(long)]
    pub meta_snapshot_id: u64,
    /// Type of meta store to restore.
    #[clap(long, arg_enum, default_value_t = MetaBackend::Etcd)]
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
    #[clap(long, default_value_t = String::from("memory"))]
    pub storage_url: String,
    /// Directory of storage to fetch meta snapshot from.
    #[clap(long, default_value_t = String::from("backup"))]
    pub storage_directory: String,
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
    let newest_id = snapshot_list
        .into_iter()
        .map(|m| m.id)
        .max()
        .expect("should exist");
    let newest_snapshot = backup_store.get(newest_id).await?;
    let mut target_snapshot = backup_store.get(target_id).await?;
    // Always use newest snapshot's `default_cf` during restoring, in order not to corrupt shared
    // data of snapshots. Otherwise, for example if we restore a older SST id generator, an
    // existent SST in object store is at risk of being overwrote by the restored cluster.
    // All these risky metadata are in `default_cf`, e.g. id generator, epoch. They must satisfy:
    // - Value is monotonically non-decreasing.
    // - Value is memcomparable.
    // - Keys of newest_snapshot is a superset of that of target_snapshot.
    assert!(newest_snapshot.id >= target_snapshot.id);
    for (k, v) in &target_snapshot.metadata.default_cf {
        let newest_v = newest_snapshot
            .metadata
            .default_cf
            .get(k)
            .unwrap_or_else(|| panic!("violate superset requirement. key {:x?}", k));
        assert!(newest_v >= v, "violate monotonicity requirement");
    }
    target_snapshot.metadata.default_cf = newest_snapshot.metadata.default_cf;

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
            tracing::info!("restore succeeded");
        }
        Err(e) => {
            tracing::warn!("restore failed: {}", e);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use clap::Parser;
    use risingwave_backup::meta_snapshot::{ClusterMetadata, MetaSnapshot};
    use risingwave_pb::hummock::HummockVersion;

    use crate::backup_restore::restore::restore_impl;
    use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};
    use crate::backup_restore::RestoreOpts;
    use crate::dispatch_meta_store;
    use crate::model::MetadataModel;
    use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

    fn get_restore_opts() -> RestoreOpts {
        RestoreOpts::parse_from([
            "restore",
            "--meta-snapshot-id",
            "1",
            "--meta-store-type",
            "mem",
            "--storage-url",
            "memory",
        ])
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
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                hummock_version: HummockVersion {
                    id: 123,
                    ..Default::default()
                },
                ..Default::default()
            },
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
            Some(empty_meta_store),
            Some(backup_store.clone()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_restore_default_cf() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let snapshot = MetaSnapshot {
            id: opts.meta_snapshot_id,
            metadata: ClusterMetadata {
                default_cf: HashMap::from([(vec![1u8, 2u8], memcomparable::to_vec(&10).unwrap())]),
                ..Default::default()
            },
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
            let mut kvs = store.list_cf(DEFAULT_COLUMN_FAMILY).await.unwrap();
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
                ..Default::default()
            },
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
                ..Default::default()
            },
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
}
