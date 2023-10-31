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

use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::Metadata;
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_backup::MetaSnapshotId;
use risingwave_common::config::MetaBackend;
use risingwave_hummock_sdk::version_checkpoint_path;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::hummock::{HummockVersion, HummockVersionCheckpoint};

use crate::backup_restore::restore_impl::v1::{LoaderV1, WriterModelV1ToMetaStoreV1};
use crate::backup_restore::restore_impl::v2::{LoaderV2, WriterModelV2ToMetaStoreV2};
use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};

/// Command-line arguments for restore.
#[derive(clap::Args, Debug, Clone)]
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
    pub hummock_storage_directory: String,
    /// Print the target snapshot, but won't restore to meta store.
    #[clap(long)]
    pub dry_run: bool,
}

async fn restore_hummock_version(
    hummock_storage_url: &str,
    hummock_storage_directory: &str,
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
    let checkpoint_path = version_checkpoint_path(hummock_storage_directory);
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
    let snapshot_list = &backup_store.manifest().snapshot_metadata;
    if !snapshot_list.iter().any(|m| m.id == target_id) {
        return Err(BackupError::Other(anyhow::anyhow!(
            "snapshot id {} not found",
            target_id
        )));
    }

    let format_version = match snapshot_list.iter().find(|m| m.id == target_id) {
        None => {
            return Err(BackupError::Other(anyhow::anyhow!(
                "snapshot id {} not found",
                target_id
            )));
        }
        Some(s) => s.format_version,
    };
    match &meta_store {
        MetaStoreBackendImpl::Sql(m) => {
            if format_version < 2 {
                todo!("write model V1 to meta store V2");
            } else {
                dispatch(
                    target_id,
                    &opts,
                    LoaderV2::new(backup_store),
                    WriterModelV2ToMetaStoreV2::new(m.to_owned()),
                )
                .await?;
            }
        }
        _ => {
            assert!(format_version < 2, "format_version {}", format_version);
            dispatch(
                target_id,
                &opts,
                LoaderV1::new(backup_store),
                WriterModelV1ToMetaStoreV1::new(meta_store),
            )
            .await?;
        }
    }

    Ok(())
}

async fn dispatch<L: Loader<S>, W: Writer<S>, S: Metadata>(
    target_id: MetaSnapshotId,
    opts: &RestoreOpts,
    loader: L,
    writer: W,
) -> BackupResult<()> {
    let target_snapshot = loader.load(target_id).await?;
    if opts.dry_run {
        return Ok(());
    }
    restore_hummock_version(
        &opts.hummock_storage_url,
        &opts.hummock_storage_directory,
        target_snapshot.metadata.hummock_version_ref(),
    )
    .await?;
    writer.write(target_snapshot).await?;
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

    use itertools::Itertools;
    use risingwave_backup::meta_snapshot_v1::{ClusterMetadata, MetaSnapshotV1};
    use risingwave_backup::storage::MetaSnapshotStorage;
    use risingwave_common::config::{MetaBackend, SystemConfig};
    use risingwave_pb::hummock::{HummockVersion, HummockVersionStats};
    use risingwave_pb::meta::SystemParams;

    use crate::backup_restore::restore::restore_impl;
    use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};
    use crate::backup_restore::RestoreOpts;
    use crate::dispatch_meta_store;
    use crate::manager::model::SystemParamsModel;
    use crate::model::MetadataModel;
    use crate::storage::{MetaStore, DEFAULT_COLUMN_FAMILY};

    type MetaSnapshot = MetaSnapshotV1;

    fn get_restore_opts() -> RestoreOpts {
        RestoreOpts {
            meta_snapshot_id: 1,
            meta_store_type: MetaBackend::Mem,
            etcd_endpoints: "".to_string(),
            etcd_auth: false,
            etcd_username: "".to_string(),
            etcd_password: "".to_string(),
            backup_storage_url: "memory".to_string(),
            backup_storage_directory: "".to_string(),
            hummock_storage_url: "memory".to_string(),
            hummock_storage_directory: "".to_string(),
            dry_run: false,
        }
    }

    fn get_system_params() -> SystemParams {
        SystemParams {
            state_store: Some("state_store".to_string()),
            data_directory: Some("data_directory".to_string()),
            ..SystemConfig::default().into_init_system_params()
        }
    }

    #[tokio::test]
    async fn test_restore_basic() {
        let opts = get_restore_opts();
        let backup_store = get_backup_store(opts.clone()).await.unwrap();
        let nonempty_meta_store = get_meta_store(opts.clone()).await.unwrap();
        dispatch_meta_store!(nonempty_meta_store.clone(), store, {
            let stats = HummockVersionStats::default();
            stats.insert(&store).await.unwrap();
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
            assert!(SystemParams::get(&store).await.unwrap().is_none());
        });
    }
}
