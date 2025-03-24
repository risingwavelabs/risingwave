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

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::anyhow;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::Metadata;
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_backup::MetaSnapshotId;
use risingwave_common::config::{MetaBackend, ObjectStoreConfig};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{version_checkpoint_path, HummockSstableObjectId, OBJECT_SUFFIX};
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::hummock::PbHummockVersionCheckpoint;
use thiserror_ext::AsReport;

use crate::backup_restore::restore_impl::v1::{LoaderV1, WriterModelV1ToMetaStoreV1};
use crate::backup_restore::restore_impl::v2::{LoaderV2, WriterModelV2ToMetaStoreV2};
use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::backup_restore::utils::{get_backup_store, get_meta_store, MetaStoreBackendImpl};

/// Command-line arguments for restore.
#[derive(clap::Args, Debug, Clone)]
pub struct RestoreOpts {
    /// Id of snapshot used to restore. Available snapshots can be found in
    /// <`storage_directory>/manifest.json`.
    #[clap(long)]
    pub meta_snapshot_id: u64,
    /// Type of meta store to restore.
    #[clap(long, value_enum, default_value_t = MetaBackend::Etcd)]
    pub meta_store_type: MetaBackend,
    #[clap(long, default_value_t = String::from(""))]
    pub sql_endpoint: String,
    /// Username of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_username: String,
    /// Password of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_password: String,
    /// Database of sql backend, required when meta backend set to MySQL or PostgreSQL.
    #[clap(long, default_value = "")]
    pub sql_database: String,
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
    /// The read timeout for object store
    #[clap(long, default_value_t = 600000)]
    pub read_attempt_timeout_ms: u64,
    /// The maximum number of read retry attempts for the object store.
    #[clap(long, default_value_t = 3)]
    pub read_retry_attempts: u64,
    /// Verify that all referenced objects exist in object store.
    #[clap(long)]
    pub validate_integrity: bool,
}

async fn restore_hummock_version(
    hummock_storage_url: &str,
    hummock_storage_directory: &str,
    hummock_version: &HummockVersion,
) -> BackupResult<()> {
    let object_store = Arc::new(
        build_remote_object_store(
            hummock_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            "Version Checkpoint",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await,
    );
    let checkpoint_path = version_checkpoint_path(hummock_storage_directory);
    let checkpoint = PbHummockVersionCheckpoint {
        version: Some(hummock_version.into()),
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

    let snapshot = match snapshot_list.iter().find(|m| m.id == target_id) {
        None => {
            return Err(BackupError::Other(anyhow::anyhow!(
                "snapshot id {} not found",
                target_id
            )));
        }
        Some(s) => s,
    };

    if opts.validate_integrity {
        tracing::info!("Start integrity validation.");
        validate_integrity(
            snapshot.ssts.clone(),
            &opts.hummock_storage_url,
            &opts.hummock_storage_directory,
        )
        .await
        .inspect_err(|_| tracing::error!("Fail integrity validation."))?;
        tracing::info!("Succeed integrity validation.");
    }

    let format_version = snapshot.format_version;
    match &meta_store {
        MetaStoreBackendImpl::Sql(m) => {
            if format_version < 2 {
                unimplemented!("not supported: write model V1 to meta store V2");
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
        tracing::info!("Complete dry run.");
        return Ok(());
    }
    let hummock_version = target_snapshot.metadata.hummock_version_ref().clone();
    writer.write(target_snapshot).await?;
    restore_hummock_version(
        &opts.hummock_storage_url,
        &opts.hummock_storage_directory,
        &hummock_version,
    )
    .await?;
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
            tracing::warn!(error = %e.as_report(), "command failed");
        }
    }
    result
}

async fn validate_integrity(
    mut object_ids: HashSet<HummockSstableObjectId>,
    hummock_storage_url: &str,
    hummock_storage_directory: &str,
) -> BackupResult<()> {
    fn try_get_object_id_from_path(path: &str) -> Option<HummockSstableObjectId> {
        let split = path.split(&['/', '.']).collect_vec();
        if split.len() <= 2 {
            return None;
        }
        if split[split.len() - 1] != OBJECT_SUFFIX {
            return None;
        }
        let id = split[split.len() - 2]
            .parse::<HummockSstableObjectId>()
            .expect(&format!(
                "expect valid sst id, got {}",
                split[split.len() - 2]
            ));
        Some(id)
    }
    tracing::info!("expect {} objects", object_ids.len());
    let object_store = Arc::new(
        build_remote_object_store(
            hummock_storage_url,
            Arc::new(ObjectStoreMetrics::unused()),
            "Version Checkpoint",
            Arc::new(ObjectStoreConfig::default()),
        )
        .await,
    );
    let mut iter = object_store.list(hummock_storage_directory).await?;
    while let Some(obj) = iter.try_next().await? {
        let Some(obj_id) = try_get_object_id_from_path(&obj.key) else {
            continue;
        };
        if object_ids.remove(&obj_id) && object_ids.is_empty() {
            break;
        }
    }
    if object_ids.is_empty() {
        return Ok(());
    }
    Err(BackupError::Other(anyhow!(
        "referenced objects not found in object store: {:?}",
        object_ids
    )))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use itertools::Itertools;
    use risingwave_backup::meta_snapshot_v1::{ClusterMetadata, MetaSnapshotV1};
    use risingwave_backup::storage::MetaSnapshotStorage;
    use risingwave_common::config::{MetaBackend, SystemConfig};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::HummockVersionId;
    use risingwave_pb::hummock::HummockVersionStats;
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
            sql_endpoint: "".to_string(),
            sql_username: "".to_string(),
            sql_password: "".to_string(),
            sql_database: "".to_string(),
            etcd_endpoints: "".to_string(),
            etcd_auth: false,
            etcd_username: "".to_string(),
            etcd_password: "".to_string(),
            backup_storage_url: "memory".to_string(),
            backup_storage_directory: "".to_string(),
            hummock_storage_url: "memory".to_string(),
            hummock_storage_directory: "".to_string(),
            dry_run: false,
            read_attempt_timeout_ms: 60000,
            read_retry_attempts: 3,
        }
    }

    fn get_system_params() -> SystemParams {
        SystemParams {
            state_store: Some("state_store".into()),
            data_directory: Some("data_directory".into()),
            use_new_object_prefix_strategy: Some(true),
            backup_storage_url: Some("backup_storage_url".into()),
            backup_storage_directory: Some("backup_storage_directory".into()),
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
                hummock_version: {
                    let mut version = HummockVersion::default();
                    version.id = HummockVersionId::new(123);
                    version
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

        backup_store.create(&snapshot, None).await.unwrap();
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
        backup_store.create(&snapshot, None).await.unwrap();

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
        backup_store.create(&snapshot_2, None).await.unwrap();
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
        backup_store.create(&snapshot, None).await.unwrap();

        // violate superset requirement
        let mut snapshot_2 = MetaSnapshot {
            id: snapshot.id + 1,
            ..Default::default()
        };
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![10u8, 20u8], memcomparable::to_vec(&1).unwrap());
        backup_store.create(&snapshot_2, None).await.unwrap();
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
        backup_store.create(&snapshot, None).await.unwrap();

        // violate monotonicity requirement
        let mut snapshot_2 = MetaSnapshot {
            id: snapshot.id + 1,
            ..Default::default()
        };
        snapshot_2
            .metadata
            .default_cf
            .insert(vec![1u8, 2u8], memcomparable::to_vec(&9).unwrap());
        backup_store.create(&snapshot_2, None).await.unwrap();
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
                hummock_version: {
                    let mut version = HummockVersion::default();
                    version.id = HummockVersionId::new(123);
                    version
                },
                system_param: system_param.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        backup_store.create(&snapshot, None).await.unwrap();
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
