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

use anyhow::{Context, anyhow};
use futures::StreamExt;
use futures::stream::BoxStream;
use risingwave_common::secret::{LocalSecretManager, SecretEncryption};
use risingwave_hummock_sdk::FrontendHummockVersion;
use risingwave_meta::MetaResult;
use risingwave_meta::controller::catalog::Catalog;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::WorkerId;
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::catalog::{Secret, Table};
use risingwave_pb::common::{ClusterResource, WorkerNode};
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, GetSessionParamsResponse, MetaSnapshot, ObjectDependency,
    PbTableCacheRefillPolicies, PbTableRefillRuntimeConfig, SubscribeRequest, SubscribeType,
};
use risingwave_pb::user::UserInfo;
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::{
    MetaSrvEnv, Notification, NotificationVersion, SnapshotMarkerSender, WorkerKey,
    notification_stream,
};
use crate::serving::ServingVnodeMappingRef;

pub struct NotificationServiceImpl {
    env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    serving_vnode_mapping: ServingVnodeMappingRef,
}

impl NotificationServiceImpl {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        backup_manager: BackupManagerRef,
        serving_vnode_mapping: ServingVnodeMappingRef,
    ) -> MetaResult<Self> {
        let service = Self {
            env,
            metadata_manager,
            hummock_manager,
            backup_manager,
            serving_vnode_mapping,
        };
        let secrets = service
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await
            .list_secrets()
            .await?;
        LocalSecretManager::global().init_secrets(service.decrypt_secrets(secrets)?);
        Ok(service)
    }

    // Read all fields of a snapshot part under its state guard, then send its marker before
    // releasing that guard. Producers must modify and publish under the same exclusive guard.
    // Independent helpers release their guards before the next helper acquires another lock.
    async fn get_catalog_snapshot_for_frontend(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<(
        Catalog,
        Vec<UserInfo>,
        Vec<ObjectDependency>,
        NotificationVersion,
    )> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let (
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
        ) = catalog_guard.snapshot().await?;
        let object_dependencies = catalog_guard.list_created_object_dependencies().await?;
        let notification_version = self.env.notification_manager().current_version().await;
        tx.snapshot_read(&*catalog_guard);
        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
            object_dependencies,
            notification_version,
        ))
    }

    /// Compute subscribers only need decrypted secrets from the catalog domain.
    async fn get_decrypted_secret_snapshot_for_compute(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<(Vec<Secret>, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let secrets = catalog_guard.list_secrets().await?;
        let notification_version = self.env.notification_manager().current_version().await;

        let decrypted_secrets = self.decrypt_secrets(secrets)?;
        tx.snapshot_read(&*catalog_guard);

        Ok((decrypted_secrets, notification_version))
    }

    fn decrypt_secrets(&self, secrets: Vec<Secret>) -> MetaResult<Vec<Secret>> {
        // Skip getting `secret_store_private_key` if there is no secret
        if secrets.is_empty() {
            return Ok(vec![]);
        }
        let secret_store_private_key = self
            .env
            .opts
            .secret_store_private_key
            .clone()
            .ok_or_else(|| anyhow!("secret_store_private_key is not configured"))?;
        let mut decrypted_secrets = Vec::with_capacity(secrets.len());
        for mut secret in secrets {
            let encrypted_secret = SecretEncryption::deserialize(secret.get_value())
                .context(format!("failed to deserialize secret {}", secret.name))?;
            let decrypted_secret = encrypted_secret
                .decrypt(secret_store_private_key.as_slice())
                .context(format!("failed to decrypt secret {}", secret.name))?;
            secret.value = decrypted_secret;
            decrypted_secrets.push(secret);
        }
        Ok(decrypted_secrets)
    }

    fn get_worker_slot_mapping_snapshot_for_frontend(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> Vec<FragmentWorkerSlotMapping> {
        let actor_guard = self.env.shared_actor_infos().read_guard();
        tx.snapshot_read(&*actor_guard);
        actor_guard.worker_slot_mappings()
    }

    fn get_serving_vnode_mappings_for_frontend(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> Vec<FragmentWorkerSlotMapping> {
        self.serving_vnode_mapping
            .all_for_snapshot(tx)
            .iter()
            .map(|(&fragment_id, mapping)| FragmentWorkerSlotMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect()
    }

    async fn get_cluster_snapshot_for_frontend(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<(Vec<WorkerNode>, ClusterResource)> {
        self.metadata_manager
            .cluster_controller
            .get_worker_nodes_snapshot(tx)
            .await
    }

    /// Compactor subscribers only need state tables from the catalog domain.
    async fn get_tables_snapshot_for_compactor(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<(Vec<Table>, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let notification_version = self.env.notification_manager().current_version().await;
        tx.snapshot_read(&*catalog_guard);
        Ok((tables, notification_version))
    }

    async fn get_catalog_snapshot_for_hummock(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<(Vec<Table>, PbTableCacheRefillPolicies, NotificationVersion)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let table_cache_refill_policies =
            catalog_guard.table_cache_refill_policies_snapshot().await?;
        let notification_version = self.env.notification_manager().current_version().await;
        tx.snapshot_read(&*catalog_guard);
        Ok((tables, table_cache_refill_policies, notification_version))
    }

    /// Hummock, compute, and compactor subscribers only need resources from the cluster domain.
    /// Frontends read nodes and resources together in `get_cluster_snapshot_for_frontend`.
    async fn get_cluster_resource_snapshot_for_workers(
        &self,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<ClusterResource> {
        self.metadata_manager
            .cluster_controller
            .get_cluster_resource_snapshot(tx)
            .await
    }

    async fn compactor_subscribe(&self, tx: &SnapshotMarkerSender) -> MetaResult<MetaSnapshot> {
        let (tables, catalog_version) = self.get_tables_snapshot_for_compactor(tx).await?;
        let cluster_resource = self.get_cluster_resource_snapshot_for_workers(tx).await?;

        Ok(MetaSnapshot {
            tables,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }

    async fn frontend_subscribe(&self, tx: &SnapshotMarkerSender) -> MetaResult<MetaSnapshot> {
        let (
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
            object_dependencies,
            catalog_version,
        ) = self.get_catalog_snapshot_for_frontend(tx).await?;

        // Use the plain text secret value for frontend. The secret value will be masked in frontend handle.
        let decrypted_secrets = self.decrypt_secrets(secrets)?;

        let streaming_worker_slot_mappings = self.get_worker_slot_mapping_snapshot_for_frontend(tx);

        let streaming_job_count = self.metadata_manager.count_streaming_job().await?;
        if streaming_job_count > 0 && streaming_worker_slot_mappings.is_empty() {
            tracing::warn!(
                streaming_job_count,
                "frontend subscribe returns empty streaming_worker_slot_mappings while streaming jobs exist; meta may still be recovering"
            );
        }

        let serving_worker_slot_mappings = self.get_serving_vnode_mappings_for_frontend(tx);

        let (nodes, cluster_resource) = self.get_cluster_snapshot_for_frontend(tx).await?;

        let hummock_version = self.hummock_manager.current_version_for_snapshot(tx).await;
        let hummock_version = FrontendHummockVersion::from_version(&hummock_version).to_protobuf();

        let session_params = self
            .env
            .session_params_manager_impl_ref()
            .get_params_for_snapshot(tx)
            .await;

        let session_params = Some(GetSessionParamsResponse {
            params: serde_json::to_string(&session_params)
                .context("failed to encode session params")?,
        });

        Ok(MetaSnapshot {
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            views,
            subscriptions,
            functions,
            connections,
            secrets: decrypted_secrets,
            users,
            nodes,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            serving_worker_slot_mappings,
            streaming_worker_slot_mappings,
            session_params,
            object_dependencies,
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }

    async fn hummock_subscribe(
        &self,
        worker_id: WorkerId,
        tx: &SnapshotMarkerSender,
    ) -> MetaResult<MetaSnapshot> {
        let (tables, table_cache_refill_policies, catalog_version) =
            self.get_catalog_snapshot_for_hummock(tx).await?;
        let hummock_version = self.hummock_manager.current_version_for_snapshot(tx).await;
        let hummock_version = hummock_version.as_ref().into();
        let hummock_write_limits = self.hummock_manager.write_limits_for_snapshot(tx).await;
        let meta_backup_manifest_id = self.backup_manager.manifest().await.manifest_id;
        let cluster_resource = self.get_cluster_resource_snapshot_for_workers(tx).await?;
        let table_refill_runtime_config = PbTableRefillRuntimeConfig {
            table_cache_refill_policies: Some(table_cache_refill_policies),
            serving_table_vnode_mappings: Some(
                self.serving_vnode_mapping
                    .table_vnode_mapping_for_snapshot(worker_id, tx),
            ),
            version: catalog_version,
        };

        Ok(MetaSnapshot {
            tables,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            meta_backup_manifest_id: Some(MetaBackupManifestId {
                id: meta_backup_manifest_id,
            }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: hummock_write_limits,
            }),
            cluster_resource: Some(cluster_resource),
            table_refill_runtime_config: Some(table_refill_runtime_config),
            ..Default::default()
        })
    }

    async fn compute_subscribe(&self, tx: &SnapshotMarkerSender) -> MetaResult<MetaSnapshot> {
        let (secrets, catalog_version) = self.get_decrypted_secret_snapshot_for_compute(tx).await?;
        let cluster_resource = self.get_cluster_resource_snapshot_for_workers(tx).await?;

        Ok(MetaSnapshot {
            secrets,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = BoxStream<'static, Notification>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let host_address = req.get_host()?.clone();
        let subscribe_type = req.get_subscribe_type()?;

        let worker_key = WorkerKey(host_address.clone());

        let start = std::time::Instant::now();
        tracing::info!(
            ?host_address,
            ?subscribe_type,
            "notification subscribe: building initial snapshot"
        );
        let (tx, rx) = self
            .env
            .notification_manager_ref()
            .start_subscription(subscribe_type, worker_key)?;

        let meta_snapshot = match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe(&tx).await?,
            SubscribeType::Frontend => self.frontend_subscribe(&tx).await?,
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe(req.get_worker_id(), &tx).await?
            }
            SubscribeType::Compute => self.compute_subscribe(&tx).await?,
            SubscribeType::Unspecified => unreachable!(),
        };

        tx.finish(subscribe_type);
        tracing::info!(
            ?host_address,
            ?subscribe_type,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "notification subscribe: initial snapshot prepared"
        );
        Ok(Response::new(
            notification_stream(subscribe_type, rx, meta_snapshot).boxed(),
        ))
    }
}

#[cfg(all(test, feature = "test"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::FutureExt;
    use risingwave_common::catalog::DEFAULT_SUPER_USER_ID;
    use risingwave_meta::backup_restore::BackupManager;
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::rpc::metrics::MetaMetrics;
    use risingwave_pb::catalog::Database;
    use risingwave_pb::common::HostAddress;
    use risingwave_pb::meta::subscribe_response::Info;

    use super::*;

    #[tokio::test]
    async fn test_initial_snapshot_and_following_catalog_updates() -> anyhow::Result<()> {
        let (env, hummock_manager, _, worker_id) = setup_compute_env(1234).await;
        let metadata_manager = hummock_manager.metadata_manager().clone();
        let backup_manager = BackupManager::new(
            env.clone(),
            hummock_manager.clone(),
            Arc::new(MetaMetrics::default()),
            "memory",
            "notification_snapshot_test",
        )
        .await?;
        let service = NotificationServiceImpl {
            env,
            metadata_manager,
            hummock_manager,
            backup_manager,
            serving_vnode_mapping: Arc::new(Default::default()),
        };
        service
            .metadata_manager
            .catalog_controller
            .create_database(Database {
                name: "before_subscribe".to_owned(),
                owner: DEFAULT_SUPER_USER_ID,
                ..Default::default()
            })
            .await?;

        for subscribe_type in [
            SubscribeType::Frontend,
            SubscribeType::Hummock,
            SubscribeType::Compute,
            SubscribeType::Compactor,
        ] {
            let response = tokio::time::timeout(
                Duration::from_secs(10),
                service.subscribe(Request::new(SubscribeRequest {
                    subscribe_type: subscribe_type as i32,
                    host: Some(HostAddress {
                        host: "127.0.0.1".to_owned(),
                        port: 1234,
                    }),
                    worker_id,
                })),
            )
            .await
            .expect("snapshot lock acquisition must not deadlock")?;
            let mut rx = response.into_inner();
            let Some(Info::Snapshot(snapshot)) = rx.next().await.unwrap().unwrap().info else {
                panic!("the first message must be the initial snapshot");
            };
            assert!(snapshot.cluster_resource.is_some());
            assert!(rx.next().now_or_never().is_none());

            if subscribe_type == SubscribeType::Frontend {
                assert!(
                    snapshot
                        .databases
                        .iter()
                        .any(|db| db.name == "before_subscribe")
                );
                let snapshot_version = snapshot.version.unwrap().catalog_version;
                let (version, _) = service
                    .metadata_manager
                    .catalog_controller
                    .create_database(Database {
                        name: "after_subscribe".to_owned(),
                        owner: DEFAULT_SUPER_USER_ID,
                        ..Default::default()
                    })
                    .await?;
                let notification = rx.next().await.unwrap().unwrap();
                assert!(notification.version > snapshot_version);
                assert!(
                    matches!(notification.info, Some(Info::Database(db)) if db.name == "after_subscribe")
                );
                let mut final_version = notification.version;
                while final_version < version {
                    let notification = rx.next().await.unwrap().unwrap();
                    assert!(notification.version > final_version);
                    final_version = notification.version;
                }
                assert_eq!(final_version, version);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_catalog_snapshot_wait_does_not_block_hummock_version_writer() -> anyhow::Result<()>
    {
        let (env, hummock_manager, _, worker_id) = setup_compute_env(1234).await;
        let backup_manager = BackupManager::new(
            env.clone(),
            hummock_manager.clone(),
            Arc::new(MetaMetrics::default()),
            "memory",
            "notification_snapshot_lock_test",
        )
        .await?;
        let service = NotificationServiceImpl {
            metadata_manager: hummock_manager.metadata_manager().clone(),
            env,
            hummock_manager,
            backup_manager,
            serving_vnode_mapping: Arc::new(Default::default()),
        };
        let catalog_guard = service
            .metadata_manager
            .catalog_controller
            .get_inner_write_guard()
            .await;
        let mut subscribe = Box::pin(service.subscribe(Request::new(SubscribeRequest {
            subscribe_type: SubscribeType::Frontend as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_owned(),
                port: 1234,
            }),
            worker_id,
        })));
        // Drive initialization until it is blocked on the catalog, rather than depending on task timing.
        assert!(futures::poll!(&mut subscribe).is_pending());
        tokio::time::timeout(
            Duration::from_secs(10),
            service.hummock_manager.disable_commit_epoch(),
        )
        .await
        .expect("catalog snapshot waits must not retain the Hummock versioning read lock");
        drop(catalog_guard);
        let mut stream = subscribe.await?.into_inner();
        assert!(matches!(
            stream.next().await.unwrap()?.info,
            Some(Info::Snapshot(_))
        ));
        Ok(())
    }
}
