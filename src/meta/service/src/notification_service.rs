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
use risingwave_common::secret::{LocalSecretManager, SecretEncryption};
use risingwave_hummock_sdk::FrontendHummockVersion;
use risingwave_meta::MetaResult;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::WorkerId;
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::catalog::Secret;
use risingwave_pb::common::WorkerType;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, GetSessionParamsResponse, MetaSnapshot, SubscribeRequest,
    SubscribeType,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::{MetaSrvEnv, Notification, WorkerKey};
use crate::serving::ServingVnodeMappingRef;
use crate::table_refill::build_hummock_table_refill_runtime_config;

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

    // Snapshot lock order: Hummock versioning -> compaction group manager -> catalog -> cluster
    // -> session parameters -> backup manifest -> notification version generator. In particular,
    // commit_epoch holds Hummock locks while reading the catalog for time travel, so catalog must
    // not be acquired first. Keep every state guard until the snapshot is sent and registered.
    // Acquire shared actor info and serving mapping guards only AFTER the last await, and acquire
    // the notification manager's synchronous core mutex last. Do not reacquire locks via getters.

    async fn compactor_subscribe(
        &self,
        worker_key: WorkerKey,
    ) -> MetaResult<UnboundedReceiver<Notification>> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let cluster_guard = self
            .metadata_manager
            .cluster_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let catalog_version = self.env.notification_manager().current_version().await;
        let cluster_resource = cluster_guard.cluster_resource().await?;

        let snapshot = MetaSnapshot {
            tables,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        };
        Ok(self.env.notification_manager().subscribe_with_snapshot(
            SubscribeType::Compactor,
            worker_key,
            snapshot,
        ))
    }

    async fn frontend_subscribe(
        &self,
        worker_key: WorkerKey,
    ) -> MetaResult<UnboundedReceiver<Notification>> {
        let version_guard = self.hummock_manager.read_version().await;
        let hummock_version = version_guard.version();
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let cluster_guard = self
            .metadata_manager
            .cluster_controller
            .get_inner_read_guard()
            .await;
        let session_params_manager = self.env.session_params_manager_impl_ref();
        let session_guard = session_params_manager.get_params_read_guard().await;
        let session_params = &*session_guard;
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
        let decrypted_secrets = self.decrypt_secrets(secrets)?;
        let compute_nodes = cluster_guard
            .list_workers(Some(WorkerType::ComputeNode.into()), Some(Running.into()))
            .await?;
        let frontends = cluster_guard
            .list_workers(Some(WorkerType::Frontend.into()), Some(Running.into()))
            .await?;
        let nodes = compute_nodes.into_iter().chain(frontends).collect();
        let cluster_resource = cluster_guard.cluster_resource().await?;
        let streaming_job_count = catalog_guard.count_streaming_jobs().await?;
        let catalog_version = self.env.notification_manager().current_version().await;
        let session_params = Some(GetSessionParamsResponse {
            params: serde_json::to_string(session_params)
                .context("failed to encode session params")?,
        });

        // No await is allowed while either parking_lot guard is held.
        let actor_guard = self.env.shared_actor_infos().read_guard();
        let serving_guard = self.serving_vnode_mapping.read_guard();
        let streaming_worker_slot_mappings = actor_guard.worker_slot_mappings();
        if streaming_job_count > 0 && streaming_worker_slot_mappings.is_empty() {
            tracing::warn!(
                streaming_job_count,
                "frontend subscribe returns empty streaming_worker_slot_mappings while streaming jobs exist; meta may still be recovering"
            );
        }
        let serving_worker_slot_mappings = serving_guard
            .iter()
            .map(|(&fragment_id, mapping)| FragmentWorkerSlotMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect();
        let snapshot = MetaSnapshot {
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
            hummock_version: Some(
                FrontendHummockVersion::from_version(hummock_version).to_protobuf(),
            ),
            version: Some(SnapshotVersion {
                catalog_version,
                worker_node_version: catalog_version,
                streaming_worker_slot_mapping_version: catalog_version,
            }),
            serving_worker_slot_mappings,
            streaming_worker_slot_mappings,
            session_params,
            object_dependencies,
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        };
        Ok(self.env.notification_manager().subscribe_with_snapshot(
            SubscribeType::Frontend,
            worker_key,
            snapshot,
        ))
    }

    async fn hummock_subscribe(
        &self,
        worker_id: WorkerId,
        worker_key: WorkerKey,
    ) -> MetaResult<UnboundedReceiver<Notification>> {
        let version_guard = self.hummock_manager.read_version().await;
        let hummock_version = version_guard.version();
        let limits_guard = self.hummock_manager.read_write_limits().await;
        let hummock_write_limits = limits_guard.write_limits().clone();
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let cluster_guard = self
            .metadata_manager
            .cluster_controller
            .get_inner_read_guard()
            .await;
        let backup_store = self.backup_manager.snapshot_storage();
        let manifest_guard = backup_store.0.manifest_read_guard().await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let table_cache_refill_policies =
            catalog_guard.table_cache_refill_policies_snapshot().await?;
        let fragment_serving_infos = catalog_guard.fragment_serving_infos().await?;
        let cluster_resource = cluster_guard.cluster_resource().await?;
        let catalog_version = self.env.notification_manager().current_version().await;

        // All asynchronous work is complete before retaining the serving mappings guard.
        let serving_guard = self.serving_vnode_mapping.read_guard();
        let table_refill_runtime_config = build_hummock_table_refill_runtime_config(
            &serving_guard,
            worker_id,
            table_cache_refill_policies,
            &fragment_serving_infos,
            catalog_version,
        );
        let snapshot = MetaSnapshot {
            tables,
            hummock_version: Some(hummock_version.into()),
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            meta_backup_manifest_id: Some(MetaBackupManifestId {
                id: manifest_guard.manifest_id,
            }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: hummock_write_limits,
            }),
            cluster_resource: Some(cluster_resource),
            table_refill_runtime_config: Some(table_refill_runtime_config),
            ..Default::default()
        };
        Ok(self.env.notification_manager().subscribe_with_snapshot(
            SubscribeType::Hummock,
            worker_key,
            snapshot,
        ))
    }

    async fn compute_subscribe(
        &self,
        worker_key: WorkerKey,
    ) -> MetaResult<UnboundedReceiver<Notification>> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let cluster_guard = self
            .metadata_manager
            .cluster_controller
            .get_inner_read_guard()
            .await;
        let secrets = self.decrypt_secrets(catalog_guard.list_secrets().await?)?;
        let cluster_resource = cluster_guard.cluster_resource().await?;
        let catalog_version = self.env.notification_manager().current_version().await;
        let snapshot = MetaSnapshot {
            secrets,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        };
        Ok(self.env.notification_manager().subscribe_with_snapshot(
            SubscribeType::Compute,
            worker_key,
            snapshot,
        ))
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = UnboundedReceiverStream<Notification>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let host_address = req.get_host()?;
        let subscribe_type = req.get_subscribe_type()?;
        let worker_key = WorkerKey(host_address.clone());
        let start = std::time::Instant::now();
        tracing::info!(
            ?host_address,
            ?subscribe_type,
            "notification subscribe: building initial snapshot"
        );
        let rx = match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe(worker_key).await?,
            SubscribeType::Frontend => self.frontend_subscribe(worker_key).await?,
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe(req.get_worker_id(), worker_key)
                    .await?
            }
            SubscribeType::Compute => self.compute_subscribe(worker_key).await?,
            SubscribeType::Unspecified => unreachable!(),
        };
        tracing::info!(
            ?host_address,
            ?subscribe_type,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "notification subscribe: initial snapshot sent and subscriber registered"
        );
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}

#[cfg(all(test, feature = "test"))]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

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
            let mut rx = response.into_inner().into_inner();
            let Some(Info::Snapshot(snapshot)) = rx.try_recv().unwrap().unwrap().info else {
                panic!("the first message must be the initial snapshot");
            };
            assert!(snapshot.cluster_resource.is_some());
            assert!(rx.try_recv().is_err());

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
                let notification = rx.try_recv().unwrap().unwrap();
                assert!(notification.version > snapshot_version);
                assert!(
                    matches!(notification.info, Some(Info::Database(db)) if db.name == "after_subscribe")
                );
                let mut final_version = notification.version;
                while let Ok(notification) = rx.try_recv() {
                    let notification = notification.unwrap();
                    assert!(notification.version > final_version);
                    final_version = notification.version;
                }
                assert_eq!(final_version, version);
            }
        }
        Ok(())
    }
}
