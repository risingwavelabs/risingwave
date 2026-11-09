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
use risingwave_meta::controller::catalog::Catalog;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::WorkerId;
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::catalog::{Secret, Table};
use risingwave_pb::common::ClusterResource;
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, GetSessionParamsResponse, MetaSnapshot, ObjectDependency,
    PbTableCacheRefillPolicies, PbTableRefillRuntimeConfig, SubscribeRequest, SubscribeType,
};
use risingwave_pb::user::UserInfo;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::{
    MetaSrvEnv, Notification, NotificationVersion, SnapshotSubscription, WorkerKey,
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

    async fn get_catalog_snapshot(
        &self,
        subscription: &mut SnapshotSubscription,
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
        let (catalog, users) = catalog_guard.snapshot().await?;
        let object_dependencies = catalog_guard.list_created_object_dependencies().await?;
        subscription.start_forwarding(&*catalog_guard);
        // The version only changes under the write lock of the catalog.
        let notification_version = self.env.notification_manager().current_version().await;
        Ok((catalog, users, object_dependencies, notification_version))
    }

    /// Get decrypted secret snapshot
    async fn get_decrypted_secret_snapshot(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<Vec<Secret>> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let secrets = catalog_guard.list_secrets().await?;
        subscription.start_forwarding(&*catalog_guard);

        self.decrypt_secrets(secrets)
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

    fn get_serving_vnode_mappings(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> Vec<FragmentWorkerSlotMapping> {
        self.serving_vnode_mapping
            .all_for_snapshot(subscription)
            .iter()
            .map(|(&fragment_id, mapping)| FragmentWorkerSlotMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect()
    }

    async fn get_tables_snapshot(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<Vec<Table>> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        subscription.start_forwarding(&*catalog_guard);
        Ok(tables)
    }

    async fn get_hummock_catalog_snapshot(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<(Vec<Table>, PbTableCacheRefillPolicies)> {
        let catalog_guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let mut tables = catalog_guard.list_all_state_tables().await?;
        tables.extend(catalog_guard.dropped_tables.values().cloned());
        let table_cache_refill_policies =
            catalog_guard.table_cache_refill_policies_snapshot().await?;
        subscription.start_forwarding(&*catalog_guard);
        Ok((tables, table_cache_refill_policies))
    }

    async fn get_cluster_resource(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<ClusterResource> {
        self.metadata_manager
            .cluster_controller
            .get_cluster_resource_snapshot(subscription)
            .await
    }

    async fn compactor_subscribe(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<MetaSnapshot> {
        let tables = self.get_tables_snapshot(subscription).await?;
        let cluster_resource = self.get_cluster_resource(subscription).await?;

        Ok(MetaSnapshot {
            tables,
            version: Some(SnapshotVersion::default()),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
    }

    async fn frontend_subscribe(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<MetaSnapshot> {
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
        ) = self.get_catalog_snapshot(subscription).await?;

        // Use the plain text secret value for frontend. The secret value will be masked in frontend handle.
        let decrypted_secrets = self.decrypt_secrets(secrets)?;

        let streaming_worker_slot_mappings = self
            .metadata_manager
            .catalog_controller
            .get_worker_slot_mappings(subscription);

        let streaming_job_count = self.metadata_manager.count_streaming_job().await?;
        if streaming_job_count > 0 && streaming_worker_slot_mappings.is_empty() {
            tracing::warn!(
                streaming_job_count,
                "frontend subscribe returns empty streaming_worker_slot_mappings while streaming jobs exist; meta may still be recovering"
            );
        }

        let serving_worker_slot_mappings = self.get_serving_vnode_mappings(subscription);

        let (nodes, cluster_resource) = self
            .metadata_manager
            .cluster_controller
            .get_worker_nodes_snapshot(subscription)
            .await?;

        let hummock_version = self
            .hummock_manager
            .on_current_version_for_snapshot(subscription, |version| {
                FrontendHummockVersion::from_version(version).to_protobuf()
            })
            .await;

        let session_params = self
            .env
            .session_params_manager_impl_ref()
            .get_params_for_snapshot(subscription)
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
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<MetaSnapshot> {
        let (tables, table_cache_refill_policies) =
            self.get_hummock_catalog_snapshot(subscription).await?;
        let hummock_version = self
            .hummock_manager
            .on_current_version_for_snapshot(subscription, |version| version.into())
            .await;
        let hummock_write_limits = self
            .hummock_manager
            .write_limits_for_snapshot(subscription)
            .await;
        let meta_backup_manifest_id = self.backup_manager.manifest().await.manifest_id;
        let cluster_resource = self.get_cluster_resource(subscription).await?;
        let serving_table_vnode_mappings = self
            .serving_vnode_mapping
            .table_vnode_mapping_for_snapshot(worker_id, subscription);

        Ok(MetaSnapshot {
            tables,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion::default()),
            meta_backup_manifest_id: Some(MetaBackupManifestId {
                id: meta_backup_manifest_id,
            }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: hummock_write_limits,
            }),
            cluster_resource: Some(cluster_resource),
            table_refill_runtime_config: Some(PbTableRefillRuntimeConfig {
                table_cache_refill_policies: Some(table_cache_refill_policies),
                serving_table_vnode_mappings: Some(serving_table_vnode_mappings),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn compute_subscribe(
        &self,
        subscription: &mut SnapshotSubscription,
    ) -> MetaResult<MetaSnapshot> {
        let secrets = self.get_decrypted_secret_snapshot(subscription).await?;
        let cluster_resource = self.get_cluster_resource(subscription).await?;

        Ok(MetaSnapshot {
            secrets,
            version: Some(SnapshotVersion::default()),
            cluster_resource: Some(cluster_resource),
            ..Default::default()
        })
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
        let host_address = req.get_host()?.clone();
        let subscribe_type = req.get_subscribe_type()?;

        let worker_key = WorkerKey(host_address);

        let (tx, rx) = mpsc::unbounded_channel();
        let mut subscription =
            self.env
                .notification_manager()
                .start_subscription(subscribe_type, worker_key, tx);

        let meta_snapshot = match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe(&mut subscription).await?,
            SubscribeType::Frontend => self.frontend_subscribe(&mut subscription).await?,
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe(req.get_worker_id(), &mut subscription)
                    .await?
            }
            SubscribeType::Compute => self.compute_subscribe(&mut subscription).await?,
            SubscribeType::Unspecified => unreachable!(),
        };

        subscription.send_snapshot(meta_snapshot);

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
