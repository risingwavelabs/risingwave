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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeManagerRef;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::license::LicenseManager;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common_service::ObserverState;
use risingwave_hummock_sdk::FrontendHummockVersion;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::{HummockVersionDeltas, HummockVersionStats};
use risingwave_pb::meta::object::{ObjectInfo, PbObjectInfo};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{FragmentWorkerSlotMapping, MetaSnapshot, SubscribeResponse};
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::watch::Sender;

use crate::catalog::FragmentId;
use crate::catalog::root_catalog::Catalog;
use crate::scheduler::HummockSnapshotManagerRef;
use crate::user::user_manager::UserInfoManager;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompletedObserverRecovery {
    pub epoch: u64,
    pub catalog_version: CatalogVersion,
    pub streaming_worker_slot_mapping_version: u64,
}

pub struct FrontendObserverNode {
    worker_node_manager: WorkerNodeManagerRef,
    version: CatalogVersion,
    streaming_worker_slot_mapping_version: u64,
    streaming_worker_slot_mapping_ready: bool,
    /// Set to `true` when a `StreamingWorkerSlotMapping` notification with
    /// `mapping: None` is replayed during init buffering (before
    /// `handle_initialization_finished` runs).  This is the recovery-complete
    /// signal from meta; we track it so we can detect the race where the signal
    /// arrives before the deferred-recovery flag is set.
    recovery_complete_seen: bool,
    recovery_deferred: bool,
    catalog_updated_tx: Sender<CatalogVersion>,
    streaming_worker_slot_mapping_updated_tx: Sender<u64>,
    completed_observer_recovery_tx: Sender<CompletedObserverRecovery>,
    catalog: Arc<RwLock<Catalog>>,
    user_info_manager: Arc<RwLock<UserInfoManager>>,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    system_params_manager: LocalSystemParamsManagerRef,
    session_params: Arc<RwLock<SessionConfig>>,
    compute_client_pool: ComputeClientPoolRef,
}

impl ObserverState for FrontendObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Frontend
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        // TODO: this clone can be avoided
        match info.to_owned() {
            Info::Database(_)
            | Info::Schema(_)
            | Info::ObjectGroup(_)
            | Info::Function(_)
            | Info::Connection(_) => {
                self.handle_catalog_notification(resp);
            }
            Info::Secret(_) => {
                self.handle_catalog_notification(resp.clone());
                self.handle_secret_notification(resp);
            }
            Info::Node(node) => {
                self.update_worker_node_manager(resp.operation(), node);
            }
            Info::User(_) => {
                self.handle_user_notification(resp);
            }
            Info::Snapshot(_) => {
                panic!(
                    "receiving a snapshot in the middle is unsupported now {:?}",
                    resp
                )
            }
            Info::HummockVersionDeltas(deltas) => {
                self.handle_hummock_snapshot_notification(deltas);
            }
            Info::MetaBackupManifestId(_) => {
                panic!("frontend node should not receive MetaBackupManifestId");
            }
            Info::HummockWriteLimits(_) => {
                panic!("frontend node should not receive HummockWriteLimits");
            }
            Info::SystemParams(p) => {
                self.system_params_manager.try_set_params(p);
            }
            Info::SessionParam(p) => {
                self.session_params
                    .write()
                    .set(&p.param, p.value().to_owned(), &mut ())
                    .unwrap();
            }
            Info::HummockStats(stats) => {
                self.handle_table_stats_notification(stats);
            }
            Info::StreamingWorkerSlotMapping(_) => self.handle_fragment_mapping_notification(resp),
            Info::ServingWorkerSlotMappings(m) => {
                self.handle_fragment_serving_mapping_notification(m.mappings, resp.operation())
            }
            Info::Recovery(_) => {
                self.compute_client_pool.invalidate_all();
            }
            Info::ClusterResource(resource) => {
                LicenseManager::get().update_cluster_resource(resource);
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        self.recovery_deferred = false;
        let mut catalog_guard = self.catalog.write();
        let mut user_guard = self.user_info_manager.write();
        catalog_guard.clear();
        user_guard.clear();

        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };
        let MetaSnapshot {
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
            users,
            nodes,
            hummock_version,
            meta_backup_manifest_id: _,
            hummock_write_limits: _,
            streaming_worker_slot_mappings,
            serving_worker_slot_mappings,
            session_params,
            version,
            secrets,
            cluster_resource,
            object_dependencies,
        } = snapshot;

        for db in databases {
            catalog_guard.create_database(&db)
        }
        for schema in schemas {
            catalog_guard.create_schema(&schema)
        }
        for source in sources {
            catalog_guard.create_source(&source)
        }
        for sink in sinks {
            catalog_guard.create_sink(&sink)
        }
        for subscription in subscriptions {
            catalog_guard.create_subscription(&subscription)
        }
        for table in tables {
            catalog_guard.create_table(&table)
        }
        for index in indexes {
            catalog_guard.create_index(&index)
        }
        for view in views {
            catalog_guard.create_view(&view)
        }
        for function in functions {
            catalog_guard.create_function(&function)
        }
        for connection in connections {
            catalog_guard.create_connection(&connection)
        }
        for secret in &secrets {
            catalog_guard.create_secret(secret)
        }
        catalog_guard.set_object_dependencies(object_dependencies);
        for user in users {
            user_guard.create_user(user)
        }

        self.worker_node_manager.refresh(
            nodes,
            convert_worker_slot_mapping(&streaming_worker_slot_mappings),
            convert_worker_slot_mapping(&serving_worker_slot_mappings),
        );
        self.hummock_snapshot_manager
            .init(FrontendHummockVersion::from_protobuf(
                hummock_version.unwrap(),
            ));

        let snapshot_version = version.unwrap();
        drop(catalog_guard);
        drop(user_guard);
        self.version = snapshot_version.catalog_version;
        self.streaming_worker_slot_mapping_ready =
            snapshot_version.streaming_worker_slot_mapping_ready;
        self.recovery_complete_seen = false;
        self.reset_streaming_worker_slot_mapping_version(
            snapshot_version.streaming_worker_slot_mapping_version,
        );
        self.catalog_updated_tx
            .send(snapshot_version.catalog_version)
            .unwrap();
        *self.session_params.write() =
            serde_json::from_str(&session_params.unwrap().params).unwrap();
        LocalSecretManager::global().init_secrets(secrets);
        LicenseManager::get().update_cluster_resource(cluster_resource.unwrap());
    }

    fn handle_initialization_finished(&mut self) {
        if self.streaming_worker_slot_mapping_ready {
            self.signal_completed_recovery();
        } else if self.recovery_complete_seen {
            // The recovery-complete notification (mapping=None) was already buffered
            // and replayed before this callback.  Signal immediately.
            self.signal_completed_recovery();
        } else {
            // Barrier manager is still recovering — streaming jobs exist but their
            // fragment mappings are not yet available.  Defer the recovery signal
            // until the recovery-complete notification arrives.
            self.recovery_deferred = true;
        }
    }
}

impl FrontendObserverNode {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        catalog: Arc<RwLock<Catalog>>,
        catalog_updated_tx: Sender<CatalogVersion>,
        streaming_worker_slot_mapping_updated_tx: Sender<u64>,
        completed_observer_recovery_tx: Sender<CompletedObserverRecovery>,
        user_info_manager: Arc<RwLock<UserInfoManager>>,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        system_params_manager: LocalSystemParamsManagerRef,
        session_params: Arc<RwLock<SessionConfig>>,
        compute_client_pool: ComputeClientPoolRef,
    ) -> Self {
        Self {
            version: 0,
            streaming_worker_slot_mapping_version: 0,
            streaming_worker_slot_mapping_ready: false,
            recovery_complete_seen: false,
            recovery_deferred: false,
            worker_node_manager,
            catalog,
            catalog_updated_tx,
            streaming_worker_slot_mapping_updated_tx,
            completed_observer_recovery_tx,
            user_info_manager,
            hummock_snapshot_manager,
            system_params_manager,
            session_params,
            compute_client_pool,
        }
    }

    fn handle_table_stats_notification(&mut self, table_stats: HummockVersionStats) {
        let mut catalog_guard = self.catalog.write();
        catalog_guard.set_table_stats(table_stats);
    }

    fn handle_catalog_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };
        tracing::trace!(op = ?resp.operation(), ?info, "handle catalog notification");

        let mut catalog_guard = self.catalog.write();
        match info {
            Info::Database(database) => match resp.operation() {
                Operation::Add => catalog_guard.create_database(database),
                Operation::Delete => catalog_guard.drop_database(database.id),
                Operation::Update => catalog_guard.update_database(database),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Schema(schema) => match resp.operation() {
                Operation::Add => catalog_guard.create_schema(schema),
                Operation::Delete => catalog_guard.drop_schema(schema.database_id, schema.id),
                Operation::Update => catalog_guard.update_schema(schema),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::ObjectGroup(object_group) => {
                if !object_group.dependencies.is_empty() {
                    catalog_guard.insert_object_dependencies(object_group.dependencies.clone());
                }
                for object in &object_group.objects {
                    let Some(obj) = object.object_info.as_ref() else {
                        continue;
                    };
                    match obj {
                        ObjectInfo::Database(db) => match resp.operation() {
                            Operation::Add => catalog_guard.create_database(db),
                            Operation::Delete => catalog_guard.drop_database(db.id),
                            Operation::Update => catalog_guard.update_database(db),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        ObjectInfo::Schema(schema) => match resp.operation() {
                            Operation::Add => catalog_guard.create_schema(schema),
                            Operation::Delete => {
                                catalog_guard.drop_schema(schema.database_id, schema.id)
                            }
                            Operation::Update => catalog_guard.update_schema(schema),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::Table(table) => match resp.operation() {
                            Operation::Add => catalog_guard.create_table(table),
                            Operation::Delete => catalog_guard.drop_table(
                                table.database_id,
                                table.schema_id,
                                table.id,
                            ),
                            Operation::Update => {
                                let old_fragment_id = catalog_guard
                                    .get_any_table_by_id(table.id)
                                    .unwrap()
                                    .fragment_id;
                                catalog_guard.update_table(table);
                                if old_fragment_id != table.fragment_id {
                                    // FIXME: the frontend node delete its fragment for the update
                                    // operation by itself.
                                    self.worker_node_manager
                                        .remove_streaming_fragment_mapping(&old_fragment_id);
                                }
                            }
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::Source(source) => match resp.operation() {
                            Operation::Add => catalog_guard.create_source(source),
                            Operation::Delete => catalog_guard.drop_source(
                                source.database_id,
                                source.schema_id,
                                source.id,
                            ),
                            Operation::Update => catalog_guard.update_source(source),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::Sink(sink) => match resp.operation() {
                            Operation::Add => catalog_guard.create_sink(sink),
                            Operation::Delete => {
                                catalog_guard.drop_sink(sink.database_id, sink.schema_id, sink.id)
                            }
                            Operation::Update => catalog_guard.update_sink(sink),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::Subscription(subscription) => match resp.operation() {
                            Operation::Add => catalog_guard.create_subscription(subscription),
                            Operation::Delete => catalog_guard.drop_subscription(
                                subscription.database_id,
                                subscription.schema_id,
                                subscription.id,
                            ),
                            Operation::Update => catalog_guard.update_subscription(subscription),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::Index(index) => match resp.operation() {
                            Operation::Add => catalog_guard.create_index(index),
                            Operation::Delete => catalog_guard.drop_index(
                                index.database_id,
                                index.schema_id,
                                index.id,
                            ),
                            Operation::Update => catalog_guard.update_index(index),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        PbObjectInfo::View(view) => match resp.operation() {
                            Operation::Add => catalog_guard.create_view(view),
                            Operation::Delete => {
                                catalog_guard.drop_view(view.database_id, view.schema_id, view.id)
                            }
                            Operation::Update => catalog_guard.update_view(view),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        ObjectInfo::Function(function) => match resp.operation() {
                            Operation::Add => catalog_guard.create_function(function),
                            Operation::Delete => catalog_guard.drop_function(
                                function.database_id,
                                function.schema_id,
                                function.id,
                            ),
                            Operation::Update => catalog_guard.update_function(function),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        ObjectInfo::Connection(connection) => match resp.operation() {
                            Operation::Add => catalog_guard.create_connection(connection),
                            Operation::Delete => catalog_guard.drop_connection(
                                connection.database_id,
                                connection.schema_id,
                                connection.id,
                            ),
                            Operation::Update => catalog_guard.update_connection(connection),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        ObjectInfo::Secret(secret) => {
                            let mut secret = secret.clone();
                            // The secret value should not be revealed to users. So mask it in the frontend catalog.
                            secret.value =
                                "SECRET VALUE SHOULD NOT BE REVEALED".as_bytes().to_vec();
                            match resp.operation() {
                                Operation::Add => catalog_guard.create_secret(&secret),
                                Operation::Delete => catalog_guard.drop_secret(
                                    secret.database_id,
                                    secret.schema_id,
                                    secret.id,
                                ),
                                Operation::Update => catalog_guard.update_secret(&secret),
                                _ => panic!("receive an unsupported notify {:?}", resp),
                            }
                        }
                    }
                }
            }
            Info::Function(function) => match resp.operation() {
                Operation::Add => catalog_guard.create_function(function),
                Operation::Delete => catalog_guard.drop_function(
                    function.database_id,
                    function.schema_id,
                    function.id,
                ),
                Operation::Update => catalog_guard.update_function(function),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Connection(connection) => match resp.operation() {
                Operation::Add => catalog_guard.create_connection(connection),
                Operation::Delete => catalog_guard.drop_connection(
                    connection.database_id,
                    connection.schema_id,
                    connection.id,
                ),
                Operation::Update => catalog_guard.update_connection(connection),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Secret(secret) => {
                let mut secret = secret.clone();
                // The secret value should not be revealed to users. So mask it in the frontend catalog.
                secret.value = "SECRET VALUE SHOULD NOT BE REVEALED".as_bytes().to_vec();
                match resp.operation() {
                    Operation::Add => catalog_guard.create_secret(&secret),
                    Operation::Delete => {
                        catalog_guard.drop_secret(secret.database_id, secret.schema_id, secret.id)
                    }
                    Operation::Update => catalog_guard.update_secret(&secret),
                    _ => panic!("receive an unsupported notify {:?}", resp),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            resp.version > self.version,
            "resp version={:?}, current version={:?}",
            resp.version,
            self.version
        );
        self.version = resp.version;
        self.catalog_updated_tx.send(resp.version).unwrap();
    }

    fn handle_user_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let mut user_guard = self.user_info_manager.write();
        match info {
            Info::User(user) => match resp.operation() {
                Operation::Add => user_guard.create_user(user.clone()),
                Operation::Delete => user_guard.drop_user(user.id),
                Operation::Update => user_guard.update_user(user.clone()),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            _ => unreachable!(),
        }
        assert!(
            resp.version > self.version,
            "resp version={:?}, current version={:?}",
            resp.version,
            self.version
        );
        self.version = resp.version;
        self.catalog_updated_tx.send(resp.version).unwrap();
    }

    fn handle_fragment_mapping_notification(&mut self, resp: SubscribeResponse) {
        if resp.version != 0 && resp.version < self.streaming_worker_slot_mapping_version {
            tracing::debug!(
                notification_version = resp.version,
                current_version = self.streaming_worker_slot_mapping_version,
                "Skip stale streaming worker slot mapping notification"
            );
            return;
        }

        let Some(info) = resp.info.as_ref() else {
            return;
        };
        match info {
            Info::StreamingWorkerSlotMapping(streaming_worker_slot_mapping) => {
                // A notification with `mapping: None` is a version-only bump — the
                // recovery-complete signal from meta.  Skip data processing but still
                // advance the version so deferred recovery can fire.
                if let Some(pb_mapping) = streaming_worker_slot_mapping.mapping.as_ref() {
                    let fragment_id = streaming_worker_slot_mapping.fragment_id;
                    let mapping = WorkerSlotMapping::from_protobuf(pb_mapping);

                    match resp.operation() {
                        Operation::Add => {
                            self.worker_node_manager
                                .insert_streaming_fragment_mapping(fragment_id, mapping);
                        }
                        Operation::Delete => {
                            self.worker_node_manager
                                .remove_streaming_fragment_mapping(&fragment_id);
                        }
                        Operation::Update => {
                            self.worker_node_manager
                                .update_streaming_fragment_mapping(fragment_id, mapping);
                        }
                        _ => panic!("receive an unsupported notify {:?}", resp),
                    }
                } else {
                    // mapping=None is the recovery-complete signal from meta.
                    self.recovery_complete_seen = true;
                    if self.recovery_deferred {
                        // The deferred recovery can now fire — we know recovery
                        // is truly complete, not just a partial mapping update.
                        // Advance version first so the signal carries the right value.
                        self.advance_streaming_worker_slot_mapping_version(resp.version);
                        self.signal_completed_recovery();
                        return;
                    }
                }

                self.advance_streaming_worker_slot_mapping_version(resp.version);
            }
            _ => unreachable!(),
        }
    }

    fn handle_fragment_serving_mapping_notification(
        &mut self,
        mappings: Vec<FragmentWorkerSlotMapping>,
        op: Operation,
    ) {
        match op {
            Operation::Add | Operation::Update => {
                self.worker_node_manager
                    .upsert_serving_fragment_mapping(convert_worker_slot_mapping(&mappings));
            }
            Operation::Delete => self.worker_node_manager.remove_serving_fragment_mapping(
                mappings
                    .into_iter()
                    .map(|m| m.fragment_id)
                    .collect_vec()
                    .as_slice(),
            ),
            Operation::Snapshot => {
                self.worker_node_manager
                    .set_serving_fragment_mapping(convert_worker_slot_mapping(&mappings));
            }
            _ => panic!("receive an unsupported notify {:?}", op),
        }
    }

    /// Update max committed epoch in `HummockSnapshotManager`.
    fn handle_hummock_snapshot_notification(&self, deltas: HummockVersionDeltas) {
        self.hummock_snapshot_manager.update(deltas);
    }

    fn advance_streaming_worker_slot_mapping_version(&mut self, version: u64) {
        if version <= self.streaming_worker_slot_mapping_version {
            return;
        }

        self.reset_streaming_worker_slot_mapping_version(version);
    }

    fn reset_streaming_worker_slot_mapping_version(&mut self, version: u64) {
        if version == self.streaming_worker_slot_mapping_version {
            return;
        }

        self.streaming_worker_slot_mapping_version = version;
        self.streaming_worker_slot_mapping_updated_tx
            .send(version)
            .unwrap();
    }

    fn signal_completed_recovery(&mut self) {
        self.recovery_deferred = false;
        self.completed_observer_recovery_tx.send_modify(|recovery| {
            recovery.epoch += 1;
            recovery.catalog_version = self.version;
            recovery.streaming_worker_slot_mapping_version =
                self.streaming_worker_slot_mapping_version;
        });
    }

    fn handle_secret_notification(&mut self, resp: SubscribeResponse) {
        let resp_op = resp.operation();
        let Some(Info::Secret(secret)) = resp.info else {
            unreachable!();
        };
        match resp_op {
            Operation::Add => {
                LocalSecretManager::global().add_secret(secret.id, secret.value);
            }
            Operation::Delete => {
                LocalSecretManager::global().remove_secret(secret.id);
            }
            Operation::Update => {
                LocalSecretManager::global().update_secret(secret.id, secret.value);
            }
            _ => {
                panic!("invalid notification operation: {resp_op:?}");
            }
        }
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        tracing::debug!(
            "Update worker nodes, operation: {:?}, node: {:?}",
            operation,
            node
        );

        match operation {
            Operation::Add => self.worker_node_manager.add_worker_node(node),
            Operation::Delete => self.worker_node_manager.remove_worker_node(node),
            _ => (),
        }
    }
}

fn convert_worker_slot_mapping(
    worker_slot_mappings: &[FragmentWorkerSlotMapping],
) -> HashMap<FragmentId, WorkerSlotMapping> {
    worker_slot_mappings
        .iter()
        .map(
            |FragmentWorkerSlotMapping {
                 fragment_id,
                 mapping,
             }| {
                let mapping = WorkerSlotMapping::from_protobuf(mapping.as_ref().unwrap());
                (*fragment_id, mapping)
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeManager;
    use risingwave_common::catalog::CatalogVersion;
    use risingwave_common::config::RpcClientConfig;
    use risingwave_common::hash::{WorkerSlotId, WorkerSlotMapping};
    use risingwave_common::session_config::SessionConfig;
    use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
    use risingwave_common_service::ObserverState;
    use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
    use risingwave_pb::meta::subscribe_response::{Info, Operation};
    use risingwave_pb::meta::{
        FragmentWorkerSlotMapping, GetSessionParamsResponse, MetaSnapshot, SubscribeResponse,
    };
    use risingwave_rpc_client::ComputeClientPool;
    use tokio::sync::watch;

    use super::{CompletedObserverRecovery, FrontendObserverNode};
    use crate::catalog::FragmentId;
    use crate::catalog::root_catalog::Catalog;
    use crate::scheduler::HummockSnapshotManager;
    use crate::test_utils::MockFrontendMetaClient;
    use crate::user::user_manager::UserInfoManager;

    fn make_observer_node() -> (
        FrontendObserverNode,
        watch::Receiver<CatalogVersion>,
        watch::Receiver<u64>,
        watch::Receiver<CompletedObserverRecovery>,
    ) {
        let worker_node_manager = Arc::new(WorkerNodeManager::new());
        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let (streaming_worker_slot_mapping_updated_tx, streaming_worker_slot_mapping_updated_rx) =
            watch::channel(0);
        let (completed_observer_recovery_tx, completed_observer_recovery_rx) =
            watch::channel(CompletedObserverRecovery::default());
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let hummock_snapshot_manager = Arc::new(HummockSnapshotManager::new(Arc::new(
            MockFrontendMetaClient {},
        )));
        let system_params_manager = Arc::new(LocalSystemParamsManager::for_test());
        let session_params = Arc::new(RwLock::new(SessionConfig::default()));
        let compute_client_pool = Arc::new(ComputeClientPool::new(1, RpcClientConfig::default()));

        (
            FrontendObserverNode::new(
                worker_node_manager,
                catalog,
                catalog_updated_tx,
                streaming_worker_slot_mapping_updated_tx,
                completed_observer_recovery_tx,
                user_info_manager,
                hummock_snapshot_manager,
                system_params_manager,
                session_params,
                compute_client_pool,
            ),
            catalog_updated_rx,
            streaming_worker_slot_mapping_updated_rx,
            completed_observer_recovery_rx,
        )
    }

    fn streaming_mapping_notification(fragment_id: FragmentId, version: u64) -> SubscribeResponse {
        SubscribeResponse {
            status: None,
            operation: Operation::Add as i32,
            info: Some(Info::StreamingWorkerSlotMapping(
                FragmentWorkerSlotMapping {
                    fragment_id,
                    mapping: Some(
                        WorkerSlotMapping::new_single(WorkerSlotId::new(1.into(), 0)).to_protobuf(),
                    ),
                },
            )),
            version,
        }
    }

    fn snapshot_notification(version: u64, ready: bool) -> SubscribeResponse {
        SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(MetaSnapshot {
                hummock_version: Some(Default::default()),
                session_params: Some(GetSessionParamsResponse {
                    params: serde_json::to_string(&SessionConfig::default()).unwrap(),
                }),
                version: Some(SnapshotVersion {
                    catalog_version: 0,
                    worker_node_version: 0,
                    streaming_worker_slot_mapping_version: version,
                    streaming_worker_slot_mapping_ready: ready,
                }),
                cluster_resource: Some(Default::default()),
                ..Default::default()
            })),
            version: 0,
        }
    }

    #[tokio::test]
    async fn initialization_snapshot_resets_streaming_mapping_version() {
        let (mut observer_node, _catalog_version_rx, mut version_rx, _reinit_rx) =
            make_observer_node();

        observer_node.handle_notification(streaming_mapping_notification(1.into(), 5));
        assert_eq!(*version_rx.borrow_and_update(), 5);

        observer_node.handle_initialization_notification(snapshot_notification(1, true));
        assert_eq!(observer_node.streaming_worker_slot_mapping_version, 1);
        assert_eq!(*version_rx.borrow_and_update(), 1);

        observer_node.handle_notification(streaming_mapping_notification(2.into(), 2));
        assert_eq!(observer_node.streaming_worker_slot_mapping_version, 2);
        observer_node
            .worker_node_manager
            .get_streaming_fragment_mapping(&2.into())
            .unwrap();
    }

    #[tokio::test]
    async fn initialization_notification_does_not_signal_reinitialized_before_finish() {
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        observer_node.handle_initialization_notification(snapshot_notification(1, true));
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );

        observer_node.handle_initialization_finished();
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 1,
            }
        );
    }

    #[tokio::test]
    async fn initialization_finished_defers_recovery_when_mapping_not_ready() {
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        // ready=false simulates barrier manager still recovering
        // (streaming jobs exist but mappings are empty).
        observer_node.handle_initialization_notification(snapshot_notification(0, false));
        observer_node.handle_initialization_finished();

        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );

        // Normal mapping updates during recovery do NOT unblock the deferred signal.
        observer_node.handle_notification(streaming_mapping_notification(1.into(), 1));
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );

        // Only the recovery-complete notification (mapping=None) fires the signal.
        observer_node.handle_notification(recovery_complete_notification(2));

        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 2,
            }
        );
    }

    #[tokio::test]
    async fn initialization_finished_signals_immediately_when_no_streaming_jobs() {
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        // ready=true with version=0: no streaming jobs exist, mapping state is
        // genuinely empty and complete.
        observer_node.handle_initialization_notification(snapshot_notification(0, true));
        observer_node.handle_initialization_finished();

        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 0,
            }
        );
    }

    fn recovery_complete_notification(version: u64) -> SubscribeResponse {
        // Recovery-complete signal: mapping=None, version bump only.
        SubscribeResponse {
            status: None,
            operation: Operation::Update as i32,
            info: Some(Info::StreamingWorkerSlotMapping(
                FragmentWorkerSlotMapping {
                    fragment_id: 0.into(),
                    mapping: None,
                },
            )),
            version,
        }
    }

    #[tokio::test]
    async fn deferred_recovery_fires_on_recovery_complete_notification() {
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        // Subscribe during recovery: ready=false, version=0
        observer_node.handle_initialization_notification(snapshot_notification(0, false));
        observer_node.handle_initialization_finished();

        // Recovery is deferred — no signal yet.
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );

        // Meta sends recovery-complete notification (mapping=None, version bump).
        observer_node.handle_notification(recovery_complete_notification(1));

        // The deferred recovery should now fire.
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 1,
            }
        );
    }

    #[tokio::test]
    async fn mapping_none_notification_does_not_panic() {
        let (mut observer_node, _catalog_version_rx, mut version_rx, _reinit_rx) =
            make_observer_node();

        // ready=true so no deferred recovery
        observer_node.handle_initialization_notification(snapshot_notification(0, true));
        observer_node.handle_initialization_finished();

        // A mapping=None notification should not panic, just advance version.
        observer_node.handle_notification(recovery_complete_notification(3));
        assert_eq!(*version_rx.borrow_and_update(), 3);
    }

    #[tokio::test]
    async fn recovery_complete_buffered_before_initialization_finished() {
        // Simulates the race: recovery-complete notification is buffered during
        // init and replayed (via handle_notification) BEFORE handle_initialization_finished.
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        // Snapshot with ready=false, version=0 (recovery in progress).
        observer_node.handle_initialization_notification(snapshot_notification(0, false));

        // The recovery-complete notification was buffered and gets replayed now,
        // before handle_initialization_finished is called.
        observer_node.handle_notification(recovery_complete_notification(1));

        // No recovery signal yet (recovery_deferred is still false at this point).
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );

        // Now handle_initialization_finished runs — it should detect that the
        // recovery-complete signal was already replayed and signal immediately.
        observer_node.handle_initialization_finished();

        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 1,
            }
        );
    }

    #[tokio::test]
    async fn normal_mapping_update_during_recovery_does_not_signal() {
        // A normal mapping=Some update replayed before handle_initialization_finished
        // should NOT trigger recovery signal — only mapping=None does.
        let (mut observer_node, _catalog_version_rx, _version_rx, mut reinit_rx) =
            make_observer_node();

        // Snapshot with ready=false, version=0 (recovery in progress).
        observer_node.handle_initialization_notification(snapshot_notification(0, false));

        // A normal mapping update is replayed during init buffering.
        observer_node.handle_notification(streaming_mapping_notification(1.into(), 1));

        // handle_initialization_finished should NOT signal — the version advanced
        // but this was a regular mapping update, not the recovery-complete signal.
        observer_node.handle_initialization_finished();

        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery::default()
        );
        assert!(observer_node.recovery_deferred);

        // Only the actual recovery-complete notification should unblock.
        observer_node.handle_notification(recovery_complete_notification(2));
        assert_eq!(
            *reinit_rx.borrow_and_update(),
            CompletedObserverRecovery {
                epoch: 1,
                catalog_version: 0,
                streaming_worker_slot_mapping_version: 2,
            }
        );
    }
}
