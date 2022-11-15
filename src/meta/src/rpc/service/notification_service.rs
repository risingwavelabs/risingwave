// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use itertools::Itertools;
use risingwave_pb::catalog::Table;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::{ParallelUnitMapping, WorkerNode, WorkerType};
use risingwave_pb::hummock::HummockVersion;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeRequest, SubscribeResponse, SubscribeType};
use risingwave_pb::user::UserInfo;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::hummock::HummockManagerRef;
use crate::manager::{
    Catalog, CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, MetaSrvEnv, Notification,
    NotificationVersion, WorkerKey,
};
use crate::storage::MetaStore;

pub struct NotificationServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    hummock_manager: HummockManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> NotificationServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            cluster_manager,
            hummock_manager,
            fragment_manager,
        }
    }

    async fn get_catalog_snapshot(&self) -> (Catalog, Vec<UserInfo>, NotificationVersion) {
        let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
        let (databases, schemas, tables, sources, sinks, indexes, views) =
            catalog_guard.database.get_catalog();
        let users = catalog_guard.user.list_users();
        let notification_version = self.env.notification_manager().current_version().await;
        (
            (databases, schemas, tables, sources, sinks, indexes, views),
            users,
            notification_version,
        )
    }

    async fn get_parallel_unit_mapping_snapshot(
        &self,
    ) -> (Vec<ParallelUnitMapping>, NotificationVersion) {
        let fragment_guard = self.fragment_manager.get_fragment_read_guard().await;
        let parallel_unit_mappings = fragment_guard.all_fragment_mappings().collect_vec();
        let notification_version = self.env.notification_manager().current_version().await;
        (parallel_unit_mappings, notification_version)
    }

    async fn get_worker_node_snapshot(&self) -> (Vec<WorkerNode>, NotificationVersion) {
        let cluster_guard = self.cluster_manager.get_cluster_core_guard().await;
        let nodes = cluster_guard.list_worker_node(WorkerType::ComputeNode, Some(Running));
        let notification_version = self.env.notification_manager().current_version().await;
        (nodes, notification_version)
    }

    async fn get_hummock_version_snapshot(&self) -> (HummockVersion, NotificationVersion) {
        let hummock_manager_guard = self.hummock_manager.get_read_guard().await;
        let hummock_version = hummock_manager_guard.current_version.clone();
        let notification_version = self.env.notification_manager().current_version().await;
        (hummock_version, notification_version)
    }

    async fn get_tables_and_creating_tables_snapshot(&self) -> (Vec<Table>, NotificationVersion) {
        let catalog_guard = self.catalog_manager.get_catalog_core_guard().await;
        let mut tables = catalog_guard.database.list_tables();
        tables.extend(catalog_guard.database.list_creating_tables());
        let notification_version = self.env.notification_manager().current_version().await;
        (tables, notification_version)
    }

    async fn compactor_subscribe(&self, tx: UnboundedSender<Notification>) {
        let (tables, catalog_version) = self.get_tables_and_creating_tables_snapshot().await;

        let notification_core = self.env.notification_manager().core_guard().await;
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(MetaSnapshot {
                tables,
                version: Some(SnapshotVersion {
                    catalog_version,
                    ..Default::default()
                }),
                ..Default::default()
            })),
            version: notification_core.current_version(),
        }))
        .unwrap()
    }

    async fn frontend_subscribe(&self, tx: UnboundedSender<Notification>) {
        let ((databases, schemas, tables, sources, sinks, indexes, views), users, catalog_version) =
            self.get_catalog_snapshot().await;
        let (parallel_unit_mappings, parallel_unit_mapping_version) =
            self.get_parallel_unit_mapping_snapshot().await;
        let (nodes, worker_node_version) = self.get_worker_node_snapshot().await;

        let hummock_snapshot = Some(self.hummock_manager.get_last_epoch().unwrap());

        let notification_core = self.env.notification_manager().core_guard().await;
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(MetaSnapshot {
                databases,
                schemas,
                sources,
                sinks,
                tables,
                indexes,
                views,
                users,
                parallel_unit_mappings,
                nodes,
                hummock_snapshot,
                version: Some(SnapshotVersion {
                    catalog_version,
                    parallel_unit_mapping_version,
                    worker_node_version,
                    ..Default::default()
                }),
                ..Default::default()
            })),
            version: notification_core.current_version(),
        }))
        .unwrap();
    }

    async fn hummock_subscribe(&self, tx: UnboundedSender<Notification>) {
        let (tables, catalog_version) = self.get_tables_and_creating_tables_snapshot().await;
        let (hummock_version, hummock_version_version) = self.get_hummock_version_snapshot().await;

        let notification_core = self.env.notification_manager().core_guard().await;
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(MetaSnapshot {
                tables,
                hummock_version: Some(hummock_version),
                version: Some(SnapshotVersion {
                    catalog_version,
                    hummock_version_version,
                    ..Default::default()
                }),
                ..Default::default()
            })),
            version: notification_core.current_version(),
        }))
        .unwrap()
    }
}

#[async_trait::async_trait]
impl<S> NotificationService for NotificationServiceImpl<S>
where
    S: MetaStore,
{
    type SubscribeStream = UnboundedReceiverStream<Notification>;

    #[cfg_attr(coverage, no_coverage)]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let host_address = req.get_host()?.clone();
        let subscribe_type = req.get_subscribe_type()?;

        let (tx, rx) = mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_sender(subscribe_type, WorkerKey(host_address), tx.clone())
            .await;

        match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe(tx).await,
            SubscribeType::Frontend => {
                self.hummock_manager
                    .pin_snapshot(req.get_worker_id())
                    .await?;
                self.frontend_subscribe(tx).await;
            }
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe(tx).await;
            }
            SubscribeType::Unspecified => unreachable!(),
        };

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
