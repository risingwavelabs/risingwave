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

use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use risingwave_rpc_client::{MetaClient, NotificationStream};
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;

use crate::catalog::root_catalog::Catalog;
use crate::scheduler::schedule::WorkerNodeManagerRef;

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend data.
pub(crate) struct ObserverManager {
    rx: Box<dyn NotificationStream>,
    meta_client: MetaClient,
    addr: HostAddr,
    worker_node_manager: WorkerNodeManagerRef,
    catalog: Arc<RwLock<Catalog>>,
    catalog_updated_tx: Sender<CatalogVersion>,
}

const RE_SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

impl ObserverManager {
    pub async fn new(
        meta_client: MetaClient,
        addr: HostAddr,
        worker_node_manager: WorkerNodeManagerRef,
        catalog: Arc<RwLock<Catalog>>,
        catalog_updated_tx: Sender<CatalogVersion>,
    ) -> Self {
        let rx = meta_client
            .subscribe(&addr, WorkerType::Frontend)
            .await
            .unwrap();
        Self {
            rx,
            meta_client,
            addr,
            worker_node_manager,
            catalog,
            catalog_updated_tx,
        }
    }

    pub fn handle_snapshot_notification(&mut self, resp: SubscribeResponse) -> Result<()> {
        let mut catalog_guard = self.catalog.write();
        catalog_guard.clear();
        match resp.info {
            Some(Info::FeSnapshot(snapshot)) => {
                for db in snapshot.database {
                    catalog_guard.create_database(db)
                }
                for schema in snapshot.schema {
                    catalog_guard.create_schema(schema)
                }
                for table in snapshot.table {
                    catalog_guard.create_table(&table)
                }
                for source in snapshot.source {
                    catalog_guard.create_source(source)
                }
                self.worker_node_manager.refresh_worker_node(snapshot.nodes);
            }
            _ => {
                return Err(ErrorCode::InternalError(format!(
                    "the first notify should be frontend snapshot, but get {:?}",
                    resp
                ))
                .into())
            }
        }
        catalog_guard.set_version(resp.version);
        self.catalog_updated_tx.send(resp.version).unwrap();
        Ok(())
    }

    pub fn handle_notification(&mut self, resp: SubscribeResponse) {
        let mut catalog_guard = self.catalog.write();
        match &resp.info {
            Some(Info::Database(_)) => {
                panic!(
                    "received a deprecated catalog notification from meta {:?}",
                    resp
                );
            }
            Some(Info::Schema(_)) => {
                panic!(
                    "received a deprecated catalog notification from meta {:?}",
                    resp
                );
            }
            Some(Info::Table(_)) => {
                panic!(
                    "received a deprecated catalog notification from meta {:?}",
                    resp
                );
            }
            Some(Info::Node(node)) => {
                self.update_worker_node_manager(resp.operation(), node.clone());
            }
            Some(Info::DatabaseV2(database)) => match resp.operation() {
                Operation::Add => catalog_guard.create_database(database.clone()),
                Operation::Delete => catalog_guard.drop_database(database.id),
                _ => panic!("receive an unsupported notify {:?}", resp.clone()),
            },
            Some(Info::SchemaV2(schema)) => match resp.operation() {
                Operation::Add => catalog_guard.create_schema(schema.clone()),
                Operation::Delete => catalog_guard.drop_schema(schema.database_id, schema.id),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Some(Info::TableV2(table)) => match resp.operation() {
                Operation::Add => catalog_guard.create_table(table),
                Operation::Delete => {
                    catalog_guard.drop_table(table.database_id, table.schema_id, table.id.into())
                }
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Some(Info::Source(source)) => match resp.operation() {
                Operation::Add => catalog_guard.create_source(source.clone()),
                Operation::Delete => {
                    catalog_guard.drop_source(source.database_id, source.schema_id, source.id)
                }
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Some(Info::FeSnapshot(_)) => {
                panic!(
                    "receiving an FeSnapshot in the middle is unsupported now {:?}",
                    resp
                )
            }
            _ => panic!("receive an unsupported notify {:?}", resp),
        }
        assert!(
            resp.version > catalog_guard.version(),
            "resp version={:?}, current version={:?}",
            resp.version,
            catalog_guard.version()
        );
        catalog_guard.set_version(resp.version);
        self.catalog_updated_tx.send(resp.version).unwrap();
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// update frontend data.
    pub async fn start(mut self) -> Result<JoinHandle<()>> {
        let first_resp = self.rx.next().await?.ok_or_else(|| {
            ErrorCode::InternalError(
                "ObserverManager start failed, Stream of notification terminated at the start."
                    .to_string(),
            )
        })?;
        self.handle_snapshot_notification(first_resp)?;
        let handle = tokio::spawn(async move {
            loop {
                if let Ok(resp) = self.rx.next().await {
                    if resp.is_none() {
                        tracing::error!("Stream of notification terminated.");
                        self.re_subscribe().await;
                        continue;
                    }
                    self.handle_notification(resp.unwrap());
                }
            }
        });
        Ok(handle)
    }

    /// `re_subscribe` is used to re-subscribe to the meta's notification.
    async fn re_subscribe(&mut self) {
        loop {
            match self
                .meta_client
                .subscribe(&self.addr, WorkerType::Frontend)
                .await
            {
                Ok(rx) => {
                    tracing::debug!("re-subscribe success");
                    self.rx = rx;
                    if let Ok(Some(snapshot_resp)) = self.rx.next().await {
                        self.handle_snapshot_notification(snapshot_resp)
                            .expect("handle snapshot notification failed after re-subscribe");
                        break;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(RE_SUBSCRIBE_RETRY_INTERVAL).await;
                }
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
