use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::catalog::CatalogVersion;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_rpc_client::{MetaClient, NotificationStream};
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;

use crate::catalog::catalog::Catalog;
use crate::scheduler::schedule::WorkerNodeManagerRef;

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend's data.
pub(crate) struct ObserverManager {
    rx: Box<dyn NotificationStream>,
    worker_node_manager: WorkerNodeManagerRef,
    catalog: Arc<RwLock<Catalog>>,
    catalog_updated_tx: Sender<CatalogVersion>,
}

impl ObserverManager {
    pub async fn new(
        client: MetaClient,
        addr: SocketAddr,
        worker_node_manager: WorkerNodeManagerRef,
        catalog: Arc<RwLock<Catalog>>,
        catalog_updated_tx: Sender<CatalogVersion>,
    ) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_node_manager,
            catalog,
            catalog_updated_tx,
        }
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// update frontend's data. `start` use `mut self` as parameter.
    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Ok(resp) = self.rx.next().await {
                    if resp.is_none() {
                        tracing::error!("Stream of notification terminated.");
                        break;
                    }
                    let resp = resp.unwrap();
                    let resp_clone = resp.clone();
                    let operation = resp.operation();
                    let mut catalog_guard = self.catalog.write();
                    match resp.info {
                        Some(Info::Database(_)) => {
                            panic!(
                                "recive a outdate version catalog notify from meta {:?}",
                                resp_clone
                            );
                        }
                        Some(Info::Schema(_)) => {
                            panic!(
                                "recive a outdate version catalog notify from meta {:?}",
                                resp_clone
                            );
                        }
                        Some(Info::Table(_)) => {
                            panic!(
                                "recive a outdate version catalog notify from meta {:?}",
                                resp_clone
                            );
                        }
                        Some(Info::Node(node)) => {
                            self.update_worker_node_manager(operation, node);
                        }
                        Some(Info::DatabaseV2(database)) => match operation {
                            Operation::Add => catalog_guard.create_database(database),
                            Operation::Delete => catalog_guard.drop_database(database.id),
                            _ => panic!("receive an unsupported notify {:?}", resp_clone),
                        },
                        Some(Info::SchemaV2(schema)) => match operation {
                            Operation::Add => catalog_guard.create_schema(schema),
                            Operation::Delete => {
                                catalog_guard.drop_schema(schema.database_id, schema.id)
                            }
                            _ => panic!("receive an unsupported notify {:?}", resp_clone),
                        },
                        Some(Info::TableV2(table)) => match operation {
                            Operation::Add => catalog_guard.create_table(&table),
                            Operation::Delete => catalog_guard.drop_table(
                                table.database_id,
                                table.schema_id,
                                table.id.into(),
                            ),
                            _ => panic!("receive an unsupported notify {:?}", resp_clone),
                        },
                        Some(Info::Source(source)) => match operation {
                            Operation::Add => catalog_guard.create_source(source),
                            Operation::Delete => catalog_guard.drop_source(
                                source.database_id,
                                source.schema_id,
                                source.id.into(),
                            ),
                            _ => panic!("receive an unsupported notify {:?}", resp_clone),
                        },
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
                            for node in snapshot.nodes {
                                self.worker_node_manager.add_worker_node(node)
                            }
                        }
                        None => panic!("receive an unsupported notify {:?}", resp),
                    }
                    assert!(resp.version > catalog_guard.version());
                    catalog_guard.set_version(resp.version);
                }
            }
        })
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
