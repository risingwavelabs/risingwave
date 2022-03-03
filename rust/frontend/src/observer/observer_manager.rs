use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use log::{error, info};
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{Database, Schema, Table};
use risingwave_rpc_client::{MetaClient, NotificationStream};
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;

use crate::catalog::catalog_service::CatalogCache;
use crate::catalog::{CatalogError, DatabaseId, SchemaId};
use crate::scheduler::schedule::WorkerNodeManagerRef;

pub const UPDATE_FINISH_NOTIFICATION: i32 = 0;

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend's data.
pub(crate) struct ObserverManager {
    rx: Box<dyn NotificationStream>,
    worker_node_manager: WorkerNodeManagerRef,
    catalog_cache: Arc<RwLock<CatalogCache>>,
    catalog_updated_tx: Sender<i32>,
}

impl ObserverManager {
    pub async fn new(
        client: MetaClient,
        addr: SocketAddr,
        worker_node_manager: WorkerNodeManagerRef,
        catalog_cache: Arc<RwLock<CatalogCache>>,
        catalog_updated_tx: Sender<i32>,
    ) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_node_manager,
            catalog_cache,
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
                        error!("Stream of notification terminated.");
                        break;
                    }
                    let resp = resp.unwrap();
                    let operation = resp.operation();

                    match resp.info {
                        Some(Info::Database(database)) => {
                            self.update_database(operation, database)
                                .unwrap_or_else(|e| error!("{}", e.to_string()));
                        }
                        Some(Info::Node(node)) => {
                            self.update_worker_node_manager(operation, node);
                        }
                        Some(Info::Schema(schema)) => {
                            self.update_schema(operation, schema)
                                .unwrap_or_else(|e| error!("{}", e.to_string()));
                        }
                        Some(Info::Table(table)) => {
                            self.update_table(operation, table)
                                .unwrap_or_else(|e| error!("{}", e.to_string()));
                        }
                        None => (),
                    }
                }
            }
        })
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        info!(
            "Update worker nodes, operation: {:?}, node: {:?}",
            operation, node
        );

        match operation {
            Operation::Add => self.worker_node_manager.add_worker_node(node),
            Operation::Delete => self.worker_node_manager.remove_worker_node(node),
            _ => (),
        }
    }

    /// `update_database` is called in `start` method.
    /// It calls `create_database` and `drop_database` of `CatalogCache`.
    fn update_database(&self, operation: Operation, database: Database) -> Result<()> {
        info!(
            "Update database, operation: {:?}, database: {:?}",
            operation, database
        );
        let db_name = database.get_database_name();
        let db_id = database.get_database_ref_id()?.database_id as u64;

        match operation {
            Operation::Add => self
                .catalog_cache
                .write()
                .unwrap()
                .create_database(db_name, db_id)?,
            Operation::Delete => self.catalog_cache.write().unwrap().drop_database(db_name)?,
            _ => {
                return Err(RwError::from(InternalError(
                    "Unsupported operation.".to_string(),
                )))
            }
        }

        self.catalog_updated_tx
            .send(UPDATE_FINISH_NOTIFICATION)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }

    /// `update_schema` is called in `start` method.
    /// It calls `create_schema` and `drop_schema` of `CatalogCache`.
    fn update_schema(&self, operation: Operation, schema: Schema) -> Result<()> {
        info!(
            "Update schema, operation: {:?}, schema: {:?}",
            operation, schema
        );
        let schema_ref_id = schema.get_schema_ref_id()?;
        let db_id = schema_ref_id.get_database_ref_id()?.database_id as DatabaseId;
        let db_name = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database_name(db_id)
            .ok_or_else(|| CatalogError::NotFound("database id", db_id.to_string()))?;
        let schema_name = schema.get_schema_name();
        let schema_id = schema_ref_id.schema_id as SchemaId;

        match operation {
            Operation::Add => self.catalog_cache.write().unwrap().create_schema(
                &db_name,
                schema_name,
                schema_id,
            )?,
            Operation::Delete => self
                .catalog_cache
                .write()
                .unwrap()
                .drop_schema(&db_name, schema_name)?,
            _ => {
                return Err(RwError::from(InternalError(
                    "Unsupported operation.".to_string(),
                )))
            }
        }

        self.catalog_updated_tx
            .send(UPDATE_FINISH_NOTIFICATION)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }

    /// `update_table` is called in `start` method.
    /// It calls `create_table` and `drop_table` of `CatalogCache`.
    fn update_table(&self, operation: Operation, table: Table) -> Result<()> {
        info!(
            "Update table, operation: {:?}, table: {:?}",
            operation, table
        );
        let schema_ref_id = table.get_table_ref_id()?.get_schema_ref_id()?;
        let db_id = schema_ref_id.get_database_ref_id()?.database_id as DatabaseId;
        let db_name = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database_name(db_id)
            .ok_or_else(|| CatalogError::NotFound("database id", db_id.to_string()))?;
        let schema_id = schema_ref_id.schema_id as SchemaId;
        let schema_name = self
            .catalog_cache
            .read()
            .unwrap()
            .get_database(&db_name)
            .unwrap()
            .get_schema_name(schema_id)
            .ok_or_else(|| CatalogError::NotFound("schema id", schema_id.to_string()))?;

        match operation {
            Operation::Add => {
                self.catalog_cache
                    .write()
                    .unwrap()
                    .create_table(&db_name, &schema_name, &table)?
            }
            Operation::Delete => self.catalog_cache.write().unwrap().drop_table(
                &db_name,
                &schema_name,
                &table.table_name,
            )?,
            _ => {
                return Err(RwError::from(InternalError(
                    "Unsupported operation.".to_string(),
                )))
            }
        }

        self.catalog_updated_tx
            .send(UPDATE_FINISH_NOTIFICATION)
            .map_err(|e| RwError::from(InternalError(e.to_string())))
    }
}
