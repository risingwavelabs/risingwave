use std::net::SocketAddr;
use std::sync::Arc;

use log::{error, info};
use parking_lot::Mutex;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{Database, Schema, SubscribeResponse, Table};
use risingwave_rpc_client::MetaClient;
use tokio::task::JoinHandle;
use tonic::Streaming;

use crate::catalog::catalog_service::CatalogCache;
use crate::catalog::{DatabaseId, SchemaId};
use crate::scheduler::schedule::WorkerNodeManagerRef;

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend's data.
pub(crate) struct ObserverManager {
    rx: Streaming<SubscribeResponse>,
    worker_node_manager: Option<WorkerNodeManagerRef>, /* Option<> will be removed when `WorkerNodeManager` is used in code. */
    catalog_cache: Arc<Mutex<CatalogCache>>,
}

impl ObserverManager {
    pub async fn new(
        client: MetaClient,
        addr: SocketAddr,
        catalog_cache: Arc<Mutex<CatalogCache>>,
    ) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_node_manager: None,
            catalog_cache,
        }
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// update frontend's data. `start` use `mut self` as parameter.
    pub fn start(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Ok(resp) = self.rx.message().await {
                    if resp.is_none() {
                        error!("Stream of notification terminated.");
                        break;
                    }
                    let resp = resp.unwrap();
                    let operation = resp.operation();

                    match resp.info {
                        Some(Info::Database(database)) => {
                            self.update_database(operation, database);
                        }
                        Some(Info::Node(node)) => {
                            self.update_worker_node_manager(operation, node);
                        }
                        Some(Info::Schema(schema)) => {
                            self.update_schema(operation, schema);
                        }
                        Some(Info::Table(table)) => {
                            self.update_table(operation, table);
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
        let manager = self
            .worker_node_manager
            .as_ref()
            .expect("forget to call `set_worker_node_manager` before call start");

        match operation {
            Operation::Add => manager.add_worker_node(node),
            Operation::Delete => manager.remove_worker_node(node),
            _ => (),
        }
    }

    /// `update_database` is called in `start` method.
    /// It calls `create_database` and `drop_database` of `CatalogCache`.
    fn update_database(&self, operation: Operation, database: Database) {
        info!(
            "Update database, operation: {:?}, database: {:?}",
            operation, database
        );
        let db_name = database.database_name.as_str();
        let db_id = database.database_ref_id.unwrap().database_id as u64;

        match operation {
            Operation::Add => self
                .catalog_cache
                .lock()
                .create_database(db_name, db_id)
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            Operation::Delete => self
                .catalog_cache
                .lock()
                .drop_database(db_name)
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            _ => (),
        }
    }

    /// `update_schema` is called in `start` method.
    /// It calls `create_schema` and `drop_schema` of `CatalogCache`.
    fn update_schema(&self, operation: Operation, schema: Schema) {
        info!(
            "Update schema, operation: {:?}, schema: {:?}",
            operation, schema
        );
        let schema_ref_id = schema.schema_ref_id.unwrap();
        let db_id = schema_ref_id.database_ref_id.as_ref().unwrap().database_id as DatabaseId;
        let db_name = self.catalog_cache.lock().get_database_name(db_id);
        if db_name.is_none() {
            error!("Not found database id: {:?}", db_id);
            return;
        }
        let db_name = db_name.unwrap();

        match operation {
            Operation::Add => self
                .catalog_cache
                .lock()
                .create_schema(
                    &db_name,
                    &schema.schema_name,
                    schema_ref_id.schema_id as SchemaId,
                )
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            Operation::Delete => self
                .catalog_cache
                .lock()
                .drop_schema(&db_name, &schema.schema_name)
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            _ => (),
        }
    }

    /// `update_table` is called in `start` method.
    /// It calls `create_table` and `drop_table` of `CatalogCache`.
    fn update_table(&self, operation: Operation, table: Table) {
        info!(
            "Update table, operation: {:?}, table: {:?}",
            operation, table
        );
        let schema_ref_id = table
            .table_ref_id
            .as_ref()
            .unwrap()
            .schema_ref_id
            .as_ref()
            .unwrap();
        let db_id = schema_ref_id.database_ref_id.as_ref().unwrap().database_id as DatabaseId;
        let schema_id = schema_ref_id.schema_id as SchemaId;
        let db_name = self.catalog_cache.lock().get_database_name(db_id);
        if db_name.is_none() {
            error!("Not found database id: {:?}", db_id);
            return;
        }
        let db_name = db_name.unwrap();

        let schema_name = self
            .catalog_cache
            .lock()
            .get_database(&db_name)
            .unwrap()
            .get_schema_name(schema_id);
        if schema_name.is_none() {
            error!("Not found schema id: {:?}", schema_id);
            return;
        }
        let schema_name = schema_name.unwrap();

        match operation {
            Operation::Add => self
                .catalog_cache
                .lock()
                .create_table(&db_name, &schema_name, &table)
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            Operation::Delete => self
                .catalog_cache
                .lock()
                .drop_table(&db_name, &schema_name, &table.table_name)
                .unwrap_or_else(|e| error!("{}", e.to_string())),
            _ => (),
        }
    }
}
