use std::net::SocketAddr;
use std::sync::Arc;

use log::{debug, error};
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
/// Use `set_***` method to init ObserverManager and then
/// call `start` to spawn a new asynchronous task
/// which receives meta's notification and update frontend's data.
pub(crate) struct ObserverManager {
    rx: Streaming<SubscribeResponse>,
    worker_node_manager: Option<WorkerNodeManagerRef>,
    catalog_cache: Option<Arc<Mutex<CatalogCache>>>,
}

impl ObserverManager {
    pub async fn new(client: MetaClient, addr: SocketAddr) -> Self {
        let rx = client.subscribe(addr, WorkerType::Frontend).await.unwrap();
        Self {
            rx,
            worker_node_manager: None,
            catalog_cache: None,
        }
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// update frontend's data. `start` use `mut self` as parameter, it supposes that the code
    /// for initialization is sequential and `start` should be called after
    /// `set_worker_node_manager`.
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

    /// `set_worker_node_manager` is used to get a quote of `WorkerNodeManager`.
    pub fn set_worker_node_manager(&mut self, worker_node_manager: WorkerNodeManagerRef) {
        self.worker_node_manager = Some(worker_node_manager);
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        debug!(
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

    /// `set_catalog_cache` is used to get a quote of `CatalogCache`.
    pub fn set_catalog_cache(&mut self, catalog_cache: Arc<Mutex<CatalogCache>>) {
        self.catalog_cache = Some(catalog_cache);
    }

    /// `update_database` is called in `start` method.
    /// It calls `create_database` and `drop_database` of `CatalogCache`.
    fn update_database(&self, operation: Operation, database: Database) {
        debug!(
            "Update database, operation: {:?}, database: {:?}",
            operation, database
        );
        let mut catalog_cache_guard = self
            .catalog_cache
            .as_ref()
            .expect("forget to call `set_catalog_cache` before call start")
            .lock();

        let db_name = database.database_name.as_str();
        let db_id = database.database_ref_id.unwrap().database_id as u64;

        match operation {
            Operation::Add => catalog_cache_guard.create_database(db_name, db_id).unwrap(),
            Operation::Delete => catalog_cache_guard.drop_database(db_name).unwrap(),
            _ => (),
        }
    }

    /// `update_schema` is called in `start` method.
    /// It calls `create_schema` and `drop_schema` of `CatalogCache`.
    fn update_schema(&self, operation: Operation, schema: Schema) {
        debug!(
            "Update schema, operation: {:?}, schema: {:?}",
            operation, schema
        );
        let mut catalog_cache_guard = self
            .catalog_cache
            .as_ref()
            .expect("forget to call `set_catalog_cache` before call start")
            .lock();

        let schema_ref_id = schema.schema_ref_id.unwrap();
        let db_id = schema_ref_id.database_ref_id.as_ref().unwrap().database_id as DatabaseId;
        let db_name = catalog_cache_guard.get_database_name(db_id);
        if db_name.is_none() {
            error!("Not found database id: {:?}", db_id);
            return;
        }
        let db_name = db_name.unwrap();

        match operation {
            Operation::Add => catalog_cache_guard
                .create_schema(
                    &db_name,
                    &schema.schema_name,
                    schema_ref_id.schema_id as SchemaId,
                )
                .unwrap(),
            Operation::Delete => catalog_cache_guard
                .drop_schema(&db_name, &schema.schema_name)
                .unwrap(),
            _ => (),
        }
    }

    /// `update_table` is called in `start` method.
    /// It calls `create_table` and `drop_table` of `CatalogCache`.
    fn update_table(&self, operation: Operation, table: Table) {
        debug!(
            "Update table, operation: {:?}, table: {:?}",
            operation, table
        );
        let mut catalog_cache_guard = self
            .catalog_cache
            .as_ref()
            .expect("forget to call `set_catalog_cache` before call start")
            .lock();

        let schema_ref_id = table
            .table_ref_id
            .as_ref()
            .unwrap()
            .schema_ref_id
            .as_ref()
            .unwrap();
        let db_id = schema_ref_id.database_ref_id.as_ref().unwrap().database_id as DatabaseId;
        let schema_id = schema_ref_id.schema_id as SchemaId;
        let db_name = catalog_cache_guard.get_database_name(db_id);
        if db_name.is_none() {
            error!("Not found database id: {:?}", db_id);
            return;
        }
        let db_name = db_name.unwrap();

        let database_catalog = catalog_cache_guard.get_database(&db_name).unwrap();
        let schema_name = database_catalog.get_schema_name(schema_id);
        if schema_name.is_none() {
            error!("Not found schema id: {:?}", schema_id);
            return;
        }
        let schema_name = schema_name.unwrap();

        match operation {
            Operation::Add => catalog_cache_guard
                .create_table(&db_name, &schema_name, &table)
                .unwrap(),
            Operation::Delete => catalog_cache_guard
                .drop_table(&db_name, &schema_name, &table.table_name)
                .unwrap(),
            _ => (),
        }
    }
}
