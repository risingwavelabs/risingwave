use std::net::SocketAddr;
use std::time::Duration;

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::meta::catalog_service_client::CatalogServiceClient;
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::{
    AddWorkerNodeRequest, CreateRequest, Database, DropRequest, HeartbeatRequest, Schema, Table,
};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tonic::transport::{Channel, Endpoint};

type DatabaseId = i32;
type SchemaId = i32;
type TableId = i32;

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Clone)]
pub struct MetaClient {
    pub cluster_client: ClusterServiceClient<Channel>,
    pub heartbeat_client: HeartbeatServiceClient<Channel>,
    pub catalog_client: CatalogServiceClient<Channel>,
    pub hummock_client: HummockManagerServiceClient<Channel>,
}

impl MetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(addr: &str) -> Result<Self> {
        let channel = Endpoint::from_shared(addr.to_string())
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(format!("failed to connect to {}", addr))?;
        let cluster_client = ClusterServiceClient::new(channel.clone());
        let heartbeat_client = HeartbeatServiceClient::new(channel.clone());
        let catalog_client = CatalogServiceClient::new(channel.clone());
        let hummock_client = HummockManagerServiceClient::new(channel);
        Ok(Self {
            cluster_client,
            heartbeat_client,
            catalog_client,
            hummock_client,
        })
    }

    /// Register the current node to the cluster and return the corresponding worker id.
    pub async fn register(&self, addr: SocketAddr) -> Result<u32> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = AddWorkerNodeRequest {
            worker_type: WorkerType::ComputeNode as i32,
            host: Some(host_address),
        };
        let resp = self
            .cluster_client
            .to_owned()
            .add_worker_node(request)
            .await
            .to_rw_result()?
            .into_inner();
        let worker_node =
            try_match_expand!(resp.node, Some, "AddWorkerNodeResponse::node is empty")?;
        Ok(worker_node.id)
    }

    /// Send heartbeat signal to meta service.
    pub async fn send_heartbeat(&self, node_id: u32) -> Result<()> {
        let request = HeartbeatRequest {
            node_id,
            worker_type: WorkerType::ComputeNode as i32,
        };
        let _resp = self
            .heartbeat_client
            .to_owned()
            .heartbeat(request)
            .await
            .to_rw_result()?;
        Ok(())
    }

    pub async fn create_table(&self, table: Table) -> Result<TableId> {
        self.create_catalog_body(CatalogBody::Table(table)).await
    }

    pub async fn create_database(&self, db: Database) -> Result<DatabaseId> {
        self.create_catalog_body(CatalogBody::Database(db)).await
    }

    pub async fn create_schema(&self, schema: Schema) -> Result<SchemaId> {
        self.create_catalog_body(CatalogBody::Schema(schema)).await
    }

    async fn create_catalog_body(&self, catalog_body: CatalogBody) -> Result<i32> {
        let request = CreateRequest {
            catalog_body: Some(catalog_body),
            ..Default::default()
        };
        let resp = self
            .catalog_client
            .to_owned()
            .create(request)
            .await
            .to_rw_result()?
            .into_inner();
        Ok(resp.id)
    }

    pub async fn drop_table(&self, table_ref_id: TableRefId) -> Result<()> {
        self.drop_catalog_id(CatalogId::TableId(table_ref_id)).await
    }

    pub async fn drop_schema(&self, schema_ref_id: SchemaRefId) -> Result<()> {
        self.drop_catalog_id(CatalogId::SchemaId(schema_ref_id))
            .await
    }

    pub async fn drop_database(&self, database_ref_id: DatabaseRefId) -> Result<()> {
        self.drop_catalog_id(CatalogId::DatabaseId(database_ref_id))
            .await
    }

    async fn drop_catalog_id(&self, catalog_id: CatalogId) -> Result<()> {
        let request = DropRequest {
            catalog_id: Some(catalog_id),
            ..Default::default()
        };
        let _resp = self
            .catalog_client
            .to_owned()
            .drop(request)
            .await
            .to_rw_result()?
            .into_inner();
        Ok(())
    }
}
