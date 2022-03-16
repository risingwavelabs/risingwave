use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
};
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use super::{DatabaseId, SchemaId};
use crate::catalog::table_catalog::{TableCatalog, ROWID_NAME};

/// For DDL (create table/schema/database), only send rpc to meta. Create and delete actions will be
/// done by `ObserverManager`. Should be used by DDL handler.
#[derive(Clone)]
pub struct CatalogWriter {
    meta_client: MetaClient,
    catalog_updated_rx: Receiver<CatalogVersion>,
}

impl CatalogWriter {
    pub fn new(meta_client: MetaClient, catalog_updated_rx: Receiver<CatalogVersion>) -> Self {
        Self {
            meta_client,
            catalog_updated_rx,
        }
    }

    async fn wait_version(&self, version: CatalogVersion) -> Result<()> {
        let mut rx = self.catalog_updated_rx.clone();
        while *rx.borrow_and_update() < version {
            rx.changed()
                .await
                .map_err(|e| RwError::from(InternalError(e.to_string())))?;
        }
        Ok(())
    }

    pub async fn create_database(&self, db_name: &str) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_database(ProstDatabase {
                name: db_name.to_string(),
                id: 0,
            })
            .await?;
        self.wait_version(version).await
    }

    pub async fn create_schema(&self, db_id: DatabaseId, schema_name: &str) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_schema(ProstSchema {
                id: 0,
                name: schema_name.to_string(),
                database_id: db_id,
            })
            .await?;
        self.wait_version(version).await
    }

    /// for the `CREATE TABLE statement`
    pub async fn create_materialized_table_source(&self, table: ProstTable) -> Result<()> {
        todo!()
    }

    // TODO: maybe here to pass a materialize plan node
    pub async fn create_materialized_view(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
    ) -> Result<()> {
        todo!()
    }
}
