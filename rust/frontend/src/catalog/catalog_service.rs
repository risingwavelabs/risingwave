use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
    TableSourceInfo,
};
use risingwave_pb::stream_plan::StreamNode;
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use super::catalog::Catalog;
use super::{DatabaseId, SchemaId};

pub type CatalogReadGuard = ArcRwLockReadGuard<RawRwLock, Catalog>;

/// [`CatalogReader`] can read catalog from local catalog and force the holder can not modify it.
#[derive(Clone)]
pub struct CatalogReader(pub Arc<RwLock<Catalog>>);
impl CatalogReader {
    pub fn read_guard(&self) -> CatalogReadGuard {
        self.0.read_arc()
    }
}

///  [`CatalogWriter`] is for DDL (create table/schema/database), it will only send rpc to meta and
/// get the catalog version as response. then it will wait the local catalog to update to sync with
/// the version.
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

    // TODO: it just change the catalog, just to unit test,will be deprecated soon
    pub async fn create_materialized_view_workaround(&self, table: ProstTable) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_materialized_view(
                table,
                StreamNode {
                    ..Default::default()
                },
            )
            .await?;
        self.wait_version(version).await
    }

    // TODO: it just change the catalog, just to unit test,will be deprecated soon
    pub async fn create_materialized_table_source_workaround(
        &self,
        table: ProstTable,
    ) -> Result<()> {
        let table_clone = table.clone();
        let table_source = ProstSource {
            id: 0,
            schema_id: table_clone.schema_id,
            database_id: table_clone.database_id,
            name: table_clone.name,
            info: Some(risingwave_pb::catalog::source::Info::TableSource(
                TableSourceInfo {
                    columns: table_clone.columns,
                },
            )),
        };
        let (_, _, version) = self
            .meta_client
            .create_materialized_source(
                table_source,
                table,
                StreamNode {
                    ..Default::default()
                },
            )
            .await?;
        self.wait_version(version).await
    }

    /// for the `CREATE TABLE statement`
    pub async fn create_materialized_table_source(&self, _table: ProstTable) -> Result<()> {
        todo!()
    }

    // TODO: maybe here to pass a materialize plan node
    pub async fn create_materialized_view(
        &self,
        _db_id: DatabaseId,
        _schema_id: SchemaId,
    ) -> Result<()> {
        todo!()
    }
}
