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

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::{CatalogVersion, IndexId, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{
    Database as ProstDatabase, Index as ProstIndex, Schema as ProstSchema, Sink as ProstSink,
    Source as ProstSource, Table as ProstTable,
};
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use super::root_catalog::Catalog;
use super::DatabaseId;
use crate::user::UserId;

pub type CatalogReadGuard = ArcRwLockReadGuard<RawRwLock, Catalog>;

/// [`CatalogReader`] can read catalog from local catalog and force the holder can not modify it.
#[derive(Clone)]
pub struct CatalogReader(Arc<RwLock<Catalog>>);
impl CatalogReader {
    pub fn new(inner: Arc<RwLock<Catalog>>) -> Self {
        CatalogReader(inner)
    }

    pub fn read_guard(&self) -> CatalogReadGuard {
        self.0.read_arc()
    }
}

///  [`CatalogWriter`] is for DDL (create table/schema/database), it will only send rpc to meta and
/// get the catalog version as response. then it will wait the local catalog to update to sync with
/// the version.
#[async_trait::async_trait]
pub trait CatalogWriter: Send + Sync {
    async fn create_database(&self, db_name: &str, owner: UserId) -> Result<()>;

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()>;

    async fn create_materialized_view(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_materialized_source(
        &self,
        source: ProstSource,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_index(
        &self,
        index: ProstIndex,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_source(&self, source: ProstSource) -> Result<()>;

    async fn create_sink(&self, sink: ProstSink, graph: StreamFragmentGraph) -> Result<()>;

    async fn drop_materialized_source(
        &self,
        source_id: u32,
        table_id: TableId,
        indexes_id: Vec<IndexId>,
    ) -> Result<()>;

    async fn drop_materialized_view(
        &self,
        table_id: TableId,
        indexes_id: Vec<IndexId>,
    ) -> Result<()>;

    async fn drop_source(&self, source_id: u32) -> Result<()>;

    async fn drop_sink(&self, sink_id: u32) -> Result<()>;

    async fn drop_database(&self, database_id: u32) -> Result<()>;

    async fn drop_schema(&self, schema_id: u32) -> Result<()>;

    async fn drop_index(&self, index_id: IndexId) -> Result<()>;
}

#[derive(Clone)]
pub struct CatalogWriterImpl {
    meta_client: MetaClient,
    catalog_updated_rx: Receiver<CatalogVersion>,
}

#[async_trait::async_trait]
impl CatalogWriter for CatalogWriterImpl {
    async fn create_database(&self, db_name: &str, owner: UserId) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_database(ProstDatabase {
                name: db_name.to_string(),
                id: 0,
                owner,
            })
            .await?;
        self.wait_version(version).await
    }

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_schema(ProstSchema {
                id: 0,
                name: schema_name.to_string(),
                database_id: db_id,
                owner,
            })
            .await?;
        self.wait_version(version).await
    }

    // TODO: maybe here to pass a materialize plan node
    async fn create_materialized_view(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_materialized_view(table, graph)
            .await?;
        self.wait_version(version).await
    }

    async fn create_index(
        &self,
        index: ProstIndex,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        let (_, version) = self.meta_client.create_index(index, table, graph).await?;
        self.wait_version(version).await
    }

    async fn create_materialized_source(
        &self,
        source: ProstSource,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        let (_, _, version) = self
            .meta_client
            .create_materialized_source(source, table, graph)
            .await?;
        self.wait_version(version).await
    }

    async fn create_source(&self, source: ProstSource) -> Result<()> {
        let (_id, version) = self.meta_client.create_source(source).await?;
        self.wait_version(version).await
    }

    async fn create_sink(&self, sink: ProstSink, graph: StreamFragmentGraph) -> Result<()> {
        let (_id, version) = self.meta_client.create_sink(sink, graph).await?;
        self.wait_version(version).await
    }

    async fn drop_materialized_source(
        &self,
        source_id: u32,
        table_id: TableId,
        indexes_id: Vec<IndexId>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_materialized_source(source_id, table_id, indexes_id)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_materialized_view(
        &self,
        table_id: TableId,
        indexes_id: Vec<IndexId>,
    ) -> Result<()> {
        let version = self
            .meta_client
            .drop_materialized_view(table_id, indexes_id)
            .await?;
        self.wait_version(version).await
    }

    async fn drop_source(&self, source_id: u32) -> Result<()> {
        let version = self.meta_client.drop_source(source_id).await?;
        self.wait_version(version).await
    }

    async fn drop_sink(&self, sink_id: u32) -> Result<()> {
        let version = self.meta_client.drop_sink(sink_id).await?;
        self.wait_version(version).await
    }

    async fn drop_index(&self, index_id: IndexId) -> Result<()> {
        let version = self.meta_client.drop_index(index_id).await?;
        self.wait_version(version).await
    }

    async fn drop_schema(&self, schema_id: u32) -> Result<()> {
        let version = self.meta_client.drop_schema(schema_id).await?;
        self.wait_version(version).await
    }

    async fn drop_database(&self, database_id: u32) -> Result<()> {
        let version = self.meta_client.drop_database(database_id).await?;
        self.wait_version(version).await
    }
}

impl CatalogWriterImpl {
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
}
