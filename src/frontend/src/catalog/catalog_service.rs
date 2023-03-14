// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::catalog::{CatalogVersion, FunctionId, IndexId, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Function as ProstFunction, Index as ProstIndex,
    Schema as ProstSchema, Sink as ProstSink, Source as ProstSource, Table as ProstTable,
    View as ProstView,
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
        // Make this recursive so that one can get this guard in the same thread without fear.
        self.0.read_arc_recursive()
    }
}

/// [`CatalogWriter`] initiate DDL operations (create table/schema/database/function).
/// It will only send rpc to meta and get the catalog version as response.
/// Then it will wait for the local catalog to be synced to the version, which is performed by
/// [observer](`crate::observer::FrontendObserverNode`).
#[async_trait::async_trait]
pub trait CatalogWriter: Send + Sync {
    async fn create_database(&self, db_name: &str, owner: UserId) -> Result<()>;

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()>;

    async fn create_view(&self, view: ProstView) -> Result<()>;

    async fn create_materialized_view(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_table(
        &self,
        source: Option<ProstSource>,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn replace_table(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
    ) -> Result<()>;

    async fn alter_table_name(&self, table_id: u32, table_name: &str) -> Result<()>;

    async fn create_index(
        &self,
        index: ProstIndex,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()>;

    async fn create_source(&self, source: ProstSource) -> Result<()>;

    async fn create_sink(&self, sink: ProstSink, graph: StreamFragmentGraph) -> Result<()>;

    async fn create_function(&self, function: ProstFunction) -> Result<()>;

    async fn drop_table(&self, source_id: Option<u32>, table_id: TableId) -> Result<()>;

    async fn drop_materialized_view(&self, table_id: TableId) -> Result<()>;

    async fn drop_view(&self, view_id: u32) -> Result<()>;

    async fn drop_source(&self, source_id: u32) -> Result<()>;

    async fn drop_sink(&self, sink_id: u32) -> Result<()>;

    async fn drop_database(&self, database_id: u32) -> Result<()>;

    async fn drop_schema(&self, schema_id: u32) -> Result<()>;

    async fn drop_index(&self, index_id: IndexId) -> Result<()>;

    async fn drop_function(&self, function_id: FunctionId) -> Result<()>;
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

    async fn create_view(&self, view: ProstView) -> Result<()> {
        let (_, version) = self.meta_client.create_view(view).await?;
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

    async fn create_table(
        &self,
        source: Option<ProstSource>,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<()> {
        let (_, version) = self.meta_client.create_table(source, table, graph).await?;
        self.wait_version(version).await
    }

    async fn replace_table(
        &self,
        table: ProstTable,
        graph: StreamFragmentGraph,
        mapping: ColIndexMapping,
    ) -> Result<()> {
        let version = self
            .meta_client
            .replace_table(table, graph, mapping)
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

    async fn create_function(&self, function: ProstFunction) -> Result<()> {
        let (_, version) = self.meta_client.create_function(function).await?;
        self.wait_version(version).await
    }

    async fn drop_table(&self, source_id: Option<u32>, table_id: TableId) -> Result<()> {
        let version = self.meta_client.drop_table(source_id, table_id).await?;
        self.wait_version(version).await
    }

    async fn drop_materialized_view(&self, table_id: TableId) -> Result<()> {
        let version = self.meta_client.drop_materialized_view(table_id).await?;
        self.wait_version(version).await
    }

    async fn drop_view(&self, view_id: u32) -> Result<()> {
        let version = self.meta_client.drop_view(view_id).await?;
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

    async fn drop_function(&self, function_id: FunctionId) -> Result<()> {
        let version = self.meta_client.drop_function(function_id).await?;
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

    async fn alter_table_name(&self, table_id: u32, table_name: &str) -> Result<()> {
        let version = self
            .meta_client
            .alter_table_name(table_id, table_name)
            .await?;
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
