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

use std::cell::RefCell;
use std::error::Error;
use std::mem;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::Result;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
};
use risingwave_pb::stream_plan::StreamNode;
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;

use crate::binder::Binder;
use crate::catalog::catalog_service::CatalogWriter;
use crate::catalog::root_catalog::Catalog;
use crate::catalog::DatabaseId;
use crate::meta::FrontendMetaClient;
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{FrontendEnv, QueryContext, SessionImpl};
use crate::FrontendOpts;

/// LocalFrontend is an embedded frontend without starting meta and without
/// starting frontend as a tcp server.
pub struct LocalFrontend {
    pub opts: FrontendOpts,
    env: FrontendEnv,
}

impl SessionManager for LocalFrontend {
    fn connect(
        &self,
        _database: &str,
    ) -> std::result::Result<Arc<dyn Session>, Box<dyn Error + Send + Sync>> {
        Ok(self.session_ref())
    }
}

impl LocalFrontend {
    pub async fn new(opts: FrontendOpts) -> Self {
        let env = FrontendEnv::mock();
        Self { opts, env }
    }

    pub async fn run_sql(
        &self,
        sql: impl Into<String>,
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql = sql.into();
        self.session_ref().run_statement(sql.as_str()).await
    }

    /// Convert a sql (must be an `Query`) into an unoptimized batch plan.
    pub async fn to_batch_plan(&self, sql: impl Into<String>) -> Result<PlanRef> {
        let statements = Parser::parse_sql(&sql.into()).unwrap();
        let statement = statements.get(0).unwrap();
        if let Statement::Query(query) = statement {
            let session = self.session_ref();

            let bound = {
                let mut binder = Binder::new(
                    session.env().catalog_reader().read_guard(),
                    session.database().to_string(),
                );
                binder.bind(Statement::Query(query.clone()))?
            };
            Ok(
                Planner::new(Rc::new(RefCell::new(QueryContext::new(session))))
                    .plan(bound)
                    .unwrap()
                    .gen_batch_query_plan(),
            )
        } else {
            unreachable!()
        }
    }

    pub fn session_ref(&self) -> Arc<SessionImpl> {
        Arc::new(SessionImpl::new(
            self.env.clone(),
            DEFAULT_DATABASE_NAME.to_string(),
        ))
    }
}

pub struct MockCatalogWriter {
    catalog: Arc<RwLock<Catalog>>,
    id: AtomicU32,
}

#[async_trait::async_trait]
impl CatalogWriter for MockCatalogWriter {
    async fn create_database(&self, db_name: &str) -> Result<()> {
        self.catalog.write().create_database(ProstDatabase {
            name: db_name.to_string(),
            id: 0,
        });
        Ok(())
    }

    async fn create_schema(&self, db_id: DatabaseId, schema_name: &str) -> Result<()> {
        self.catalog.write().create_schema(ProstSchema {
            id: 0,
            name: schema_name.to_string(),
            database_id: db_id,
        });
        Ok(())
    }

    async fn create_materialized_source(
        &self,
        source: ProstSource,
        table: ProstTable,
        _plan: StreamNode,
    ) -> Result<()> {
        self.create_source(source).await?;
        self.create_materialized_view(table).await?;
        Ok(())
    }

    async fn create_materialized_view(&self, mut table: ProstTable) -> Result<()> {
        table.id = self.gen_id();
        self.catalog.write().create_table(&table);
        Ok(())
    }

    async fn create_source(&self, mut source: ProstSource) -> Result<()> {
        source.id = self.gen_id();
        self.catalog.write().create_source(source);
        Ok(())
    }
}

impl MockCatalogWriter {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        catalog.write().create_database(ProstDatabase {
            name: DEFAULT_DATABASE_NAME.to_string(),
            id: 0,
        });
        catalog.write().create_schema(ProstSchema {
            id: 0,
            name: DEFAULT_SCHEMA_NAME.to_string(),
            database_id: 0,
        });
        Self {
            catalog,
            id: AtomicU32::new(0),
        }
    }

    fn gen_id(&self) -> u32 {
        self.id.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct MockFrontendMetaClient {}

#[async_trait::async_trait]
impl FrontendMetaClient for MockFrontendMetaClient {
    async fn pin_snapshot(&self) -> Result<u64> {
        Ok(0)
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn unpin_snapshot(&self, _epoch: u64) -> Result<()> {
        Ok(())
    }
}
