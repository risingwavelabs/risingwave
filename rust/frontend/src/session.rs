use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::error::Result;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_sqlparser::parser::Parser;

use crate::catalog::catalog_service::{
    CatalogConnector, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::handler::handle;
use crate::FrontendOpts;

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    meta_client: MetaClient,
    // Different session may access catalog manager at the same time.
    catalog_manager: CatalogConnector,
}

impl FrontendEnv {
    pub async fn init(opts: &FrontendOpts) -> Result<Self> {
        let mut meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        // Register in meta by calling `AddWorkerNode` RPC.
        meta_client
            .register(opts.host.parse().unwrap(), WorkerType::Frontend)
            .await
            .unwrap();
        Self::with_meta_client(meta_client, opts).await
    }

    pub async fn with_meta_client(
        mut meta_client: MetaClient,
        opts: &FrontendOpts,
    ) -> Result<Self> {
        meta_client
            .register(opts.host.parse().unwrap(), WorkerType::Frontend)
            .await
            .unwrap();

        // Create default database when env init.
        let catalog_manager = CatalogConnector::new(meta_client.clone());
        catalog_manager
            .create_database(DEFAULT_DATABASE_NAME)
            .await?;
        catalog_manager
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await?;
        Ok(Self {
            meta_client,
            catalog_manager,
        })
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta_client
    }

    pub fn catalog_mgr(&self) -> &CatalogConnector {
        &self.catalog_manager
    }
}

pub struct RwSession {
    env: FrontendEnv,
    database: String,
}

impl RwSession {
    #[cfg(test)]
    pub fn new(env: FrontendEnv, database: String) -> Self {
        Self { env, database }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

pub struct RwSessionManager {
    env: FrontendEnv,
}

impl SessionManager for RwSessionManager {
    fn connect(&self) -> Box<dyn Session> {
        Box::new(RwSession {
            env: self.env.clone(),
            database: "dev".to_string(),
        })
    }
}

impl RwSessionManager {
    pub async fn new(opts: &FrontendOpts) -> Result<Self> {
        Ok(Self {
            env: FrontendEnv::init(opts).await?,
        })
    }
}

#[async_trait::async_trait]
impl Session for RwSession {
    async fn run_statement(
        &self,
        sql: &str,
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)?;
        // With pgwire, there would be at most 1 statement in the vec.
        assert_eq!(stmts.len(), 1);
        let stmt = stmts.swap_remove(0);
        let rsp = handle(self, stmt).await?;
        Ok(rsp)
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    #[serial_test::serial]
    async fn test_run_statement() {
        use std::ffi::OsString;

        use clap::StructOpt;
        use risingwave_meta::test_utils::LocalMeta;

        use super::*;

        let meta = LocalMeta::start(12008).await;
        let args: [OsString; 0] = []; // No argument.
        let mut opts = FrontendOpts::parse_from(args);
        opts.meta_addr = format!("http://{}", meta.meta_addr());
        let mgr = RwSessionManager::new(&opts).await.unwrap();
        // Check default database is created.
        assert!(mgr
            .env
            .catalog_manager
            .get_database(DEFAULT_DATABASE_NAME)
            .is_some());
        let session = mgr.connect();
        assert!(session.run_statement("select * from t").await.is_err());
        meta.stop().await;
    }
}
