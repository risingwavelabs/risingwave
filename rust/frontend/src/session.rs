use std::sync::{Arc, Mutex};

use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_rpc_client::MetaClient;
use risingwave_sqlparser::parser::Parser;

use crate::catalog::catalog_service::{
    RemoteCatalogManager, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
};
use crate::handler::handle;
use crate::pgwire::pg_response::PgResponse;
use crate::pgwire::pg_server::{Session, SessionManager};
use crate::FrontendOpts;

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    meta_client: MetaClient,
    // Different session may access catalog manager at the same time.
    catalog_manager: Arc<Mutex<RemoteCatalogManager>>,
}

impl FrontendEnv {
    pub async fn init(opts: FrontendOpts) -> Result<Self> {
        let meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        // Create default database when env init.
        let mut catalog_manager = RemoteCatalogManager::new(meta_client.clone());
        catalog_manager
            .create_database(DEFAULT_DATABASE_NAME)
            .await?;
        catalog_manager
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await?;
        Ok(Self {
            meta_client,
            catalog_manager: Arc::new(Mutex::new(catalog_manager)),
        })
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta_client
    }

    pub fn catalog_mgr(&self) -> Arc<Mutex<RemoteCatalogManager>> {
        self.catalog_manager.clone()
    }
}

pub struct RwSession {
    env: FrontendEnv,
}

pub struct RwSessionManager {
    env: FrontendEnv,
}

impl SessionManager for RwSessionManager {
    fn connect(&self) -> Box<dyn Session> {
        Box::new(RwSession {
            env: self.env.clone(),
        })
    }
}

impl RwSessionManager {
    pub async fn new(opts: FrontendOpts) -> Result<Self> {
        Ok(Self {
            env: FrontendEnv::init(opts).await?,
        })
    }
}

#[async_trait::async_trait]
impl Session for RwSession {
    async fn run_statement(&self, sql: &str) -> Result<PgResponse> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)
            .map_err(|e| RwError::from(ErrorCode::ParseError(Box::new(e))))?;
        // With pgwire, there would be at most 1 statement in the vec.
        assert_eq!(stmts.len(), 1);
        let stmt = stmts.swap_remove(0);
        handle(&self.env, stmt).await
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_run_statement() {
        use std::ffi::OsString;

        use clap::StructOpt;
        use risingwave_meta::test_utils::LocalMeta;

        use super::*;

        let sled_root = tempfile::tempdir().unwrap();
        let meta = LocalMeta::start(sled_root).await;
        let args: [OsString; 0] = []; // No argument.
        let opts = FrontendOpts::parse_from(args);
        let mgr = RwSessionManager::new(opts.clone()).await.unwrap();
        // Check default database is created.
        assert!(mgr
            .env
            .catalog_manager
            .lock()
            .unwrap()
            .get_database(DEFAULT_DATABASE_NAME)
            .is_some());
        let session = mgr.connect();
        assert!(session.run_statement("select * from t").await.is_err());
        meta.stop().await;
    }
}
