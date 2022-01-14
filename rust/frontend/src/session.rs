use risingwave_common::error::Result;
use risingwave_meta::rpc::meta_client::MetaClient;
use risingwave_sqlparser::parser::Parser;

use crate::handler::handle;
use crate::pgwire::pg_result::PgResult;
use crate::pgwire::pg_server::{Session, SessionManager};
use crate::FrontendOpts;

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    meta_client: MetaClient,
}

impl FrontendEnv {
    pub async fn init(opts: FrontendOpts) -> Result<Self> {
        let meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        Ok(Self { meta_client })
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta_client
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
    async fn run_statement(&self, sql: &str) -> PgResult {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql).unwrap();
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

        let meta = LocalMeta::start().await;
        let args: [OsString; 0] = []; // No argument.
        let opts = FrontendOpts::parse_from(args);
        let mgr = RwSessionManager::new(opts).await.unwrap();
        let session = mgr.connect();
        let _result = session.run_statement("select 1").await;
        meta.stop().await;
    }
}
