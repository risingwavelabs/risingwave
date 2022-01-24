use std::ffi::OsString;

use clap::StructOpt;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::Session;
use risingwave_meta::test_utils::LocalMeta;

use crate::session::{FrontendEnv, RwSession};
use crate::FrontendOpts;

pub struct LocalFrontend {
    pub opts: FrontendOpts,
    pub session: RwSession,
}

impl LocalFrontend {
    pub async fn new() -> Self {
        let args: [OsString; 0] = []; // No argument.
        let mut opts: FrontendOpts = FrontendOpts::parse_from(args);
        opts.host = "127.0.0.1:45666".to_string();
        opts.meta_addr = format!("http://{}", LocalMeta::meta_addr());
        let env = FrontendEnv::init(&opts).await.unwrap();
        let session = RwSession::new(env, "dev".to_string());
        Self { opts, session }
    }

    pub async fn run_sql(
        &self,
        sql: impl Into<String>,
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql = sql.into();
        self.session.run_statement(sql.as_str()).await
    }

    pub fn session(&self) -> &RwSession {
        &self.session
    }
}
