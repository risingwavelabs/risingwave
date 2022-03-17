#![feature(map_try_insert)]
#![feature(let_chains)]

#[macro_use]
pub mod catalog;
pub mod binder;
pub mod expr;
pub mod handler;
pub mod observer;
pub mod optimizer;
pub mod planner;
mod scheduler;
pub mod session;
pub mod utils;
extern crate log;
pub mod test_utils;

use std::sync::Arc;

use clap::Parser;
use pgwire::pg_server::pg_serve;
use session::SessionManagerImpl;

#[derive(Parser, Clone, Debug)]
pub struct FrontendOpts {
    #[clap(long, default_value = "127.0.0.1:4566")]
    pub host: String,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_addr: String,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

/// Start frontend
pub async fn start(opts: FrontendOpts) {
    let session_mgr = Arc::new(SessionManagerImpl::new(&opts).await.unwrap());
    pg_serve(&opts.host, session_mgr).await.unwrap();
}
