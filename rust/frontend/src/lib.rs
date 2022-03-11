#![feature(map_try_insert)]

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

#[derive(Parser, Clone)]
pub struct FrontendOpts {
    #[clap(long, default_value = "127.0.0.1:4566")]
    pub host: String,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_addr: String,

    /// Interval to send heartbeat in ms
    #[clap(long, default_value = "1000")]
    pub heartbeat_interval: u32,
}

/// Start frontend
pub async fn start(opts: FrontendOpts) {
    let session_mgr = Arc::new(SessionManagerImpl::new(&opts).await.unwrap());
    pg_serve(&opts.host, session_mgr).await.unwrap();
}
