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

#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(rustdoc::private_intra_doc_links)]
#![feature(map_try_insert)]
#![feature(negative_impls)]
#![feature(generators)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(let_else)]
#![feature(trait_alias)]
#![feature(drain_filter)]
#![feature(if_let_guard)]
#![feature(assert_matches)]
#![feature(map_first_last)]
#![feature(lint_reasons)]
#![feature(box_patterns)]
#![feature(once_cell)]

#[macro_use]
mod catalog;
pub use catalog::TableCatalog;
mod binder;
pub use binder::{bind_data_type, Binder};
pub mod expr;
pub mod handler;
pub use handler::PgResponseStream;
mod observer;
mod optimizer;
pub use optimizer::PlanRef;
mod planner;
pub use planner::Planner;
#[expect(dead_code)]
mod scheduler;
pub use scheduler::{DistributedQueryStream, LocalQueryStream, SchedulerResult};
pub mod session;
mod stream_fragmenter;
pub use stream_fragmenter::build_graph;
mod utils;
pub use utils::{explain_stream_graph, WithOptions};
mod meta_client;
pub mod test_utils;
mod user;

mod monitor;

use std::ffi::OsString;
use std::iter;
use std::sync::Arc;

use clap::Parser;
use pgwire::pg_server::pg_serve;
use serde::{Deserialize, Serialize};
use session::SessionManagerImpl;

#[derive(Parser, Clone, Debug)]
pub struct FrontendOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, default_value = "127.0.0.1:4566")]
    pub host: String,

    // Optional, we will use listen_address if not specified.
    #[clap(long)]
    pub client_address: Option<String>,

    // TODO: This is currently unused.
    #[clap(long)]
    pub port: Option<u16>,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_addr: String,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,

    #[clap(long, default_value = "127.0.0.1:2222")]
    pub prometheus_listener_addr: String,

    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    #[clap(long, default_value = "0")]
    pub metrics_level: u32,
}

impl Default for FrontendOpts {
    fn default() -> Self {
        FrontendOpts::parse_from(iter::empty::<OsString>())
    }
}

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::ServerConfig;

/// Start frontend
pub fn start(opts: FrontendOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let session_mgr = Arc::new(SessionManagerImpl::new(&opts).await.unwrap());
        pg_serve(&opts.host, session_mgr).await.unwrap();
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct FrontendConfig {
    // For connection
    #[serde(default)]
    pub server: ServerConfig,
}
