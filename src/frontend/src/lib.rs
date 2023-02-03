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

#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(rustdoc::private_intra_doc_links)]
#![feature(map_try_insert)]
#![feature(negative_impls)]
#![feature(generators)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(trait_alias)]
#![feature(drain_filter)]
#![feature(if_let_guard)]
#![feature(let_chains)]
#![feature(assert_matches)]
#![feature(lint_reasons)]
#![feature(box_patterns)]
#![feature(once_cell)]
#![feature(result_option_inspect)]
#![feature(macro_metavar_expr)]
#![recursion_limit = "256"]

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
pub use optimizer::{OptimizerContext, OptimizerContextRef, PlanRef};
mod planner;
pub use planner::Planner;
#[expect(dead_code)]
mod scheduler;
pub mod session;
mod stream_fragmenter;
use risingwave_common_proc_macro::OverrideConfig;
pub use stream_fragmenter::build_graph;
mod utils;
pub use utils::{explain_stream_graph, WithOptions};
mod meta_client;
pub mod test_utils;
mod user;

pub mod health_service;
mod monitor;

use std::ffi::OsString;
use std::iter;
use std::sync::Arc;

use clap::Parser;
use pgwire::pg_server::pg_serve;
use session::SessionManagerImpl;

/// Command-line arguments for frontend-node.
#[derive(Parser, Clone, Debug)]
pub struct FrontendOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, env = "RW_HOST", default_value = "127.0.0.1:4566")]
    pub host: String,

    // Optional, we will use listen_address if not specified.
    #[clap(long, env = "RW_CLIENT_ADDRESS")]
    pub client_address: Option<String>,

    // TODO: This is currently unused.
    #[clap(long, env = "RW_PORT")]
    pub port: Option<u16>,

    #[clap(long, env = "RW_META_ADDR", default_value = "http://127.0.0.1:5690")]
    pub meta_addr: String,

    #[clap(
        long,
        env = "RW_PROMETHEUS_LISTENER_ADDR",
        default_value = "127.0.0.1:2222"
    )]
    pub prometheus_listener_addr: String,

    #[clap(
        long,
        env = "RW_HEALTH_CHECK_LISTENER_ADDR",
        default_value = "127.0.0.1:6786"
    )]
    pub health_check_listener_addr: String,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    ///
    /// Note that internal system parameters should be defined in the configuration file at
    /// [`risingwave_common::config`] instead of command line arguments.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    #[clap(flatten)]
    override_opts: OverrideConfigOpts,
}

/// Command-line arguments for frontend-node that overrides the config file.
#[derive(Parser, Clone, Debug, OverrideConfig)]
struct OverrideConfigOpts {
    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    #[clap(long, env = "RW_METRICS_LEVEL")]
    #[override_opts(path = server.metrics_level)]
    pub metrics_level: Option<u32>,
}

impl Default for FrontendOpts {
    fn default() -> Self {
        FrontendOpts::parse_from(iter::empty::<OsString>())
    }
}

use std::future::Future;
use std::pin::Pin;

use pgwire::pg_protocol::TlsConfig;

/// Start frontend
pub fn start(opts: FrontendOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let addr = opts.host.clone();
        let session_mgr = Arc::new(SessionManagerImpl::new(opts).await.unwrap());
        pg_serve(&addr, session_mgr, Some(TlsConfig::new_default()))
            .await
            .unwrap();
    })
}
