// Copyright 2023 Singularity Data
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
use risingwave_common::config::{load_config, OverwriteConfig, RwConfig};
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

/// CLI argguments received by meta node. Overwrites fields in
/// [`risingwave_common::config::CompactorConfig`].
#[derive(Parser, Clone, Debug)]
pub struct FrontendOpts {
    #[clap(long = "host")]
    pub listen_addr: Option<String>,

    #[clap(long)]
    pub client_address: Option<String>,

    #[clap(long)]
    pub meta_addr: Option<String>,

    #[clap(long)]
    pub prometheus_listener_addr: Option<String>,

    #[clap(long)]
    pub health_check_listener_addr: Option<String>,

    /// Used for control the metrics level, similar to log level.
    /// 0 = close metrics
    /// >0 = open metrics
    #[clap(long)]
    pub metrics_level: Option<u32>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

impl Default for FrontendOpts {
    fn default() -> Self {
        FrontendOpts::parse_from(iter::empty::<OsString>())
    }
}

impl OverwriteConfig for FrontendOpts {
    fn overwrite(self, config: &mut RwConfig) {
        let mut c = &mut config.frontend;
        if let Some(v) = self.listen_addr {
            c.listen_addr = v;
        }
        if self.client_address.is_some() {
            c.client_address = self.client_address;
        }
        if let Some(v) = self.meta_addr {
            c.meta_addr = v;
        }
        if let Some(v) = self.prometheus_listener_addr {
            c.prometheus_listener_addr = v;
        }
        if let Some(v) = self.health_check_listener_addr {
            c.health_check_listener_addr = v;
        }
        if let Some(v) = self.metrics_level {
            c.metrics_level = v;
        }
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
        let config = load_config(&opts.config_path.clone(), Some(opts));
        let listen_addr = config.frontend.listen_addr.clone();
        let session_mgr = Arc::new(SessionManagerImpl::new(config).await.unwrap());
        pg_serve(&listen_addr, session_mgr, Some(TlsConfig::new_default()))
            .await
            .unwrap();
    })
}
