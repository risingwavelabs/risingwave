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

#![warn(clippy::dbg_macro)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![warn(clippy::no_effect_underscore_binding)]
#![warn(clippy::await_holding_lock)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![cfg_attr(coverage, feature(no_coverage))]

#[macro_use]
extern crate log;

pub mod rpc;
pub mod server;

use clap::Parser;

/// Command-line arguments for compute-node.
#[derive(Parser, Debug)]
pub struct ComputeNodeOpts {
    #[clap(long, default_value = "127.0.0.1:5688")]
    pub host: String,

    #[clap(long, default_value = "in-memory")]
    pub state_store: String,

    #[clap(long, default_value = "127.0.0.1:1222")]
    pub prometheus_listener_addr: String,

    #[clap(long, default_value = "0")]
    pub metrics_level: u32,

    #[clap(long, default_value = "http://127.0.0.1:5690")]
    pub meta_address: String,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,

    /// Enable reporting tracing information to jaeger
    #[clap(long)]
    pub enable_jaeger_tracing: bool,
}

use crate::server::compute_node_serve;

/// Start compute node
pub async fn start(opts: ComputeNodeOpts) {
    tracing::info!("meta address: {}", opts.meta_address.clone());

    let addr = opts.host.parse().unwrap();
    tracing::info!("Starting server at {}", addr);

    let (join_handle, _shutdown_send) = compute_node_serve(addr, opts).await;
    join_handle.await.unwrap();
}
