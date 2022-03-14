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
#![feature(test)]
#![feature(map_first_last)]

#[macro_use]
extern crate log;
extern crate test;

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

    /// Interval to send heartbeat in ms
    #[clap(long, default_value = "1000")]
    pub heartbeat_interval: u32,
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
