#![allow(dead_code)]
#![warn(clippy::doc_markdown)]
#![warn(clippy::explicit_into_iter_loop)]
#![warn(clippy::explicit_iter_loop)]
#![warn(clippy::inconsistent_struct_constructor)]
#![warn(clippy::map_flatten)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(test)]
#![feature(map_first_last)]

// This is a bug of rustc which warn me to remove macro_use, I have add this.
#[allow(unused_imports)]
#[macro_use]
extern crate risingwave_common;
#[macro_use]
extern crate log;
extern crate test;

pub mod rpc;
pub mod server;

use clap::Parser;

/// Command-line arguments for compute-node.
#[derive(Parser, Debug)]
pub struct ComputeNodeOpts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    pub log4rs_config: String,

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

    /// Use `tokio-tracing` instead of `log4rs` for observability.
    #[clap(long)]
    pub enable_tracing: bool,
}
