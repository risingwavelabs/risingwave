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
#![feature(option_result_contains)]
#![feature(let_chains)]
#![feature(type_alias_impl_trait)]
#![feature(map_first_last)]
#![cfg_attr(coverage, feature(no_coverage))]

mod barrier;
pub mod cluster;
mod dashboard;
pub mod hummock;
pub mod manager;
mod model;
pub mod rpc;
pub mod storage;
mod stream;
pub mod test_utils;

use std::time::Duration;

use clap::{ArgEnum, Parser};

use crate::rpc::server::{rpc_serve, MetaStoreBackend};

#[derive(Copy, Clone, Debug, ArgEnum)]
enum Backend {
    Mem,
    Etcd,
}

#[derive(Debug, Parser)]
pub struct MetaNodeOpts {
    #[clap(long, default_value = "127.0.0.1:5690")]
    host: String,

    #[clap(long)]
    dashboard_host: Option<String>,

    #[clap(long)]
    prometheus_host: Option<String>,

    #[clap(long, arg_enum, default_value_t = Backend::Mem)]
    backend: Backend,

    #[clap(long, default_value_t = String::from(""))]
    etcd_endpoints: String,

    /// Maximum allowed heartbeat interval in ms
    #[clap(long, default_value = "10000")]
    max_heartbeat_interval: u32,
}

/// Start meta node
pub async fn start(opts: MetaNodeOpts) {
    let addr = opts.host.parse().unwrap();
    let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
    let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());
    let backend = match opts.backend {
        Backend::Etcd => MetaStoreBackend::Etcd {
            endpoints: opts
                .etcd_endpoints
                .split(',')
                .map(|x| x.to_string())
                .collect(),
        },
        Backend::Mem => MetaStoreBackend::Mem,
    };
    let max_heartbeat_interval = Duration::from_millis(opts.max_heartbeat_interval as u64);

    tracing::info!("Starting meta server at {}", addr);
    let (join_handle, _shutdown_send) = rpc_serve(
        addr,
        prometheus_addr,
        dashboard_addr,
        backend,
        max_heartbeat_interval,
    )
    .await
    .unwrap();
    join_handle.await.unwrap();
}
