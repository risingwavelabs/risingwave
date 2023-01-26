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
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(option_result_contains)]
#![feature(type_alias_impl_trait)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(is_some_and)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(once_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![feature(assert_matches)]
#![feature(try_blocks)]
#![cfg_attr(coverage, feature(no_coverage))]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]

pub mod backup_restore;
mod barrier;
#[cfg(not(madsim))] // no need in simulation test
mod dashboard;
mod error;
pub mod hummock;
pub mod manager;
mod model;
mod rpc;
pub mod storage;
mod stream;

use std::time::Duration;

use clap::{ArgEnum, Parser};
pub use error::{MetaError, MetaResult};

use crate::manager::MetaOpts;
use crate::rpc::server::{rpc_serve, AddressInfo, MetaStoreBackend};

#[derive(Copy, Clone, Debug, ArgEnum)]
pub enum Backend {
    Mem,
    Etcd,
}

#[derive(Debug, Clone, Parser)]
pub struct MetaNodeOpts {
    // TODO: rename to listen_address and separate out the port.
    /// The address for this service to listen to locally
    #[clap(long, default_value = "127.0.0.1:5690")]
    listen_address: String,

    /// The address for contacting this instance of the meta service.
    /// This also serves as its unique identifier in cluster
    /// membership and leader election. Must be specified for etcd cluster.
    #[clap(long, required_if_eq("backend", "Backend::Etcd"))]
    contact_address: Option<String>,

    #[clap(long)]
    dashboard_host: Option<String>,

    #[clap(long)]
    prometheus_host: Option<String>,

    #[clap(long, arg_enum, default_value_t = Backend::Mem)]
    backend: Backend,

    #[clap(long, default_value_t = String::from(""))]
    etcd_endpoints: String,

    /// Enable authentication with etcd. By default disabled.
    #[clap(long)]
    etcd_auth: bool,

    /// Username of etcd, required when --etcd-auth is enabled.
    /// Default value is read from the 'ETCD_USERNAME' environment variable.
    #[clap(long, env = "ETCD_USERNAME", default_value = "")]
    etcd_username: String,

    /// Password of etcd, required when --etcd-auth is enabled.
    /// Default value is read from the 'ETCD_PASSWORD' environment variable.
    #[clap(long, env = "ETCD_PASSWORD", default_value = "")]
    etcd_password: String,

    #[clap(long)]
    dashboard_ui_path: Option<String>,

    /// For dashboard service to fetch cluster info.
    #[clap(long)]
    prometheus_endpoint: Option<String>,

    /// Endpoint of the connector node, there will be a sidecar connector node
    /// colocated with Meta node in the cloud environment
    #[clap(long, env = "META_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    ///
    /// Note that internal system parameters should be defined in the configuration file at
    /// [`risingwave_common::config`] instead of command line arguments.
    #[clap(long, default_value = "")]
    pub config_path: String,
}

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::load_config;

/// Start meta node
pub fn start(opts: MetaNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let config = load_config(&opts.config_path);
        tracing::info!("Starting meta node with config {:?}", config);
        tracing::info!("Starting meta node with options {:?}", opts);
        let listen_addr = opts.listen_address.parse().unwrap();
        let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());
        let (contact_addr, backend) = match opts.backend {
            Backend::Etcd => (
                opts.contact_address
                    .expect("contact_address must be specified when using etcd"),
                MetaStoreBackend::Etcd {
                    endpoints: opts
                        .etcd_endpoints
                        .split(',')
                        .map(|x| x.to_string())
                        .collect(),
                    credentials: match opts.etcd_auth {
                        true => Some((opts.etcd_username, opts.etcd_password)),
                        false => None,
                    },
                },
            ),
            Backend::Mem => (
                opts.contact_address
                    .unwrap_or_else(|| opts.listen_address.clone()),
                MetaStoreBackend::Mem,
            ),
        };

        let max_heartbeat_interval =
            Duration::from_secs(config.meta.max_heartbeat_interval_secs as u64);
        let barrier_interval = Duration::from_millis(config.streaming.barrier_interval_ms as u64);
        let max_idle_ms = config.meta.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = config.streaming.in_flight_barrier_nums;
        let checkpoint_frequency = config.streaming.checkpoint_frequency;

        tracing::info!("Meta server listening at {}", listen_addr);
        let add_info = AddressInfo {
            contact_addr,
            listen_addr,
            prometheus_addr,
            dashboard_addr,
            ui_path: opts.dashboard_ui_path,
        };
        let (join_handle, leader_lost_handle, _shutdown_send) = rpc_serve(
            add_info,
            backend,
            max_heartbeat_interval,
            config.meta.meta_leader_lease_secs,
            MetaOpts {
                enable_recovery: !config.meta.disable_recovery,
                barrier_interval,
                in_flight_barrier_nums,
                max_idle_ms,
                checkpoint_frequency,
                compaction_deterministic_test: config.meta.enable_compaction_deterministic,
                vacuum_interval_sec: config.meta.vacuum_interval_sec,
                min_sst_retention_time_sec: config.meta.min_sst_retention_time_sec,
                collect_gc_watermark_spin_interval_sec: config
                    .meta
                    .collect_gc_watermark_spin_interval_sec,
                enable_committed_sst_sanity_check: config.meta.enable_committed_sst_sanity_check,
                periodic_compaction_interval_sec: config.meta.periodic_compaction_interval_sec,
                node_num_monitor_interval_sec: config.meta.node_num_monitor_interval_sec,
                prometheus_endpoint: opts.prometheus_endpoint,
                connector_rpc_endpoint: opts.connector_rpc_endpoint,
                backup_storage_url: config.backup.storage_url,
                backup_storage_directory: config.backup.storage_directory,
            },
        )
        .await
        .unwrap();

        if let Some(leader_lost_handle) = leader_lost_handle {
            tokio::select! {
                _ = join_handle => {},
                _ = leader_lost_handle => {},
            }
        } else {
            join_handle.await.unwrap();
        }
    })
}
