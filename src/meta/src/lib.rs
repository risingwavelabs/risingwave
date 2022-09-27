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

#![feature(backtrace)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(generic_associated_types)]
#![feature(binary_heap_drain_sorted)]
#![feature(option_result_contains)]
#![feature(let_else)]
#![feature(type_alias_impl_trait)]
#![feature(map_first_last)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(is_some_with)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(once_cell)]
#![cfg_attr(coverage, feature(no_coverage))]
#![test_runner(risingwave_test_runner::test_runner::run_failpont_tests)]

mod barrier;
#[cfg(not(madsim))] // no need in simulation test
mod dashboard;
mod error;
pub mod hummock;
pub mod manager;
mod model;
pub mod rpc;
pub mod storage;
mod stream;

use std::time::Duration;

use clap::{ArgEnum, Parser};
pub use error::{MetaError, MetaResult};
use serde::{Deserialize, Serialize};

use crate::manager::MetaOpts;
use crate::rpc::server::{rpc_serve, AddressInfo, MetaStoreBackend};

#[derive(Copy, Clone, Debug, ArgEnum)]
enum Backend {
    Mem,
    Etcd,
}

#[derive(Debug, Parser)]
pub struct MetaNodeOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, default_value = "127.0.0.1:5690")]
    listen_addr: String,

    #[clap(long)]
    host: Option<String>,

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

    /// Maximum allowed heartbeat interval in seconds.
    #[clap(long, default_value = "300")]
    max_heartbeat_interval_secs: u32,

    #[clap(long)]
    dashboard_ui_path: Option<String>,

    /// No given `config_path` means to use default config.
    #[clap(long, default_value = "")]
    pub config_path: String,

    /// Whether to enable fail-on-recovery. If not set, default to enable. Should only be used in
    /// e2e tests.
    #[clap(long)]
    disable_recovery: bool,

    #[clap(long, default_value = "10")]
    meta_leader_lease_secs: u64,

    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// It is mainly useful for playgrounds.
    #[clap(long)]
    dangerous_max_idle_secs: Option<u64>,

    /// Interval of GC metadata in meta store and stale SSTs in object store.
    #[clap(long, default_value = "30")]
    vacuum_interval_sec: u64,

    /// Threshold used by worker node to filter out new SSTs when scanning object store, during
    /// full SST GC.
    #[clap(long, default_value = "604800")]
    min_sst_retention_time_sec: u64,

    /// The spin interval when collecting global GC watermark in hummock
    #[clap(long, default_value = "5")]
    collect_gc_watermark_spin_interval_sec: u64,

    /// Enable sanity check when SSTs are committed. By default disabled.
    #[clap(long)]
    enable_committed_sst_sanity_check: bool,

    /// Schedule compaction for all compaction groups with this interval.
    #[clap(long, default_value = "60")]
    pub periodic_compaction_interval_sec: u64,

    /// Seconds compaction scheduler should stall when there is no available compactor.
    #[clap(long, default_value = "5")]
    pub no_available_compactor_stall_sec: u64,

    #[clap(long, default_value = "10")]
    node_num_monitor_interval_sec: u64,
}

use std::future::Future;
use std::pin::Pin;

use risingwave_common::config::{load_config, StreamingConfig};

/// Start meta node
pub fn start(opts: MetaNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        let meta_config: MetaNodeConfig = load_config(&opts.config_path).unwrap();
        tracing::info!("Starting meta node with config {:?}", meta_config);
        let meta_addr = opts.host.unwrap_or_else(|| opts.listen_addr.clone());
        let listen_addr = opts.listen_addr.parse().unwrap();
        let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());
        let backend = match opts.backend {
            Backend::Etcd => MetaStoreBackend::Etcd {
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
            Backend::Mem => MetaStoreBackend::Mem,
        };

        let max_heartbeat_interval = Duration::from_secs(opts.max_heartbeat_interval_secs as u64);
        let barrier_interval =
            Duration::from_millis(meta_config.streaming.barrier_interval_ms as u64);
        let max_idle_ms = opts.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = meta_config.streaming.in_flight_barrier_nums as usize;
        let checkpoint_frequency = meta_config.streaming.checkpoint_frequency as usize;

        tracing::info!("Meta server listening at {}", listen_addr);
        let add_info = AddressInfo {
            addr: meta_addr,
            listen_addr,
            prometheus_addr,
            dashboard_addr,
            ui_path: opts.dashboard_ui_path,
        };
        let (join_handle, _shutdown_send) = rpc_serve(
            add_info,
            backend,
            max_heartbeat_interval,
            opts.meta_leader_lease_secs,
            MetaOpts {
                enable_recovery: !opts.disable_recovery,
                barrier_interval,
                in_flight_barrier_nums,
                minimal_scheduling: meta_config.streaming.minimal_scheduling,
                max_idle_ms,
                checkpoint_frequency,
                vacuum_interval_sec: opts.vacuum_interval_sec,
                min_sst_retention_time_sec: opts.min_sst_retention_time_sec,
                collect_gc_watermark_spin_interval_sec: opts.collect_gc_watermark_spin_interval_sec,
                enable_committed_sst_sanity_check: opts.enable_committed_sst_sanity_check,
                periodic_compaction_interval_sec: opts.periodic_compaction_interval_sec,
                no_available_compactor_stall_sec: opts.no_available_compactor_stall_sec,
                node_num_monitor_interval_sec: opts.node_num_monitor_interval_sec,
            },
        )
        .await
        .unwrap();
        join_handle.await.unwrap();
        tracing::info!("Meta server is stopped");
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MetaNodeConfig {
    // Below for streaming.
    #[serde(default)]
    pub streaming: StreamingConfig,
}
