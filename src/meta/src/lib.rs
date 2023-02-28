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

use clap::Parser;
pub use error::{MetaError, MetaResult};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_proc_macro::OverrideConfig;

use crate::manager::MetaOpts;
use crate::rpc::server::{rpc_serve, AddressInfo, MetaStoreBackend};

#[derive(Debug, Clone, Parser)]
pub struct MetaNodeOpts {
    // TODO: rename to listen_address and separate out the port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5690")]
    listen_addr: String,

    /// Deprecated. But we keep it for backward compatibility.
    #[clap(long, env = "RW_HOST")]
    host: Option<String>,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// It will serve as a unique identifier in cluster
    /// membership and leader election. Must be specified for etcd backend.
    /// TODO: After host is removed, we require that this parameter must be provided when using
    /// etcd
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    advertise_addr: Option<String>,

    #[clap(long, env = "RW_DASHBOARD_HOST")]
    dashboard_host: Option<String>,

    #[clap(long, env = "RW_PROMETHEUS_HOST")]
    prometheus_host: Option<String>,

    #[clap(long, env = "RW_ETCD_ENDPOINTS", default_value_t = String::from(""))]
    etcd_endpoints: String,

    /// Enable authentication with etcd. By default disabled.
    #[clap(long, env = "RW_ETCD_AUTH")]
    etcd_auth: bool,

    /// Username of etcd, required when --etcd-auth is enabled.
    #[clap(long, env = "RW_ETCD_USERNAME", default_value = "")]
    etcd_username: String,

    /// Password of etcd, required when --etcd-auth is enabled.
    #[clap(long, env = "RW_ETCD_PASSWORD", default_value = "")]
    etcd_password: String,

    #[clap(long, env = "RW_DASHBOARD_UI_PATH")]
    dashboard_ui_path: Option<String>,

    /// For dashboard service to fetch cluster info.
    #[clap(long, env = "RW_PROMETHEUS_ENDPOINT")]
    prometheus_endpoint: Option<String>,

    // TODO(zhidong): Make it required in v0.1.18
    /// State store url.
    #[clap(long, env = "RW_STATE_STORE")]
    state_store: Option<String>,

    /// Endpoint of the connector node, there will be a sidecar connector node
    /// colocated with Meta node in the cloud environment
    #[clap(long, env = "RW_CONNECTOR_RPC_ENDPOINT")]
    pub connector_rpc_endpoint: Option<String>,

    /// The path of `risingwave.toml` configuration file.
    ///
    /// If empty, default configuration values will be used.
    #[clap(long, env = "RW_CONFIG_PATH", default_value = "")]
    pub config_path: String,

    #[clap(flatten)]
    pub override_opts: OverrideConfigOpts,
}

/// Command-line arguments for compute-node that overrides the config file.
#[derive(Parser, Clone, Debug, OverrideConfig)]
pub struct OverrideConfigOpts {
    #[clap(long, env = "RW_BACKEND", arg_enum)]
    #[override_opts(path = meta.backend)]
    backend: Option<MetaBackend>,

    /// Target size of the Sstable.
    #[clap(long, env = "RW_SSTABLE_SIZE_MB")]
    #[override_opts(path = storage.sstable_size_mb)]
    sstable_size_mb: Option<u32>,

    /// Size of each block in bytes in SST.
    #[clap(long, env = "RW_BLOCK_SIZE_KB")]
    #[override_opts(path = storage.block_size_kb)]
    block_size_kb: Option<u32>,

    /// False positive probability of bloom filter.
    #[clap(long, env = "RW_BLOOM_FALSE_POSITIVE")]
    #[override_opts(path = storage.bloom_false_positive)]
    bloom_false_positive: Option<f64>,

    /// Remote directory for storing data and metadata objects.
    #[clap(long, env = "RW_DATA_DIRECTORY")]
    #[override_opts(path = storage.data_directory)]
    data_directory: Option<String>,

    /// Remote storage url for storing snapshots.
    #[clap(long, env = "RW_BACKUP_STORAGE_URL")]
    #[override_opts(path = backup.storage_url)]
    backup_storage_url: Option<String>,

    /// Remote directory for storing snapshots.
    #[clap(long, env = "RW_STORAGE_DIRECTORY")]
    #[override_opts(path = backup.storage_directory)]
    backup_storage_directory: Option<String>,
}

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use risingwave_common::config::{load_config, MetaBackend, RwConfig};
use tracing::info;

/// Start meta node
pub fn start(opts: MetaNodeOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    // WARNING: don't change the function signature. Making it `async fn` will cause
    // slow compile in release mode.
    Box::pin(async move {
        info!("Starting meta node");
        info!("> options: {:?}", opts);
        let config = load_config(&opts.config_path, Some(opts.override_opts));
        info!("> config: {:?}", config);
        info!("> version: {} ({})", RW_VERSION, GIT_SHA);
        let listen_addr: SocketAddr = opts.listen_addr.parse().unwrap();
        let meta_addr = opts.host.unwrap_or_else(|| listen_addr.ip().to_string());
        let dashboard_addr = opts.dashboard_host.map(|x| x.parse().unwrap());
        let prometheus_addr = opts.prometheus_host.map(|x| x.parse().unwrap());
        let advertise_addr = opts
            .advertise_addr
            .unwrap_or_else(|| format!("{}:{}", meta_addr, listen_addr.port()));
        let backend = match config.meta.backend {
            MetaBackend::Etcd => MetaStoreBackend::Etcd {
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
            MetaBackend::Mem => MetaStoreBackend::Mem,
        };

        validate_config(&config);

        let max_heartbeat_interval =
            Duration::from_secs(config.meta.max_heartbeat_interval_secs as u64);
        let barrier_interval = Duration::from_millis(config.streaming.barrier_interval_ms as u64);
        let max_idle_ms = config.meta.dangerous_max_idle_secs.unwrap_or(0) * 1000;
        let in_flight_barrier_nums = config.streaming.in_flight_barrier_nums;
        let checkpoint_frequency = config.streaming.checkpoint_frequency;

        info!("Meta server listening at {}", listen_addr);
        let add_info = AddressInfo {
            advertise_addr,
            listen_addr,
            prometheus_addr,
            dashboard_addr,
            ui_path: opts.dashboard_ui_path,
        };
        let (mut join_handle, leader_lost_handle, shutdown_send) = rpc_serve(
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
                sstable_size_mb: config.storage.sstable_size_mb,
                block_size_kb: config.storage.block_size_kb,
                bloom_false_positive: config.storage.bloom_false_positive,
                state_store: opts.state_store,
                data_directory: config.storage.data_directory,
                periodic_space_reclaim_compaction_interval_sec: config
                    .meta
                    .periodic_space_reclaim_compaction_interval_sec,
            },
        )
        .await
        .unwrap();

        let res = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("receive ctrl+c");
                shutdown_send.send(()).unwrap();
                join_handle.await
            }
            res = &mut join_handle => res,
        };
        res.unwrap();
        if let Some(leader_lost_handle) = leader_lost_handle {
            leader_lost_handle.abort();
        }
    })
}

fn validate_config(config: &RwConfig) {
    if config.meta.meta_leader_lease_secs <= 1 {
        let error_msg = "meta leader lease secs should be larger than 1";
        tracing::error!(error_msg);
        panic!("{}", error_msg);
    }
}
