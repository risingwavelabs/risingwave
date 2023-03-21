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
#![feature(is_sorted)]

use std::future::Future;
use std::net::{TcpListener, TcpStream};
use std::pin::Pin;

use clap::Parser;
use risingwave_common::config::{load_config, MetaBackend, RwConfig};
use risingwave_common_proc_macro::OverrideConfig;
use tracing::info;

/// Command-line arguments for compute-node that overrides the config file.
#[derive(Parser, Clone, Debug, OverrideConfig)]
pub struct OverrideConfigOpts {
    #[clap(long, env = "RW_BACKEND", value_enum)]
    #[override_opts(path = meta.backend)]
    backend: Option<MetaBackend>,

    /// The interval of periodic barrier.
    #[clap(long, env = "RW_BARRIER_INTERVAL_MS")]
    #[override_opts(path = system.barrier_interval_ms)]
    barrier_interval_ms: Option<u32>,

    /// Target size of the Sstable.
    #[clap(long, env = "RW_SSTABLE_SIZE_MB")]
    #[override_opts(path = system.sstable_size_mb)]
    sstable_size_mb: Option<u32>,

    /// Size of each block in bytes in SST.
    #[clap(long, env = "RW_BLOCK_SIZE_KB")]
    #[override_opts(path = system.block_size_kb)]
    block_size_kb: Option<u32>,

    /// False positive probability of bloom filter.
    #[clap(long, env = "RW_BLOOM_FALSE_POSITIVE")]
    #[override_opts(path = system.bloom_false_positive)]
    bloom_false_positive: Option<f64>,

    /// State store url
    #[clap(long, env = "RW_STATE_STORE")]
    #[override_opts(path = system.state_store)]
    state_store: Option<String>,

    /// Remote directory for storing data and metadata objects.
    #[clap(long, env = "RW_DATA_DIRECTORY")]
    #[override_opts(path = system.data_directory)]
    data_directory: Option<String>,

    /// Remote storage url for storing snapshots.
    #[clap(long, env = "RW_BACKUP_STORAGE_URL")]
    #[override_opts(path = system.backup_storage_url)]
    backup_storage_url: Option<String>,

    /// Remote directory for storing snapshots.
    #[clap(long, env = "RW_BACKUP_STORAGE_DIRECTORY")]
    #[override_opts(path = system.backup_storage_directory)]
    backup_storage_directory: Option<String>,
}

// TODO: actually make this the kill opts
#[derive(Debug, Clone, Parser)]
pub struct KillOpts {
    #[clap(long, env = "RW_VPC_ID")]
    vpd_id: Option<String>,

    #[clap(long, env = "RW_VPC_SECURITY_GROUP_ID")]
    security_group_id: Option<String>,

    // TODO: rename to listen_address and separate out the port.
    #[clap(long, env = "RW_LISTEN_ADDR", default_value = "127.0.0.1:5690")]
    listen_addr: String,

    /// The address for contacting this instance of the service.
    /// This would be synonymous with the service's "public address"
    /// or "identifying address".
    /// It will serve as a unique identifier in cluster
    /// membership and leader election. Must be specified for etcd backend.
    #[clap(long, env = "RW_ADVERTISE_ADDR")]
    advertise_addr: String,

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

/// Start kill node
pub fn start(opts: KillOpts) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        let port = 2345;
        let err_msg = format!("expected port {} to be available", port);
        let listener = TcpListener::bind("127.0.0.1:2345").expect(err_msg.as_str());
        for stream in listener.incoming() {
            handle_client(stream.expect(err_msg.as_str()));
        }
    })
}

fn handle_client(stream: TcpStream) {
    panic!("received kill signal");
}
