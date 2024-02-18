// Copyright 2024 RisingWave Labs
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

use std::sync::LazyLock;

use clap::Parser;
use home::home_dir;
use risingwave_common::config::{AsyncStackTraceOption, MetaBackend};
use risingwave_compactor::CompactorOpts;
use risingwave_compute::{default_parallelism, default_total_memory_bytes, ComputeNodeOpts};
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;

use crate::ParsedStandaloneOpts;

pub static DEFAULT_STORE_DIRECTORY: LazyLock<String> = LazyLock::new(|| {
    let mut home_path = home_dir().unwrap();
    home_path.push(".risingwave");
    let home_path = home_path.to_str().unwrap();
    home_path.to_string()
});

pub static DEFAULT_SINGLE_NODE_SQLITE_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{}/meta_store/single_node.db", &*DEFAULT_STORE_DIRECTORY));

pub static DEFAULT_SINGLE_NODE_SQL_ENDPOINT: LazyLock<String> =
    LazyLock::new(|| format!("sqlite://{}?mode=rwc", *DEFAULT_SINGLE_NODE_SQLITE_PATH));

pub static DEFAULT_SINGLE_NODE_STATE_STORE_PATH: LazyLock<String> =
    LazyLock::new(|| format!("{}/state_store", DEFAULT_STORE_DIRECTORY.clone()));

pub static DEFAULT_SINGLE_NODE_STATE_STORE_URL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "hummock+fs://{}",
        DEFAULT_SINGLE_NODE_STATE_STORE_PATH.clone()
    )
});

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
#[command(
    version,
    about = "[default] The Single Node mode. Start all services in one process, with process-level options. This will be executed if no subcommand is specified"
)]
/// Here we define our own defaults for the single node mode.
pub struct SingleNodeOpts {
    /// The address prometheus polls metrics from.
    #[clap(long, env = "RW_SINGLE_NODE_PROMETHEUS_LISTENER_ADDR")]
    prometheus_listener_addr: Option<String>,

    /// The path to the cluster configuration file.
    #[clap(long, env = "RW_SINGLE_NODE_CONFIG_PATH")]
    config_path: Option<String>,

    /// The store directory used by meta store and object store.
    #[clap(long, env = "RW_SINGLE_NODE_STORE_DIRECTORY")]
    store_directory: Option<String>,

    /// The address of the meta node.
    #[clap(long, env = "RW_SINGLE_NODE_META_ADDR")]
    meta_addr: Option<String>,

    /// The address of the compute node
    #[clap(long, env = "RW_SINGLE_NODE_COMPUTE_ADDR")]
    compute_addr: Option<String>,

    /// The address of the frontend node
    #[clap(long, env = "RW_SINGLE_NODE_FRONTEND_ADDR")]
    frontend_addr: Option<String>,

    /// The address of the compactor node
    #[clap(long, env = "RW_SINGLE_NODE_COMPACTOR_ADDR")]
    compactor_addr: Option<String>,
}

pub fn make_single_node_sql_endpoint(store_directory: &String) -> String {
    format!(
        "sqlite://{}/meta_store/single_node.db?mode=rwc",
        store_directory
    )
}

pub fn make_single_node_state_store_url(store_directory: &String) -> String {
    format!("hummock+fs://{}/state_store", store_directory)
}

pub fn map_single_node_opts_to_standalone_opts(opts: &SingleNodeOpts) -> ParsedStandaloneOpts {
    let mut meta_opts = SingleNodeOpts::default_meta_opts();
    let mut compute_opts = SingleNodeOpts::default_compute_opts();
    let mut frontend_opts = SingleNodeOpts::default_frontend_opts();
    let mut compactor_opts = SingleNodeOpts::default_compactor_opts();
    if let Some(prometheus_listener_addr) = &opts.prometheus_listener_addr {
        meta_opts.prometheus_listener_addr = Some(prometheus_listener_addr.clone());
        compute_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        frontend_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        compactor_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
    }
    if let Some(config_path) = &opts.config_path {
        meta_opts.config_path = config_path.clone();
        compute_opts.config_path = config_path.clone();
        frontend_opts.config_path = config_path.clone();
        compactor_opts.config_path = config_path.clone();
    }
    if let Some(store_directory) = &opts.store_directory {
        let state_store_url = make_single_node_state_store_url(store_directory);
        let meta_store_endpoint = make_single_node_sql_endpoint(store_directory);
        meta_opts.state_store = Some(state_store_url);
        meta_opts.sql_endpoint = Some(meta_store_endpoint);
    }
    if let Some(meta_addr) = &opts.meta_addr {
        meta_opts.listen_addr = meta_addr.clone();
        meta_opts.advertise_addr = meta_addr.clone();

        compute_opts.meta_address = meta_addr.parse().unwrap();
        frontend_opts.meta_addr = meta_addr.parse().unwrap();
        compactor_opts.meta_address = meta_addr.parse().unwrap();
    }
    if let Some(compute_addr) = &opts.compute_addr {
        compute_opts.listen_addr = compute_addr.clone();
    }
    if let Some(frontend_addr) = &opts.frontend_addr {
        frontend_opts.listen_addr = frontend_addr.clone();
    }
    if let Some(compactor_addr) = &opts.compactor_addr {
        compactor_opts.listen_addr = compactor_addr.clone();
    }
    ParsedStandaloneOpts {
        meta_opts: Some(meta_opts),
        compute_opts: Some(compute_opts),
        frontend_opts: Some(frontend_opts),
        compactor_opts: Some(compactor_opts),
    }
}

impl SingleNodeOpts {
    fn default_frontend_opts() -> FrontendOpts {
        FrontendOpts {
            listen_addr: "0.0.0.0:4566".to_string(),
            advertise_addr: Some("0.0.0.0:4566".to_string()),
            port: None,
            meta_addr: "http://0.0.0.0:5690".parse().unwrap(),
            prometheus_listener_addr: "0.0.0.0:1250".to_string(),
            health_check_listener_addr: "0.0.0.0:6786".to_string(),
            config_path: "".to_string(),
            metrics_level: None,
            enable_barrier_read: None,
        }
    }

    fn default_meta_opts() -> MetaNodeOpts {
        MetaNodeOpts {
            vpc_id: None,
            security_group_id: None,
            listen_addr: "0.0.0.0:5690".to_string(),
            advertise_addr: "0.0.0.0:5690".to_string(),
            dashboard_host: Some("0.0.0.0:5691".to_string()),
            prometheus_listener_addr: Some("0.0.0.0:1250".to_string()),
            etcd_endpoints: Default::default(),
            etcd_auth: false,
            etcd_username: Default::default(),
            etcd_password: Default::default(),
            sql_endpoint: Some(DEFAULT_SINGLE_NODE_SQL_ENDPOINT.clone()),
            dashboard_ui_path: None,
            prometheus_endpoint: None,
            prometheus_selector: None,
            connector_rpc_endpoint: None,
            privatelink_endpoint_default_tags: None,
            config_path: "".to_string(),
            backend: Some(MetaBackend::Sql),
            barrier_interval_ms: None,
            sstable_size_mb: None,
            block_size_kb: None,
            bloom_false_positive: None,
            state_store: Some(DEFAULT_SINGLE_NODE_STATE_STORE_URL.clone()),
            data_directory: Some("hummock_001".to_string()),
            do_not_config_object_storage_lifecycle: None,
            backup_storage_url: None,
            backup_storage_directory: None,
            heap_profiling_dir: None,
        }
    }

    pub fn default_compute_opts() -> ComputeNodeOpts {
        ComputeNodeOpts {
            listen_addr: "0.0.0.0:5688".to_string(),
            advertise_addr: Some("0.0.0.0:5688".to_string()),
            prometheus_listener_addr: "0.0.0.0:1250".to_string(),
            meta_address: "http://0.0.0.0:5690".parse().unwrap(),
            connector_rpc_endpoint: None,
            connector_rpc_sink_payload_format: None,
            config_path: "".to_string(),
            total_memory_bytes: default_total_memory_bytes(),
            parallelism: default_parallelism(),
            role: Default::default(),
            metrics_level: None,
            data_file_cache_dir: None,
            meta_file_cache_dir: None,
            async_stack_trace: Some(AsyncStackTraceOption::ReleaseVerbose),
            heap_profiling_dir: None,
        }
    }

    fn default_compactor_opts() -> CompactorOpts {
        CompactorOpts {
            listen_addr: "0.0.0.0:6660".to_string(),
            advertise_addr: Some("0.0.0.0:6660".to_string()),
            port: None,
            prometheus_listener_addr: "0.0.0.0:1250".to_string(),
            meta_address: "http://0.0.0.0:5690".parse().unwrap(),
            compaction_worker_threads_number: None,
            config_path: "".to_string(),
            metrics_level: None,
            async_stack_trace: None,
            heap_profiling_dir: None,
            compactor_mode: None,
            proxy_rpc_endpoint: "".to_string(),
        }
    }
}
