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

use std::iter;
use std::sync::LazyLock;

use anyhow::Result;
use clap::Parser;
use home::home_dir;
use risingwave_common::config::{AsyncStackTraceOption, MetaBackend};
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::{default_parallelism, default_total_memory_bytes, ComputeNodeOpts};
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use shell_words::split;
use tokio::signal;

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
// TODO(kwannoel): Support user supplied profiles.
// Perhaps https://docs.rs/clap-serde-derive/0.2.1/clap_serde_derive/?
pub struct SingleNodeOpts {
    /// The address prometheus polls metrics from.
    #[clap(long, env = "RW_SINGLE_NODE_PROMETHEUS_LISTENER_ADDR")]
    prometheus_listener_addr: Option<String>,

    /// The path to the cluster configuration file.
    #[clap(long, env = "RW_SINGLE_NODE_CONFIG_PATH")]
    config_path: Option<String>,

    /// The store directory used by meta store and object store.
    #[clap(long, env = "RW_SINGLE_NODE_STORE_DIRECTORY")]
    pub store_directory: Option<String>,

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

    /// Extra options for meta node.
    #[clap(long, env = "RW_SINGLE_NODE_META_EXTRA_OPTS")]
    meta_extra_opts: Option<String>,

    /// Extra options for compute node.
    #[clap(long, env = "RW_SINGLE_NODE_COMPUTE_EXTRA_OPTS")]
    compute_extra_opts: Option<String>,

    /// Extra options for frontend node.
    #[clap(long, env = "RW_SINGLE_NODE_FRONTEND_EXTRA_OPTS")]
    frontend_extra_opts: Option<String>,

    /// Extra options for compactor node.
    #[clap(long, env = "RW_SINGLE_NODE_COMPACTOR_EXTRA_OPTS")]
    compactor_extra_opts: Option<String>,
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

pub fn parse_single_node_opts(opts: &SingleNodeOpts) -> Result<ParsedSingleNodeOpts> {
    // Get default options
    let mut meta_opts = SingleNodeOpts::default_meta_opts();
    let mut compute_opts = SingleNodeOpts::default_compute_opts();
    let mut frontend_opts = SingleNodeOpts::default_frontend_opts();
    let mut compactor_opts = SingleNodeOpts::default_compactor_opts();

    // Override with process level options
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

        compute_opts.meta_address = meta_addr.parse()?;
        frontend_opts.meta_addr = meta_addr.parse()?;
        compactor_opts.meta_address = meta_addr.parse()?;
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

    // Override with node-level extra options
    fn update_extra_opts(opts: &mut impl Parser, extra_opts: &Option<String>) -> Result<()> {
        if let Some(extra_opts) = extra_opts {
            let extra_opts = split(extra_opts)?;
            // This hack is required, because `update_from` treats arg[0] as the command name.
            let prefix = "".to_string();
            let extra_opts = iter::once(&prefix).chain(extra_opts.iter());
            opts.update_from(extra_opts);
        }
        Ok(())
    }
    update_extra_opts(&mut meta_opts, &opts.meta_extra_opts)?;
    update_extra_opts(&mut compute_opts, &opts.compute_extra_opts)?;
    update_extra_opts(&mut frontend_opts, &opts.frontend_extra_opts)?;
    update_extra_opts(&mut compactor_opts, &opts.compactor_extra_opts)?;

    Ok(ParsedSingleNodeOpts {
        meta_opts: Some(meta_opts),
        compute_opts: Some(compute_opts),
        frontend_opts: Some(frontend_opts),
        compactor_opts: Some(compactor_opts),
    })
}

// Defaults
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

impl SingleNodeOpts {
    pub fn create_store_directories(&self) -> Result<()> {
        let store_directory = self
            .store_directory
            .as_ref()
            .unwrap_or_else(|| &*DEFAULT_STORE_DIRECTORY);
        std::fs::create_dir_all(format!("{}/meta_store", store_directory))?;
        std::fs::create_dir_all(format!("{}/state_store", store_directory))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ParsedSingleNodeOpts {
    pub meta_opts: Option<MetaNodeOpts>,
    pub compute_opts: Option<ComputeNodeOpts>,
    pub frontend_opts: Option<FrontendOpts>,
    pub compactor_opts: Option<CompactorOpts>,
}

impl risingwave_common::opts::Opts for ParsedSingleNodeOpts {
    fn name() -> &'static str {
        "single_node"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        if let Some(opts) = self.meta_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.compute_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.frontend_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.compactor_opts.as_ref() {
            opts.meta_addr()
        } else {
            unreachable!("at least one service should be specified as checked during parsing")
        }
    }
}

/// This mode will eventually unify `playground`, `standalone` and `single_node` modes.
/// It will provide the following options:
/// 1. Profile-level options. Provides a set of node level options for a specific profile.
///    `playground`: Provide a set of options for a playground profile, uses in-memory store.
///    `single-node`: Provide a set of options for a single node profile, uses persistent local store.
///    `standalone`: No profile created for it. In this mode, most options are configured manually.
///                  The equivalent options will be the node level "extra-options".
/// 2. Process level options. These options will be mapped to each node's options.
///    They override profile-level options.
/// 3. Node level extra options: These have the highest precedence.
///    They override profile and process level options.
pub async fn single_node(
    ParsedSingleNodeOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }: ParsedSingleNodeOpts,
) -> Result<()> {
    tracing::info!("launching Risingwave in single_node mode");

    if let Some(opts) = meta_opts {
        tracing::info!("starting meta-node thread with cli args: {:?}", opts);

        let _meta_handle = tokio::spawn(async move {
            risingwave_meta_node::start(opts).await;
            tracing::warn!("meta is stopped, shutdown all nodes");
        });
        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    if let Some(opts) = compute_opts {
        tracing::info!("starting compute-node thread with cli args: {:?}", opts);
        let _compute_handle = tokio::spawn(async move { risingwave_compute::start(opts).await });
    }
    if let Some(opts) = frontend_opts {
        tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
        let _frontend_handle = tokio::spawn(async move { risingwave_frontend::start(opts).await });
    }
    if let Some(opts) = compactor_opts {
        tracing::info!("starting compactor-node thread with cli args: {:?}", opts);
        let _compactor_handle =
            tokio::spawn(async move { risingwave_compactor::start(opts).await });
    }

    // wait for log messages to be flushed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    eprintln!("-------------------------------");
    eprintln!("RisingWave single_node mode is ready.");

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    // TODO(kwannoel): Why can't be shutdown gracefully? Is it that the service just does not
    // support it?
    signal::ctrl_c().await.unwrap();
    tracing::info!("Ctrl+C received, now exiting");

    Ok(())
}

#[cfg(test)]
mod test {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_parse_extra_opts() {
        let raw_opts = "
--meta-extra-opts=--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\"\
--compute-extra-opts=--listen-addr 127.0.0.1:8888 --total-memory-bytes 123 --parallelism 10
";
        let actual = SingleNodeOpts::parse_from(raw_opts.lines());
        check(
            &actual,
            expect![[r#"
            SingleNodeOpts {
                prometheus_listener_addr: None,
                config_path: None,
                store_directory: None,
                meta_addr: None,
                compute_addr: None,
                frontend_addr: None,
                compactor_addr: None,
                meta_extra_opts: Some(
                    "--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\"",
                ),
            }"#]],
        );
        let actual_parsed = parse_single_node_opts(&actual).unwrap();
        check(
            actual_parsed,
            expect![[r#"
            ParsedSingleNodeOpts {
                meta_opts: Some(
                    MetaNodeOpts {
                        vpc_id: None,
                        security_group_id: None,
                        listen_addr: "127.0.0.1:5690",
                        advertise_addr: "127.0.0.1:9999",
                        dashboard_host: Some(
                            "0.0.0.0:5691",
                        ),
                        prometheus_listener_addr: Some(
                            "0.0.0.0:1250",
                        ),
                        etcd_endpoints: "",
                        etcd_auth: false,
                        etcd_username: "",
                        etcd_password: [REDACTED alloc::string::String],
                        sql_endpoint: Some(
                            "sqlite:///Users/noelkwan/.risingwave/meta_store/single_node.db?mode=rwc",
                        ),
                        dashboard_ui_path: None,
                        prometheus_endpoint: None,
                        prometheus_selector: None,
                        connector_rpc_endpoint: None,
                        privatelink_endpoint_default_tags: None,
                        config_path: "",
                        backend: Some(
                            Sql,
                        ),
                        barrier_interval_ms: None,
                        sstable_size_mb: None,
                        block_size_kb: None,
                        bloom_false_positive: None,
                        state_store: Some(
                            "hummock+fs:///Users/noelkwan/.risingwave/state_store",
                        ),
                        data_directory: Some(
                            "some path with spaces",
                        ),
                        do_not_config_object_storage_lifecycle: None,
                        backup_storage_url: None,
                        backup_storage_directory: None,
                        heap_profiling_dir: None,
                    },
                ),
                compute_opts: Some(
                    ComputeNodeOpts {
                        listen_addr: "0.0.0.0:5688",
                        advertise_addr: Some(
                            "0.0.0.0:5688",
                        ),
                        prometheus_listener_addr: "0.0.0.0:1250",
                        meta_address: List(
                            [
                                http://0.0.0.0:5690/,
                            ],
                        ),
                        connector_rpc_endpoint: None,
                        connector_rpc_sink_payload_format: None,
                        config_path: "",
                        total_memory_bytes: 24051816857,
                        parallelism: 10,
                        role: Both,
                        metrics_level: None,
                        data_file_cache_dir: None,
                        meta_file_cache_dir: None,
                        async_stack_trace: Some(
                            ReleaseVerbose,
                        ),
                        heap_profiling_dir: None,
                    },
                ),
                frontend_opts: Some(
                    FrontendOpts {
                        listen_addr: "0.0.0.0:4566",
                        advertise_addr: Some(
                            "0.0.0.0:4566",
                        ),
                        port: None,
                        meta_addr: List(
                            [
                                http://0.0.0.0:5690/,
                            ],
                        ),
                        prometheus_listener_addr: "0.0.0.0:1250",
                        health_check_listener_addr: "0.0.0.0:6786",
                        config_path: "",
                        metrics_level: None,
                        enable_barrier_read: None,
                    },
                ),
                compactor_opts: Some(
                    CompactorOpts {
                        listen_addr: "0.0.0.0:6660",
                        advertise_addr: Some(
                            "0.0.0.0:6660",
                        ),
                        port: None,
                        prometheus_listener_addr: "0.0.0.0:1250",
                        meta_address: List(
                            [
                                http://0.0.0.0:5690/,
                            ],
                        ),
                        compaction_worker_threads_number: None,
                        config_path: "",
                        metrics_level: None,
                        async_stack_trace: None,
                        heap_profiling_dir: None,
                        compactor_mode: None,
                        proxy_rpc_endpoint: "",
                    },
                ),
            }"#]],
        );
    }
}
