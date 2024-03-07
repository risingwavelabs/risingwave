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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::LazyLock;

use anyhow::Result;
use clap::Parser;
use home::home_dir;
use risingwave_common::config::{AsyncStackTraceOption, MetaBackend};
use risingwave_compactor::CompactorOpts;
use risingwave_compute::{default_parallelism, default_total_memory_bytes, ComputeNodeOpts};
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use shell_words::split;

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

    /// Frontend Node level overrides
    #[clap(long, env = "RW_SINGLE_NODE_FRONTEND_NODE_OVERRIDE_OPTS")]
    pub frontend_node_override_opts: Option<RawOpts>,

    /// Meta Node level overrides
    #[clap(long, env = "RW_SINGLE_NODE_META_NODE_OVERRIDE_OPTS")]
    pub meta_node_override_opts: Option<RawOpts>,

    /// Compute Node level overrides
    #[clap(long, env = "RW_SINGLE_NODE_COMPUTE_NODE_OVERRIDE_OPTS")]
    pub compute_node_override_opts: Option<RawOpts>,

    /// Compactor Node level overrides
    #[clap(long, env = "RW_SINGLE_NODE_COMPACTOR_NODE_OVERRIDE_OPTS")]
    pub compactor_node_override_opts: Option<RawOpts>,

    /// Disable frontend
    #[clap(long, env = "RW_SINGLE_NODE_DISABLE_FRONTEND")]
    pub disable_frontend: bool,

    /// Disable meta
    #[clap(long, env = "RW_SINGLE_NODE_DISABLE_META")]
    pub disable_meta: bool,

    /// Disable compute
    #[clap(long, env = "RW_SINGLE_NODE_DISABLE_COMPUTE")]
    pub disable_compute: bool,

    /// Disable compactor
    #[clap(long, env = "RW_SINGLE_NODE_DISABLE_COMPACTOR")]
    pub disable_compactor: bool,
}

struct NormalizedSingleNodeOpts {
    frontend_opts: Option<RawOpts>,
    meta_opts: Option<RawOpts>,
    compute_opts: Option<RawOpts>,
    compactor_opts: Option<RawOpts>,
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
    todo!()
}

pub fn normalized_single_node_opts(opts: &SingleNodeOpts) -> NormalizedSingleNodeOpts {
    let mut meta_opts = RawOpts::default_meta_opts();
    let mut compute_opts = RawOpts::default_compute_opts();
    let mut frontend_opts = RawOpts::default_frontend_opts();
    let mut compactor_opts = RawOpts::default_compactor_opts();

    if let Some(prometheus_listener_addr) = &opts.prometheus_listener_addr {
        meta_opts.inner.insert(
            "--prometheus-listener-addr".to_string(),
            prometheus_listener_addr.clone(),
        );
        compute_opts.inner.insert(
            "--prometheus-listener-addr".to_string(),
            prometheus_listener_addr.clone(),
        );
        frontend_opts.inner.insert(
            "--prometheus-listener-addr".to_string(),
            prometheus_listener_addr.clone(),
        );
        compactor_opts.inner.insert(
            "--prometheus-listener-addr".to_string(),
            prometheus_listener_addr.clone(),
        );
    }
    if let Some(config_path) = &opts.config_path {
        meta_opts
            .inner
            .insert("--config-path".to_string(), config_path.clone());
        compute_opts
            .inner
            .insert("--config-path".to_string(), config_path.clone());
        frontend_opts
            .inner
            .insert("--config-path".to_string(), config_path.clone());
        compactor_opts
            .inner
            .insert("--config-path".to_string(), config_path.clone());
    }
    if let Some(store_directory) = &opts.store_directory {
        let state_store_url = make_single_node_state_store_url(store_directory);
        let meta_store_endpoint = make_single_node_sql_endpoint(store_directory);
        meta_opts
            .inner
            .insert("--state-store".to_string(), state_store_url);
        meta_opts
            .inner
            .insert("--sql-endpoint".to_string(), meta_store_endpoint);
    }
    if let Some(meta_addr) = &opts.meta_addr {
        meta_opts
            .inner
            .insert("--listen-addr".to_string(), meta_addr.to_string());
        meta_opts
            .inner
            .insert("--advertise-addr".to_string(), meta_addr.to_string());
        compute_opts
            .inner
            .insert("--meta-address".to_string(), meta_addr.to_string());
        frontend_opts
            .inner
            .insert("--meta-addr".to_string(), meta_addr.to_string());
        compactor_opts
            .inner
            .insert("--meta-address".to_string(), meta_addr.to_string());
    }
    if let Some(compute_addr) = &opts.compute_addr {
        compute_opts
            .inner
            .insert("--listen-addr".to_string(), compute_addr.to_string());
    }
    if let Some(frontend_addr) = &opts.frontend_addr {
        frontend_opts
            .inner
            .insert("--listen-addr".to_string(), frontend_addr.to_string());
    }
    if let Some(compactor_addr) = &opts.compactor_addr {
        compactor_opts
            .inner
            .insert("--listen-addr".to_string(), compactor_addr.to_string());
    }
    if let Some(frontend_node_override_opts) = &opts.frontend_node_override_opts {
        frontend_opts
            .inner
            .extend(frontend_node_override_opts.inner.clone());
    }
    if let Some(meta_node_override_opts) = &opts.meta_node_override_opts {
        meta_opts
            .inner
            .extend(meta_node_override_opts.inner.clone());
    }
    if let Some(compute_node_override_opts) = &opts.compute_node_override_opts {
        compute_opts
            .inner
            .extend(compute_node_override_opts.inner.clone());
    }
    if let Some(compactor_node_override_opts) = &opts.compactor_node_override_opts {
        compactor_opts
            .inner
            .extend(compactor_node_override_opts.inner.clone());
    }
    let frontend_opts = if opts.disable_frontend {
        None
    } else {
        Some(frontend_opts)
    };
    let meta_opts = if opts.disable_meta {
        None
    } else {
        Some(meta_opts)
    };
    let compute_opts = if opts.disable_compute {
        None
    } else {
        Some(compute_opts)
    };
    let compactor_opts = if opts.disable_compactor {
        None
    } else {
        Some(compactor_opts)
    };
    NormalizedSingleNodeOpts {
        frontend_opts,
        meta_opts,
        compute_opts,
        compactor_opts,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawOpts {
    inner: HashMap<String, String>,
}

// FIXME(kwannoel): This is a placeholder implementation
impl Ord for RawOpts {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        Ordering::Equal
    }
}

impl PartialOrd for RawOpts {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<&str> for RawOpts {
    fn from(s: &str) -> Self {
        let mut inner = HashMap::new();
        let args = split(s).unwrap();
        let mut args = args.into_iter().peekable();
        loop {
            let Some(arg) = args.peek() else {
                break;
            };
            assert!(arg.starts_with("--"));
            let key = args.next().unwrap();

            // No next arg, current key must be a flag.
            let Some(arg2) = args.peek() else {
                inner.insert(key, "".to_string());
                break;
            };

            // Next arg is a key, current key must be a flag.
            if arg2.starts_with("--") {
                inner.insert(key, "".to_string());
                continue;
            } else {
                // Next arg must be a value if it's not a key.
                inner.insert(key, args.next().unwrap());
            }
        }
        Self { inner }
    }
}

impl RawOpts {
    fn default_frontend_opts() -> Self {
        let opts = [
            ("--listen-addr", "0.0.0.0:4566"),
            ("--advertise-addr", "0.0.0.0:4566"),
            ("--meta-addr", "http://0.0.0.0:5690"),
            ("--prometheus-listener-addr", "0.0.0.0:1250"),
            ("--health-check-listener-addr", "0.0.0.0:6786"),
        ];
        let inner = opts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self { inner }
    }

    fn default_meta_opts() -> Self {
        let opts = [
            ("--listen-addr", "0.0.0.0:5690"),
            ("--advertise-addr", "0.0.0.0:5690"),
            ("--dashboard-host", "0.0.0.0:5691"),
            ("--prometheus-listener_addr", "0.0.0.0:1250"),
            ("--sql-endpoint", &DEFAULT_SINGLE_NODE_SQL_ENDPOINT.clone()),
            ("--backend", "sql"),
            (
                "--state-store",
                &DEFAULT_SINGLE_NODE_STATE_STORE_URL.clone(),
            ),
            ("--data-directory", "hummock_001"),
        ];
        let inner = opts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self { inner }
    }

    fn default_compute_opts() -> Self {
        let opts = [
            ("--listen-addr", "0.0.0.0:5688"),
            ("--advertise-addr", "0.0.0.0:5688"),
            ("--prometheus-listener-addr", "0.0.0.0:1250"),
            ("--meta-address", "http://0.0.0.0:5690"),
            ("--async-stack-trace", "release-verbose"),
        ];
        let inner = opts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self { inner }
    }

    fn default_compactor_opts() -> Self {
        let opts = [
            ("--listen-addr", "0.0.0.0:6660"),
            ("--advertise-addr", "0.0.0.0:6660"),
            ("--prometheus-listener-addr", "0.0.0.0:1250"),
            ("--meta-address", "http://0.0.0.0:5690"),
        ];
        let inner = opts
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self { inner }
    }
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

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_raw_opts_from_str() {
        let str = "--some-opt \"some value\" --some-flag --another-flag --some-opt-2 abc";
        let hash_map = RawOpts::from(str);
        check(hash_map, expect![[r#"
            RawOpts {
                inner: {
                    "--some-opt-2": "abc",
                    "--some-flag": "",
                    "--some-opt": "some value",
                    "--another-flag": "",
                },
            }"#]])
    }
}
