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

use anyhow::Result;
use clap::Parser;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use shell_words::split;
use tokio::signal;

use crate::common::osstrs;

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
pub struct StandaloneOpts {
    /// Compute node options
    /// If missing, compute node won't start
    #[clap(short, long, env = "RW_STANDALONE_COMPUTE_OPTS")]
    compute_opts: Option<String>,

    #[clap(short, long, env = "RW_STANDALONE_META_OPTS")]
    /// Meta node options
    /// If missing, meta node won't start
    meta_opts: Option<String>,

    #[clap(short, long, env = "RW_STANDALONE_FRONTEND_OPTS")]
    /// Frontend node options
    /// If missing, frontend node won't start
    frontend_opts: Option<String>,

    #[clap(long, env = "RW_STANDALONE_COMPACTOR_OPTS")]
    /// Compactor node options
    /// If missing compactor node won't start
    compactor_opts: Option<String>,

    #[clap(long, env = "RW_STANDALONE_PROMETHEUS_LISTENER_ADDR")]
    /// Prometheus listener address
    /// If present, it will override prometheus listener address for
    /// Frontend, Compute and Compactor nodes
    prometheus_listener_addr: Option<String>,

    #[clap(long, env = "RW_STANDALONE_CONFIG_PATH")]
    /// Path to the config file
    /// If present, it will override config path for
    /// Frontend, Compute and Compactor nodes
    config_path: Option<String>,
}

#[derive(Debug)]
pub struct ParsedStandaloneOpts {
    pub meta_opts: Option<MetaNodeOpts>,
    pub compute_opts: Option<ComputeNodeOpts>,
    pub frontend_opts: Option<FrontendOpts>,
    pub compactor_opts: Option<CompactorOpts>,
}

fn parse_opt_args(opts: &StandaloneOpts) -> ParsedStandaloneOpts {
    let meta_opts = opts.meta_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "meta-node".into());
        s
    });
    let mut meta_opts = meta_opts.map(|o| MetaNodeOpts::parse_from(osstrs(o)));

    let compute_opts = opts.compute_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compute-node".into());
        s
    });
    let mut compute_opts = compute_opts.map(|o| ComputeNodeOpts::parse_from(osstrs(o)));

    let frontend_opts = opts.frontend_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "frontend-node".into());
        s
    });
    let mut frontend_opts = frontend_opts.map(|o| FrontendOpts::parse_from(osstrs(o)));

    let compactor_opts = opts.compactor_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compactor-node".into());
        s
    });
    let mut compactor_opts = compactor_opts.map(|o| CompactorOpts::parse_from(osstrs(o)));

    if let Some(config_path) = opts.config_path.as_ref() {
        if let Some(meta_opts) = meta_opts.as_mut() {
            meta_opts.config_path = config_path.clone();
        }
        if let Some(compute_opts) = compute_opts.as_mut() {
            compute_opts.config_path = config_path.clone();
        }
        if let Some(frontend_opts) = frontend_opts.as_mut() {
            frontend_opts.config_path = config_path.clone();
        }
        if let Some(compactor_opts) = compactor_opts.as_mut() {
            compactor_opts.config_path = config_path.clone();
        }
    }
    if let Some(prometheus_listener_addr) = opts.prometheus_listener_addr.as_ref() {
        if let Some(compute_opts) = compute_opts.as_mut() {
            compute_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        }
        if let Some(frontend_opts) = frontend_opts.as_mut() {
            frontend_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        }
        if let Some(compactor_opts) = compactor_opts.as_mut() {
            compactor_opts.prometheus_listener_addr = prometheus_listener_addr.clone();
        }
        if let Some(meta_opts) = meta_opts.as_mut() {
            meta_opts.prometheus_host = Some(prometheus_listener_addr.clone());
        }
    }
    ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }
}

pub async fn standalone(opts: StandaloneOpts) -> Result<()> {
    tracing::info!("launching Risingwave in standalone mode");

    let ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    } = parse_opt_args(&opts);

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
    eprintln!("RisingWave standalone mode is ready.");

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
    fn test_parse_opt_args() {
        // Test parsing into standalone-level opts.
        let raw_opts = "
--compute-opts=--listen-addr 127.0.0.1:8000 --total-memory-bytes 34359738368 --parallelism 10
--meta-opts=--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\" --listen-addr 127.0.0.1:8001 --etcd-password 1234
--frontend-opts=--config-path=src/config/original.toml
--prometheus-listener-addr=127.0.0.1:1234
--config-path=src/config/test.toml
";
        let actual = StandaloneOpts::parse_from(raw_opts.lines());
        let opts = StandaloneOpts {
            compute_opts: Some("--listen-addr 127.0.0.1:8000 --total-memory-bytes 34359738368 --parallelism 10".into()),
            meta_opts: Some("--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\" --listen-addr 127.0.0.1:8001 --etcd-password 1234".into()),
            frontend_opts: Some("--config-path=src/config/original.toml".into()),
            compactor_opts: None,
            prometheus_listener_addr: Some("127.0.0.1:1234".into()),
            config_path: Some("src/config/test.toml".into()),
        };
        assert_eq!(actual, opts);

        // Test parsing into node-level opts.
        let actual = parse_opt_args(&opts);
        check(
            actual,
            expect![[r#"
                ParsedStandaloneOpts {
                    meta_opts: Some(
                        MetaNodeOpts {
                            vpc_id: None,
                            security_group_id: None,
                            listen_addr: "127.0.0.1:8001",
                            advertise_addr: "127.0.0.1:9999",
                            dashboard_host: None,
                            prometheus_host: Some(
                                "127.0.0.1:1234",
                            ),
                            etcd_endpoints: "",
                            etcd_auth: false,
                            etcd_username: "",
                            etcd_password: [REDACTED alloc::string::String],
                            sql_endpoint: None,
                            dashboard_ui_path: None,
                            prometheus_endpoint: None,
                            connector_rpc_endpoint: None,
                            privatelink_endpoint_default_tags: None,
                            config_path: "src/config/test.toml",
                            backend: None,
                            barrier_interval_ms: None,
                            sstable_size_mb: None,
                            block_size_kb: None,
                            bloom_false_positive: None,
                            state_store: None,
                            data_directory: Some(
                                "some path with spaces",
                            ),
                            do_not_config_object_storage_lifecycle: None,
                            backup_storage_url: None,
                            backup_storage_directory: None,
                            object_store_streaming_read_timeout_ms: None,
                            object_store_streaming_upload_timeout_ms: None,
                            object_store_upload_timeout_ms: None,
                            object_store_read_timeout_ms: None,
                            heap_profiling_dir: None,
                        },
                    ),
                    compute_opts: Some(
                        ComputeNodeOpts {
                            listen_addr: "127.0.0.1:8000",
                            advertise_addr: None,
                            prometheus_listener_addr: "127.0.0.1:1234",
                            meta_address: "http://127.0.0.1:5690",
                            connector_rpc_endpoint: None,
                            connector_rpc_sink_payload_format: None,
                            config_path: "src/config/test.toml",
                            total_memory_bytes: 34359738368,
                            parallelism: 10,
                            role: Both,
                            metrics_level: None,
                            data_file_cache_dir: None,
                            meta_file_cache_dir: None,
                            async_stack_trace: None,
                            heap_profiling_dir: None,
                            object_store_streaming_read_timeout_ms: None,
                            object_store_streaming_upload_timeout_ms: None,
                            object_store_upload_timeout_ms: None,
                            object_store_read_timeout_ms: None,
                        },
                    ),
                    frontend_opts: Some(
                        FrontendOpts {
                            listen_addr: "127.0.0.1:4566",
                            advertise_addr: None,
                            port: None,
                            meta_addr: "http://127.0.0.1:5690",
                            prometheus_listener_addr: "127.0.0.1:1234",
                            health_check_listener_addr: "127.0.0.1:6786",
                            config_path: "src/config/test.toml",
                            metrics_level: None,
                            enable_barrier_read: None,
                        },
                    ),
                    compactor_opts: None,
                }"#]],
        );
    }
}
