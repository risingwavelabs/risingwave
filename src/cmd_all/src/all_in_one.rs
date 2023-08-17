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

use std::env;
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::sync::LazyLock;

use anyhow::Result;
use clap::Parser;
use tempfile::TempPath;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::signal;

pub enum RisingWaveService {
    Compute(Vec<OsString>),
    Meta(Vec<OsString>),
    Frontend(Vec<OsString>),
    Compactor(Vec<OsString>),
    ConnectorNode(Vec<OsString>),
}

impl RisingWaveService {
    /// Extend additional arguments to the service.
    fn extend_args(&mut self, args: &[&str]) {
        match self {
            RisingWaveService::Compute(args0)
            | RisingWaveService::Meta(args0)
            | RisingWaveService::Frontend(args0)
            | RisingWaveService::Compactor(args0)
            | RisingWaveService::ConnectorNode(args0) => {
                args0.extend(args.iter().map(|s| s.into()))
            }
        }
    }
}

const IDLE_EXIT_SECONDS: u64 = 1800;

/// Embed the config file and create a temporary file at runtime.
static CONFIG_PATH_WITH_IDLE_EXIT: LazyLock<TempPath> = LazyLock::new(|| {
    let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
    write!(
        file,
        "[meta]
disable_recovery = true
dangerous_max_idle_secs = {IDLE_EXIT_SECONDS}
max_heartbeat_interval_secs = 600",
    )
    .expect("failed to write config file");
    file.into_temp_path()
});

fn get_services() -> Vec<RisingWaveService> {
    let mut services =
    vec![
        RisingWaveService::Meta(osstrs([
            "--dashboard-host",
            "0.0.0.0:5691",
            "--state-store",
            "hummock+memory",
            "--data-directory",
            "hummock_001",
            "--advertise-addr",
            "127.0.0.1:5690",
            "--connector-rpc-endpoint",
            "127.0.0.1:50051",
        ])),
        RisingWaveService::Compute(osstrs(["--connector-rpc-endpoint", "127.0.0.1:50051"])),
        RisingWaveService::Frontend(osstrs([])),
        RisingWaveService::ConnectorNode(osstrs([])),
    ];
    services
}

/// TODO: Refactor into utils.
fn osstrs<const N: usize>(s: [&str; N]) -> Vec<OsString> {
    s.iter().map(OsString::from).collect()
}

pub async fn all_in_one() -> Result<()> {
    tracing::info!("launching Risingwave in all-in-one mode");

    let services = get_services();

    for service in services {
        match service {
            RisingWaveService::Meta(mut opts) => {
                opts.insert(0, "meta-node".into());
                tracing::info!("starting meta-node thread with cli args: {:?}", opts);
                let opts = risingwave_meta::MetaNodeOpts::parse_from(opts);
                let _meta_handle = tokio::spawn(async move {
                    risingwave_meta::start(opts).await;
                    tracing::warn!("meta is stopped, shutdown all nodes");
                });
                // wait for the service to be ready
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            RisingWaveService::Compute(mut opts) => {
                opts.insert(0, "compute-node".into());
                tracing::info!("starting compute-node thread with cli args: {:?}", opts);
                let opts = risingwave_compute::ComputeNodeOpts::parse_from(opts);
                let _compute_handle = tokio::spawn(async move {
                    risingwave_compute::start(opts, prometheus::Registry::new()).await
                });
            }
            RisingWaveService::Frontend(mut opts) => {
                opts.insert(0, "frontend-node".into());
                tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
                let opts = risingwave_frontend::FrontendOpts::parse_from(opts);
                let _frontend_handle =
                    tokio::spawn(async move { risingwave_frontend::start(opts).await });
            }
            RisingWaveService::Compactor(mut opts) => {
                opts.insert(0, "compactor".into());
                tracing::info!("starting compactor thread with cli args: {:?}", opts);
                let opts = risingwave_compactor::CompactorOpts::parse_from(opts);
                let _compactor_handle =
                    tokio::spawn(async move { risingwave_compactor::start(opts).await });
            }
            RisingWaveService::ConnectorNode(_) => {
                panic!("Connector node unsupported in Risingwave all-in-one mode.");
            }
        }
    }

    // wait for log messages to be flushed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    eprintln!("-------------------------------");
    eprintln!("RisingWave all-in-one mode is ready.");

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    // TODO(kwannoel): Why can't be shutdown gracefully? Is it that the service just does not support it?
    signal::ctrl_c().await.unwrap();
    tracing::info!("Ctrl+C received, now exiting");

    Ok(())
}
