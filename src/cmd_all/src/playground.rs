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

fn get_services(profile: &str) -> (Vec<RisingWaveService>, bool) {
    let mut services = match profile {
        "playground" => vec![
            RisingWaveService::Meta(osstrs([
                "--dashboard-host",
                "0.0.0.0:5691",
                "--state-store",
                "hummock+memory",
                "--connector-rpc-endpoint",
                "127.0.0.1:50051",
            ])),
            RisingWaveService::Compute(osstrs(["--connector-rpc-endpoint", "127.0.0.1:50051"])),
            RisingWaveService::Frontend(osstrs([])),
            RisingWaveService::ConnectorNode(osstrs([])),
        ],
        "playground-3cn" => vec![
            RisingWaveService::Meta(osstrs([
                "--dashboard-host",
                "0.0.0.0:5691",
                "--state-store",
                "hummock+memory-shared",
            ])),
            RisingWaveService::Compute(osstrs([
                "--listen-addr",
                "127.0.0.1:5687",
                "--parallelism",
                "4",
            ])),
            RisingWaveService::Compute(osstrs([
                "--listen-addr",
                "127.0.0.1:5688",
                "--parallelism",
                "4",
            ])),
            RisingWaveService::Compute(osstrs([
                "--listen-addr",
                "127.0.0.1:5689",
                "--parallelism",
                "4",
            ])),
            RisingWaveService::Frontend(osstrs([])),
        ],
        "online-docker-playground" | "docker-playground" => {
            vec![
                RisingWaveService::Meta(osstrs([
                    "--listen-addr",
                    "0.0.0.0:5690",
                    "--advertise-addr",
                    "127.0.0.1:5690",
                    "--dashboard-host",
                    "0.0.0.0:5691",
                    "--state-store",
                    "hummock+memory",
                    "--connector-rpc-endpoint",
                    "127.0.0.1:50051",
                ])),
                RisingWaveService::Compute(osstrs([
                    "--listen-addr",
                    "0.0.0.0:5688",
                    "--advertise-addr",
                    "127.0.0.1:5688",
                    "--connector-rpc-endpoint",
                    "127.0.0.1:50051",
                ])),
                RisingWaveService::Frontend(osstrs([
                    "--listen-addr",
                    "0.0.0.0:4566",
                    "--advertise-addr",
                    "127.0.0.1:4566",
                ])),
                RisingWaveService::ConnectorNode(osstrs([])),
            ]
        }
        _ => {
            tracing::warn!("Unknown playground profile. All components will be started using the default command line options.");
            return get_services("playground");
        }
    };
    let idle_exit = profile != "docker-playground";
    if idle_exit {
        services.iter_mut().for_each(|s| {
            s.extend_args(&[
                "--config-path",
                &CONFIG_PATH_WITH_IDLE_EXIT.as_os_str().to_string_lossy(),
            ])
        })
    }
    (services, idle_exit)
}

fn osstrs<const N: usize>(s: [&str; N]) -> Vec<OsString> {
    s.iter().map(OsString::from).collect()
}

pub async fn playground() -> Result<()> {
    tracing::info!("launching playground");

    let profile = if let Ok(profile) = std::env::var("PLAYGROUND_PROFILE") {
        profile.to_string()
    } else {
        "playground".to_string()
    };

    let (services, idle_exit) = get_services(&profile);

    for service in services {
        match service {
            RisingWaveService::Meta(mut opts) => {
                opts.insert(0, "meta-node".into());
                tracing::info!("starting meta-node thread with cli args: {:?}", opts);
                let opts = risingwave_meta::MetaNodeOpts::parse_from(opts);
                let _meta_handle = tokio::spawn(async move {
                    risingwave_meta::start(opts).await;
                    tracing::warn!("meta is stopped, shutdown all nodes");
                    // As a playground, it's fine to just kill everything.
                    if idle_exit {
                        eprintln!("{}",
                        console::style(format_args!(
                                "RisingWave playground exited after being idle for {IDLE_EXIT_SECONDS} seconds. Bye!"
                            )).bold());
                    }
                    std::process::exit(0);
                });
                // wait for the service to be ready
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            RisingWaveService::Compute(mut opts) => {
                opts.insert(0, "compute-node".into());
                tracing::info!("starting compute-node thread with cli args: {:?}", opts);
                let opts = risingwave_compute::ComputeNodeOpts::parse_from(opts);
                let _compute_handle =
                    tokio::spawn(async move { risingwave_compute::start(opts).await });
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
            // connector node only supports in docker-playground profile
            RisingWaveService::ConnectorNode(_) => {
                let prefix_bin = match profile.as_str() {
                    "docker-playground" | "online-docker-playground" => {
                        "/risingwave/bin".to_string()
                    }
                    "playground" => std::env::var("PREFIX_BIN").unwrap_or_default(),
                    _ => "".to_string(),
                };
                let cmd_path = Path::new(&prefix_bin)
                    .join("connector-node")
                    .join("start-service.sh");
                if cmd_path.exists() {
                    tracing::info!("start connector-node with prefix_bin {}", prefix_bin);
                    let mut child = Command::new(cmd_path)
                        .arg("-p")
                        .arg("50051")
                        .stderr(std::process::Stdio::piped())
                        .spawn()?;
                    let stderr = child.stderr.take().unwrap();

                    let _child_handle = tokio::spawn(async move {
                        signal::ctrl_c().await.unwrap();
                        let _ = child.start_kill();
                    });
                    let _stderr_handle = tokio::spawn(async move {
                        let mut reader = BufReader::new(stderr).lines();
                        while let Ok(Some(line)) = reader.next_line().await {
                            tracing::error!(target: "risingwave_connector_node", "{}", line);
                        }
                    });
                } else {
                    tracing::warn!(
                        "Will not start connector node since `{}` does not exist.",
                        cmd_path.display()
                    );
                }
            }
        }
    }

    // wait for log messages to be flushed
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    eprintln!("-------------------------------");
    eprintln!("RisingWave playground is ready.");
    eprint!(
        "* {} RisingWave playground SHOULD NEVER be used in benchmarks and production environment!!!\n  It is fully in-memory",
        console::style("WARNING:").red().bold(),
    );
    if idle_exit {
        eprintln!(
            " and will be automatically stopped after being idle for {}.",
            console::style(format_args!("{IDLE_EXIT_SECONDS}s")).dim()
        );
    } else {
        eprintln!();
    }
    eprintln!(
        "* Use {} instead if you want to start a full cluster.",
        console::style("./risedev d").blue().bold()
    );
    eprintln!(
        "* Run {} in a different terminal to start Postgres interactive shell.",
        console::style(format_args!("psql -h localhost -p 4566 -d dev -U root"))
            .blue()
            .bold()
    );
    eprintln!("-------------------------------");

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    signal::ctrl_c().await.unwrap();
    tracing::info!("Ctrl+C received, now exiting");

    Ok(())
}
