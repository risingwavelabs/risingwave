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

use std::env;
use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use clap::StructOpt;
use risedev::{
    CompactorService, ComputeNodeService, ConfigExpander, FrontendService, HummockInMemoryStrategy,
    MetaNodeService, ServiceConfig,
};
use risingwave_common::config::load_config;
use tokio::signal;

fn load_risedev_config(profile: &str) -> Result<(Option<String>, Vec<ServiceConfig>)> {
    let (config_path, risedev_config) = ConfigExpander::expand(".", profile)?;
    let services = ConfigExpander::deserialize(&risedev_config)?;

    Ok((config_path, services))
}

pub enum RisingWaveService {
    Compute(Vec<OsString>),
    Meta(Vec<OsString>),
    Frontend(Vec<OsString>),
    Compactor(Vec<OsString>),
}

pub async fn playground() -> Result<()> {
    eprintln!("launching playground");

    let profile = if let Ok(profile) = std::env::var("PLAYGROUND_PROFILE") {
        profile.to_string()
    } else {
        "playground".to_string()
    };

    let apply_config_file = |cmd: &mut Command, config_path: Option<&str>| {
        if let Some(c) = config_path {
            println!("config file: {}", c);
            cmd.arg("--config-path").arg(c);
        }
    };

    let services = match load_risedev_config(&profile) {
        Err(e) => {
            tracing::warn!("Failed to load risedev config. All components will be started using the default command line options.\n{}", e);
            vec![
                RisingWaveService::Meta(vec!["--backend".into(), "mem".into()]),
                RisingWaveService::Compute(vec!["--state-store".into(), "hummock+memory".into()]),
                RisingWaveService::Frontend(vec![]),
            ]
        }
        Ok((config_path, services)) => {
            tracing::info!(
                "Launching services from risedev config playground using profile: {}",
                profile
            );

            let compute_node_count = services
                .iter()
                .filter(|s| matches!(s, ServiceConfig::ComputeNode(_)))
                .count();
            let compactor_node_count = services
                .iter()
                .filter(|s| matches!(s, ServiceConfig::CompactorNode(_)))
                .count();
            let hummock_in_memory_strategy = if compute_node_count > 1 || compactor_node_count > 0 {
                HummockInMemoryStrategy::Shared
            } else {
                HummockInMemoryStrategy::Isolated
            };

            let mut rw_services = vec![];
            for service in &services {
                match service {
                    ServiceConfig::ComputeNode(c) => {
                        let mut command = Command::new("compute-node");
                        ComputeNodeService::apply_command_args(
                            &mut command,
                            c,
                            hummock_in_memory_strategy,
                        )?;
                        apply_config_file(&mut command, config_path.as_deref());
                        if c.enable_tiered_cache {
                            let prefix_data = env::var("PREFIX_DATA")?;
                            command.arg("--file-cache-dir").arg(
                                PathBuf::from(prefix_data)
                                    .join("filecache")
                                    .join(c.port.to_string()),
                            );
                        }
                        rw_services.push(RisingWaveService::Compute(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    ServiceConfig::MetaNode(c) => {
                        let mut command = Command::new("meta-node");
                        MetaNodeService::apply_command_args(
                            &mut command,
                            c,
                            hummock_in_memory_strategy,
                        )?;
                        apply_config_file(&mut command, config_path.as_deref());
                        rw_services.push(RisingWaveService::Meta(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    ServiceConfig::Frontend(c) => {
                        let mut command = Command::new("frontend-node");
                        FrontendService::apply_command_args(&mut command, c)?;
                        apply_config_file(&mut command, config_path.as_deref());
                        rw_services.push(RisingWaveService::Frontend(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    ServiceConfig::Compactor(c) => {
                        let mut command = Command::new("compactor");
                        CompactorService::apply_command_args(
                            &mut command,
                            c,
                            hummock_in_memory_strategy,
                        )?;
                        apply_config_file(&mut command, config_path.as_deref());
                        rw_services.push(RisingWaveService::Compactor(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    _ => {
                        return Err(anyhow!("unsupported service: {:?}", service));
                    }
                }
            }
            rw_services
        }
    };

    let mut port = 4566;
    let mut idle = None;

    for service in services {
        match service {
            RisingWaveService::Meta(mut opts) => {
                opts.insert(0, "meta-node".into());
                tracing::info!("starting meta-node thread with cli args: {:?}", opts);
                let opts = risingwave_meta::MetaNodeOpts::parse_from(opts);

                let config = load_config(&opts.config_path);
                idle = config.meta.dangerous_max_idle_secs;

                tracing::info!("opts: {:#?}", opts);
                let _meta_handle = tokio::spawn(async move {
                    risingwave_meta::start(opts).await;
                    tracing::warn!("meta is stopped, shutdown all nodes");
                    // As a playground, it's fine to just kill everything.
                    if let Some(idle) = idle {
                        eprintln!("{}",
                        console::style(format_args!(
                                "RisingWave playground exited after being idle for {idle} seconds. Bye!"
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
                tracing::info!("opts: {:#?}", opts);
                let _compute_handle =
                    tokio::spawn(async move { risingwave_compute::start(opts).await });
            }
            RisingWaveService::Frontend(mut opts) => {
                opts.insert(0, "frontend-node".into());
                tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
                let opts = risingwave_frontend::FrontendOpts::parse_from(opts);
                port = SocketAddr::from_str(&opts.host).unwrap().port();
                tracing::info!("opts: {:#?}", opts);
                let _frontend_handle =
                    tokio::spawn(async move { risingwave_frontend::start(opts).await });
            }
            RisingWaveService::Compactor(mut opts) => {
                opts.insert(0, "compactor".into());
                tracing::info!("starting compactor thread with cli args: {:?}", opts);
                let opts = risingwave_compactor::CompactorOpts::parse_from(opts);
                tracing::info!("opts: {:#?}", opts);
                let _compactor_handle =
                    tokio::spawn(async move { risingwave_compactor::start(opts).await });
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
    if let Some(idle) = idle {
        eprintln!(
            " and will be automatically stopped after being idle for {}.",
            console::style(format_args!("{idle}s")).dim()
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
        console::style(format_args!("psql -h localhost -p {port} -d dev -U root"))
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
