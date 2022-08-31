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

use std::collections::HashMap;
use std::env;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};
use clap::StructOpt;
use risedev::{
    CompactorService, ComputeNodeService, ConfigExpander, FrontendService, HummockInMemoryStrategy,
    MetaNodeService, ServiceConfig,
};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::signal;

async fn load_risedev_config(
    profile: &str,
) -> Result<(Vec<String>, HashMap<String, ServiceConfig>)> {
    let risedev_config = {
        let mut content = String::new();
        File::open("risedev.yml")
            .await?
            .read_to_string(&mut content)
            .await?;
        content
    };
    let risedev_config = ConfigExpander::expand(&risedev_config, profile)?;
    let (steps, services) = ConfigExpander::select(&risedev_config, profile)?;

    Ok((steps, services))
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
    let force_shared_hummock_in_mem = std::env::var("FORCE_SHARED_HUMMOCK_IN_MEM").is_ok();

    // TODO: may allow specifying the config file for the playground.
    let apply_config_file = |cmd: &mut Command| {
        let path = Path::new("src/config/risingwave.toml");
        if path.exists() {
            cmd.arg("--config-path").arg(path);
        }
    };

    let services = match load_risedev_config(&profile).await {
        Ok((steps, services)) => {
            tracing::info!(
                "Launching services from risedev config playground using profile: {}",
                profile
            );
            tracing::info!("steps: {:?}", steps);

            let steps: Vec<_> = steps
                .into_iter()
                .map(|step| services.get(&step).expect("service not found"))
                .collect();

            let compute_node_count = steps
                .iter()
                .filter(|s| matches!(s, ServiceConfig::ComputeNode(_)))
                .count();

            let mut rw_services = vec![];
            for step in steps {
                match step {
                    ServiceConfig::ComputeNode(c) => {
                        let mut command = Command::new("compute-node");
                        ComputeNodeService::apply_command_args(
                            &mut command,
                            c,
                            if force_shared_hummock_in_mem || compute_node_count > 1 {
                                HummockInMemoryStrategy::Shared
                            } else {
                                HummockInMemoryStrategy::Isolated
                            },
                        )?;
                        apply_config_file(&mut command);
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
                        MetaNodeService::apply_command_args(&mut command, c)?;
                        apply_config_file(&mut command);
                        rw_services.push(RisingWaveService::Meta(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    ServiceConfig::Frontend(c) => {
                        let mut command = Command::new("frontend-node");
                        FrontendService::apply_command_args(&mut command, c)?;
                        rw_services.push(RisingWaveService::Frontend(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    ServiceConfig::Compactor(c) => {
                        let mut command = Command::new("compactor");
                        CompactorService::apply_command_args(&mut command, c)?;
                        apply_config_file(&mut command);
                        rw_services.push(RisingWaveService::Compactor(
                            command.get_args().map(ToOwned::to_owned).collect(),
                        ));
                    }
                    _ => {
                        return Err(anyhow!("unsupported service: {:?}", step));
                    }
                }
            }
            rw_services
        }
        Err(e) => {
            tracing::warn!("Failed to load risedev config. All components will be started using the default command line options.\n{}", e);
            vec![
                RisingWaveService::Meta(vec!["--backend".into(), "mem".into()]),
                RisingWaveService::Compute(vec!["--state-store".into(), "hummock+memory".into()]),
                RisingWaveService::Frontend(vec![]),
            ]
        }
    };

    for service in services {
        match service {
            RisingWaveService::Meta(mut opts) => {
                opts.insert(0, "meta-node".into());
                tracing::info!("starting meta-node thread with cli args: {:?}", opts);
                let opts = risingwave_meta::MetaNodeOpts::parse_from(opts);
                tracing::info!("opts: {:#?}", opts);
                let _meta_handle = tokio::spawn(async move {
                    risingwave_meta::start(opts).await;
                    tracing::info!("meta is stopped, shutdown all nodes");
                    // As a playground, it's fine to just kill everything.
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

    sync_point::on("CLUSTER_READY").await;

    // TODO: should we join all handles?
    // Currently, not all services can be shutdown gracefully, just quit on Ctrl-C now.
    signal::ctrl_c().await.unwrap();
    tracing::info!("Ctrl+C received, now exiting");

    Ok(())
}
