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

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;

use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use risedev::{
    compose_deploy, compute_risectl_env, Compose, ComposeConfig, ComposeDeployConfig, ComposeFile,
    ComposeService, ComposeVolume, ConfigExpander, DockerImageConfig, ServiceConfig,
};
use serde::Deserialize;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct RiseDevComposeOpts {
    #[clap(short, long)]
    directory: String,

    #[clap(default_value = "compose")]
    profile: String,

    /// Whether to generate deployment script. If enabled, network mode will be set to host, a
    /// deploy.sh will be generated.
    #[clap(long)]
    deploy: bool,
}

fn load_docker_image_config(
    risedev_config: &str,
    override_risingwave_image: Option<&String>,
) -> Result<DockerImageConfig> {
    #[derive(Deserialize)]
    struct ConfigInRiseDev {
        compose: DockerImageConfig,
    }
    let mut config: ConfigInRiseDev = serde_yaml::from_str(risedev_config)?;
    if let Some(override_risingwave_image) = override_risingwave_image {
        config.compose.risingwave = override_risingwave_image.to_string();
    }
    Ok(config.compose)
}

fn main() -> Result<()> {
    let opts = RiseDevComposeOpts::parse();

    let risedev_config_content = {
        let mut content = String::new();
        File::open("risedev.yml")?.read_to_string(&mut content)?;
        content
    };

    let (risedev_config, compose_deploy_config) = if opts.deploy {
        let compose_deploy_config = {
            let mut content = String::new();
            File::open("risedev-compose.yml")?.read_to_string(&mut content)?;
            content
        };
        let compose_deploy_config: ComposeDeployConfig =
            serde_yaml::from_str(&compose_deploy_config)?;

        (
            ConfigExpander::expand_with_extra_info(
                &risedev_config_content,
                &opts.profile,
                compose_deploy_config
                    .instances
                    .iter()
                    .map(|i| (format!("dns-host:{}", i.id), i.dns_host.clone()))
                    .chain(
                        compose_deploy_config
                            .risedev_extra_args
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone())),
                    )
                    .collect(),
            )?,
            Some(compose_deploy_config),
        )
    } else {
        (
            ConfigExpander::expand(&risedev_config_content, &opts.profile)?,
            None,
        )
    };

    let compose_config = ComposeConfig {
        image: load_docker_image_config(
            &risedev_config_content,
            compose_deploy_config
                .as_ref()
                .and_then(|x| x.risingwave_image_override.as_ref()),
        )?,
        config_directory: opts.directory.clone(),
    };

    let (steps, services) = ConfigExpander::select(&risedev_config, &opts.profile)?;

    let mut compose_services: BTreeMap<String, BTreeMap<String, ComposeService>> = BTreeMap::new();
    let mut service_on_node: BTreeMap<String, String> = BTreeMap::new();
    let mut volumes = BTreeMap::new();

    let mut log_buffer = String::new();
    use std::fmt::Write;

    for step in &steps {
        let service = services.get(step).unwrap();
        let compose_deploy_config = compose_deploy_config.as_ref();
        let (address, mut compose) = match service {
            ServiceConfig::Minio(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Etcd(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Prometheus(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::ComputeNode(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::MetaNode(c) => {
                if opts.deploy {
                    let public_ip = &compose_deploy_config
                        .unwrap()
                        .lookup_instance_by_host(&c.address)
                        .public_ip;
                    writeln!(
                        log_buffer,
                        "-- Dashboard --\nuse VSCode to forward {} from {}\nor use {}\n",
                        style(format!("{}", c.dashboard_port)).green(),
                        style(format!("ubuntu@{}", public_ip)).green(),
                        style(format!(
                            "ssh -N -L {}:localhost:{} ubuntu@{}",
                            c.dashboard_port, c.dashboard_port, public_ip
                        ))
                        .green()
                    )?;
                }
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Frontend(c) => {
                if opts.deploy {
                    let arg = format!("--frontend {} --frontend-port {}", c.address, c.port);
                    writeln!(
                        log_buffer,
                        "-- Frontend --\nAccess inside cluster: {}\ntpch-bench args: {}\n",
                        style(format!(
                            "psql -d dev -h {} -p {} -U root",
                            c.address, c.port
                        ))
                        .green(),
                        style(&arg).green()
                    )?;
                    fs::write(
                        Path::new(&opts.directory).join("tpch-bench-args-frontend"),
                        arg,
                    )?;
                }
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Compactor(c) => (c.address.clone(), c.compose(&compose_config)?),
            ServiceConfig::Grafana(c) => {
                if opts.deploy {
                    let public_ip = &compose_deploy_config
                        .unwrap()
                        .lookup_instance_by_host(&c.address)
                        .public_ip;
                    writeln!(
                        log_buffer,
                        "-- Grafana --\nuse VSCode to forward {} from {}\nor use {}\n",
                        style(format!("{}", c.port)).green(),
                        style(format!("ubuntu@{}", public_ip)).green(),
                        style(format!(
                            "ssh -N -L {}:localhost:{} ubuntu@{}",
                            c.port, c.port, public_ip
                        ))
                        .green()
                    )?;
                }
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Jaeger(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Kafka(_) => {
                return Err(anyhow!("not supported, please use redpanda instead"))
            }
            ServiceConfig::ZooKeeper(_) => {
                return Err(anyhow!("not supported, please use redpanda instead"))
            }
            ServiceConfig::AwsS3(_) => continue,
            ServiceConfig::RedPanda(c) => {
                if opts.deploy {
                    let arg = format!("--kafka-addr {}:{}", c.address, c.internal_port);
                    writeln!(
                        log_buffer,
                        "-- Redpanda --\ntpch-bench: {}\n",
                        style(&arg).green()
                    )?;
                    fs::write(
                        Path::new(&opts.directory).join("tpch-bench-args-kafka"),
                        arg,
                    )?;
                }
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Redis(_) => return Err(anyhow!("not supported")),
        };
        compose.container_name = service.id().to_string();
        if opts.deploy {
            compose.network_mode = Some("host".into());
            compose.depends_on = vec![];
        }
        compose_services
            .entry(address.clone())
            .or_default()
            .insert(step.to_string(), compose);
        service_on_node.insert(step.clone(), address);
    }

    if opts.deploy {
        for (node, services) in &compose_services {
            let mut node_volumes = BTreeMap::new();
            services.keys().for_each(|k| {
                if let Some(v) = volumes.get(k) {
                    node_volumes.insert(k.clone(), v.clone());
                }
            });
            let compose_file = ComposeFile {
                version: "3".into(),
                services: services.clone(),
                volumes: node_volumes,
                name: format!("risingwave-{}", opts.profile),
            };

            let yaml = serde_yaml::to_string(&compose_file)?;

            let ec2_instance = compose_deploy_config
                .as_ref()
                .unwrap()
                .lookup_instance_by_host(node);
            if ec2_instance.r#type == "meta" {
                let public_ip = &ec2_instance.public_ip;
                writeln!(
                    log_buffer,
                    "-- Meta Node --\nLogin to meta node by {}\nor using VSCode {}\n",
                    style(format!("ssh ubuntu@{}", public_ip)).green(),
                    style(format!(
                        "code --remote ssh-remote+ubuntu@{} <path>",
                        public_ip
                    ))
                    .green()
                )?;
            }

            fs::write(
                Path::new(&opts.directory).join(format!("{}.yml", node)),
                yaml,
            )?;
        }

        let env = compute_risectl_env(&services)?;
        writeln!(log_buffer, "-- risectl --\n{}\n", style(env).green())?;

        compose_deploy(
            Path::new(&opts.directory),
            &steps,
            &compose_deploy_config.as_ref().unwrap().instances,
            &compose_config,
            &service_on_node,
        )?;

        println!("\n{}", log_buffer);

        std::fs::write(
            Path::new(&opts.directory).join("_message.partial.sh"),
            log_buffer,
        )?;
    } else {
        let mut services = BTreeMap::new();
        for (_, s) in compose_services {
            for (k, v) in s {
                services.insert(k, v);
            }
        }
        let compose_file = ComposeFile {
            version: "3".into(),
            services,
            volumes,
            name: format!("risingwave-{}", opts.profile),
        };

        let yaml = serde_yaml::to_string(&compose_file)?;

        fs::write(Path::new(&opts.directory).join("docker-compose.yml"), yaml)?;
    }

    Ok(())
}
