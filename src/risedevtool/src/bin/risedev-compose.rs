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
use risedev::{
    Compose, ComposeConfig, ComposeFile, ComposeService, ComposeVolume, ConfigExpander,
    DockerImageConfig, ServiceConfig,
};
use serde::Deserialize;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct RiseDevComposeOpts {
    #[clap(short, long)]
    directory: Option<String>,

    #[clap(default_value = "compose")]
    profile: String,

    /// Whether to use `network_mode: host` for compose. If enabled, will generate multiple compose
    /// files based on listen address.
    #[clap(long)]
    host_mode: bool,

    /// Whether to store all configs into a single docker-compose file.
    #[clap(long)]
    single_file: bool,
}

fn load_docker_image_config(risedev_config: &str) -> Result<DockerImageConfig> {
    #[derive(Deserialize)]
    struct ConfigInRiseDev {
        compose: DockerImageConfig,
    }
    let config: ConfigInRiseDev = serde_yaml::from_str(risedev_config)?;
    Ok(config.compose)
}

fn main() -> Result<()> {
    let opts = RiseDevComposeOpts::parse();

    let risedev_config = {
        let mut content = String::new();
        File::open("risedev.yml")?.read_to_string(&mut content)?;
        content
    };

    let compose_config = ComposeConfig {
        image: load_docker_image_config(&risedev_config)?,
        config_directory: if !opts.single_file {
            opts.directory.clone()
        } else {
            None
        },
    };

    let risedev_config = ConfigExpander::expand(&risedev_config)?;
    let (steps, services) = ConfigExpander::select(&risedev_config, &opts.profile)?;

    let mut compose_services: BTreeMap<String, BTreeMap<String, ComposeService>> = BTreeMap::new();
    let mut volumes = BTreeMap::new();

    for step in steps.iter() {
        let service = services.get(step).unwrap();
        let (address, mut compose) = match service {
            ServiceConfig::Minio(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }
            ServiceConfig::Etcd(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Prometheus(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                (c.address.clone(), c.compose(&compose_config)?)
            }

            ServiceConfig::ComputeNode(c) => (c.address.clone(), c.compose(&compose_config)?),
            ServiceConfig::MetaNode(c) => (c.address.clone(), c.compose(&compose_config)?),
            ServiceConfig::FrontendV2(c) => (c.address.clone(), c.compose(&compose_config)?),
            ServiceConfig::Compactor(c) => (c.address.clone(), c.compose(&compose_config)?),
            ServiceConfig::Grafana(c) => {
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
            ServiceConfig::RedPanda(c) => (c.address.clone(), c.compose(&compose_config)?),
        };
        compose.container_name = service.id().to_string();
        if opts.host_mode {
            compose.network_mode = Some("host".into());
            compose.volumes.push("/etc/hosts:/etc/hosts".into());
            compose.depends_on = vec![];
        }
        compose_services
            .entry(address)
            .or_default()
            .insert(step.to_string(), compose);
    }

    if opts.host_mode {
        for (node, services) in compose_services {
            let mut node_volumes = BTreeMap::new();
            services.keys().for_each(|k| {
                if let Some(v) = volumes.get(k) {
                    node_volumes.insert(k.clone(), v.clone());
                }
            });
            let compose_file = ComposeFile {
                version: "3".into(),
                services,
                volumes: node_volumes,
                name: format!("risingwave-{}", opts.profile),
            };

            let yaml = serde_yaml::to_string(&compose_file)?;

            if let Some(ref directory) = opts.directory {
                fs::write(Path::new(directory).join(format!("{}.yml", node)), yaml)?;
            } else {
                return Err(anyhow!("need to specify a directory"));
            }
        }
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

        if let Some(directory) = opts.directory {
            fs::write(Path::new(&directory).join("docker-compose.yml"), yaml)?;
        } else {
            println!("{}", yaml);
        }
    }

    Ok(())
}
