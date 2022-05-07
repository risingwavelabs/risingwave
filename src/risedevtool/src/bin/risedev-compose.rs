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
    Compose, ComposeFile, ComposeVolume, ConfigExpander, DockerImageConfig, ServiceConfig,
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

    let docker_image_config = load_docker_image_config(&risedev_config)?;
    let risedev_config = ConfigExpander::expand(&risedev_config)?;
    let (steps, services) = ConfigExpander::select(&risedev_config, &opts.profile)?;

    let mut compose_services = BTreeMap::new();
    let mut volumes = BTreeMap::new();

    for step in steps.iter() {
        let service = services.get(step).unwrap();
        let mut compose = match service {
            ServiceConfig::Minio(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                c.compose(&docker_image_config)?
            }
            ServiceConfig::Etcd(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Prometheus(c) => {
                volumes.insert(c.id.clone(), ComposeVolume::default());
                c.compose(&docker_image_config)?
            }
            ServiceConfig::ComputeNode(c) => c.compose(&docker_image_config)?,
            ServiceConfig::MetaNode(c) => c.compose(&docker_image_config)?,
            ServiceConfig::Frontend(_) => return Err(anyhow!("not supported")),
            ServiceConfig::FrontendV2(c) => c.compose(&docker_image_config)?,
            ServiceConfig::Compactor(c) => c.compose(&docker_image_config)?,
            ServiceConfig::Grafana(_) => {
                return Err(anyhow!("not supported, please run Grafana outside cluster"))
            }
            ServiceConfig::Jaeger(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Kafka(_) => {
                return Err(anyhow!("not supported, please use redpanda instead"))
            }
            ServiceConfig::ZooKeeper(_) => {
                return Err(anyhow!("not supported, please use redpanda instead"))
            }
            ServiceConfig::AwsS3(_) => continue,
            ServiceConfig::RedPanda(c) => c.compose(&docker_image_config)?,
        };
        compose.container_name = service.id().to_string();
        compose_services.insert(step.to_string(), compose);
    }

    let compose_file = ComposeFile {
        version: "3".into(),
        services: compose_services,
        volumes,
        name: format!("risingwave-{}", opts.profile),
    };

    let yaml = serde_yaml::to_string(&compose_file)?;

    if let Some(directory) = opts.directory {
        fs::write(Path::new(&directory).join("docker-compose.yml"), yaml)?;
    } else {
        println!("{}", yaml);
    }

    Ok(())
}
