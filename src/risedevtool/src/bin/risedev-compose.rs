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
use std::os::unix::prelude::PermissionsExt;
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
    directory: String,

    #[clap(default_value = "compose")]
    profile: String,

    /// Whether to generate deployment script. If enabled, network mode will be set to host, a
    /// deploy.sh will be generated.
    #[clap(long)]
    deploy: bool,

    /// Force override RisingWave image. Normally it will be used along with deploy compose mode.
    #[clap(long)]
    override_risingwave_image: Option<String>,
}

fn load_docker_image_config(
    risedev_config: &str,
    override_risingwave_image: &Option<String>,
) -> Result<DockerImageConfig> {
    #[derive(Deserialize)]
    struct ConfigInRiseDev {
        compose: DockerImageConfig,
    }
    let mut config: ConfigInRiseDev = serde_yaml::from_str(risedev_config)?;
    if let Some(override_risingwave_image) = override_risingwave_image {
        config.compose.risingwave = override_risingwave_image.clone();
    }
    Ok(config.compose)
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct Ec2Instance {
    id: String,
    dns_host: String,
    public_ip: String,
}

fn main() -> Result<()> {
    let opts = RiseDevComposeOpts::parse();

    let risedev_config = {
        let mut content = String::new();
        File::open("risedev.yml")?.read_to_string(&mut content)?;
        content
    };

    let compose_config = ComposeConfig {
        image: load_docker_image_config(&risedev_config, &opts.override_risingwave_image)?,
        config_directory: opts.directory.clone(),
    };

    let (risedev_config, ec2_instances) = if opts.deploy {
        let risedev_compose_config = {
            let mut content = String::new();
            File::open("risedev-compose.yml")?.read_to_string(&mut content)?;
            content
        };
        let ec2_instances: Vec<Ec2Instance> = serde_yaml::from_str(&risedev_compose_config)?;

        (
            ConfigExpander::expand_with_extra_info(
                &risedev_config,
                ec2_instances
                    .iter()
                    .map(|i| (format!("dns-host:{}", i.id), i.dns_host.clone()))
                    .collect(),
            )?,
            Some(ec2_instances),
        )
    } else {
        (ConfigExpander::expand(&risedev_config)?, None)
    };

    let (steps, services) = ConfigExpander::select(&risedev_config, &opts.profile)?;

    let mut compose_services: BTreeMap<String, BTreeMap<String, ComposeService>> = BTreeMap::new();
    let mut service_on_node: BTreeMap<String, String> = BTreeMap::new();
    let mut volumes = BTreeMap::new();

    for step in &steps {
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

            fs::write(
                Path::new(&opts.directory).join(format!("{}.yml", node)),
                yaml,
            )?;
        }
        let ec2_instances = ec2_instances.unwrap();
        let shell_script = {
            use std::fmt::Write;
            let mut x = String::new();
            writeln!(x, "#!/bin/bash -e")?;
            writeln!(x)?;
            writeln!(
                x,
                r#"
DIR="$( cd "$( dirname "${{BASH_SOURCE[0]}}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR""#
            )?;
            writeln!(x)?;
            writeln!(x, "# --- Sync Config ---")?;
            for instance in &ec2_instances {
                let host = &instance.dns_host;
                let public_ip = &instance.public_ip;
                let id = &instance.id;
                let base_folder = "~/risingwave-deploy";
                writeln!(x, r#"echo "{id}: $(tput setaf 2)sync config$(tput sgr0)""#,)?;
                writeln!(
                    x,
                    "rsync -avH -e \"ssh -oStrictHostKeyChecking=no\" ./ ubuntu@{public_ip}:{base_folder}",
                )?;
                writeln!(
                    x,
                    "scp -oStrictHostKeyChecking=no ./{host}.yml ubuntu@{public_ip}:{base_folder}/docker-compose.yaml"
                )?;
            }
            writeln!(x)?;
            writeln!(x, "# --- Tear Down Services ---")?;
            for instance in &ec2_instances {
                let id = &instance.id;
                writeln!(
                    x,
                    r#"echo "{id}: $(tput setaf 2)stop services and pull latest image$(tput sgr0)""#,
                )?;
                let public_ip = &instance.public_ip;
                let base_folder = "~/risingwave-deploy";
                writeln!(x, "ssh -oStrictHostKeyChecking=no ubuntu@{public_ip} -T \"bash -c 'cd {base_folder} && docker compose stop -t 0 && docker compose down -v && docker pull {}'\"",
                    compose_config.image.risingwave
                )?;
            }
            writeln!(x)?;
            writeln!(x, "# --- Start Services ---")?;
            for step in &steps {
                let dns_host = service_on_node.get(step).unwrap();
                let instance = ec2_instances
                    .iter()
                    .find(|ec2| &ec2.dns_host == dns_host)
                    .unwrap();
                let id = &instance.id;
                writeln!(
                    x,
                    r#"echo "{id}: $(tput setaf 2)start service {step}$(tput sgr0)""#,
                )?;
                let public_ip = &instance.public_ip;
                let base_folder = "~/risingwave-deploy";
                writeln!(x, "ssh -oStrictHostKeyChecking=no ubuntu@{public_ip} -T \"bash -c 'cd {base_folder} && docker compose up -d {step}'\"")?;
            }
            writeln!(x)?;
            writeln!(x, "# --- Check Services ---")?;
            for instance in &ec2_instances {
                let id = &instance.id;
                writeln!(x, r#"echo "{id}: $(tput setaf 2)check status$(tput sgr0)""#,)?;
                let public_ip = &instance.public_ip;
                let base_folder = "~/risingwave-deploy";
                writeln!(x, "ssh -oStrictHostKeyChecking=no ubuntu@{public_ip} -T \"bash -c 'cd {base_folder} && docker compose ps'\"")?;
            }
            x
        };
        let deploy_sh = Path::new(&opts.directory).join("deploy.sh");
        fs::write(&deploy_sh, &shell_script)?;
        let mut perms = fs::metadata(&deploy_sh)?.permissions();
        perms.set_mode(perms.mode() | 0o755);
        fs::set_permissions(&deploy_sh, perms)?;
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
