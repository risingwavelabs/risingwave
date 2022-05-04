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
use std::fs::File;
use std::io::Read;

use anyhow::{anyhow, Result};
use risedev::{Compose, ComposeFile, ConfigExpander, ServiceConfig};

fn main() -> Result<()> {
    let risedev_config = {
        let mut content = String::new();
        File::open("risedev.yml")?.read_to_string(&mut content)?;
        content
    };
    let risedev_config = ConfigExpander::expand(&risedev_config)?;
    let (steps, services) = ConfigExpander::select(&risedev_config, "compose")?;

    let mut compose_services = HashMap::new();

    for step in steps.iter() {
        let service = services.get(step).unwrap();
        let compose = match service {
            ServiceConfig::Minio(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Etcd(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Prometheus(_) => return Err(anyhow!("not supported")),
            ServiceConfig::ComputeNode(c) => c.compose()?,
            ServiceConfig::MetaNode(c) => c.compose()?,
            ServiceConfig::Frontend(_) => return Err(anyhow!("not supported")),
            ServiceConfig::FrontendV2(c) => c.compose()?,
            ServiceConfig::Compactor(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Grafana(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Jaeger(_) => return Err(anyhow!("not supported")),
            ServiceConfig::Kafka(_) => return Err(anyhow!("not supported")),
            ServiceConfig::ZooKeeper(_) => return Err(anyhow!("not supported")),
            ServiceConfig::AwsS3(_) => return Err(anyhow!("not supported")),
        };
        compose_services.insert(step.to_string(), compose);
    }

    let compose_file = ComposeFile {
        version: "3".into(),
        services: compose_services,
    };

    let yaml = serde_yaml::to_string(&compose_file)?;

    println!("{}", yaml);

    Ok(())
}
