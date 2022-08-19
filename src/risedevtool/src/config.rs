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

use anyhow::{anyhow, Result};
use itertools::Itertools;
use yaml_rust::{yaml, Yaml, YamlEmitter, YamlLoader};

use self::dollar_expander::DollarExpander;
use self::id_expander::IdExpander;
use self::use_expander::UseExpander;
use crate::ServiceConfig;

mod dollar_expander;
mod id_expander;
mod provide_expander;
mod use_expander;
pub use provide_expander::*;

pub struct ConfigExpander;

impl ConfigExpander {
    pub fn expand(config: &str, section: &str) -> Result<Yaml> {
        Self::expand_with_extra_info(config, section, HashMap::new())
    }

    pub fn expand_with_extra_info(
        config: &str,
        section: &str,
        extra_info: HashMap<String, String>,
    ) -> Result<Yaml> {
        let [config]: [_; 1] = YamlLoader::load_from_str(config)?
            .try_into()
            .map_err(|_| anyhow!("expect yaml config to have only one section"))?;

        let global_config = config
            .as_hash()
            .ok_or_else(|| anyhow!("expect config to be a hashmap"))?;
        let risedev_section = global_config
            .get(&Yaml::String("risedev".to_string()))
            .ok_or_else(|| anyhow!("expect `risedev` section"))?;
        let risedev_section = risedev_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect `risedev` section to be a hashmap"))?;
        let template_section = global_config
            .get(&Yaml::String("template".to_string()))
            .ok_or_else(|| anyhow!("expect `risedev` section"))?;
        let risedev_section: Vec<(Yaml, Yaml)> = risedev_section
            .iter()
            .filter(|(k, _)| k == &&Yaml::String(section.to_string()))
            .map(|(k, v)| {
                let k = k
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `risedev` section to use string key"))?;
                let mut use_expander = UseExpander::new(template_section)?;
                let v = use_expander.visit(v.clone())?;
                let mut dollar_expander = DollarExpander::new(extra_info.clone());
                let v = dollar_expander.visit(v)?;
                let mut id_expander = IdExpander::new(&v)?;
                let v = id_expander.visit(v)?;
                let mut provide_expander = ProvideExpander::new(&v)?;
                let v = provide_expander.visit(v)?;
                Ok::<_, anyhow::Error>((Yaml::String(k.to_string()), v))
            })
            .try_collect()?;
        let risedev_section = yaml::Hash::from_iter(risedev_section.into_iter());

        Ok(Yaml::Hash(risedev_section))
    }

    pub fn select(
        risedev_section: &Yaml,
        name: &str,
    ) -> Result<(Vec<String>, HashMap<String, ServiceConfig>)> {
        let risedev_section = risedev_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect risedev section to be a hashmap"))?;
        let scene = risedev_section
            .get(&Yaml::String(name.to_string()))
            .ok_or_else(|| anyhow!("{} not found", name))?;
        let steps = scene
            .as_vec()
            .ok_or_else(|| anyhow!("expect steps to be an array"))?;
        let config: Vec<ServiceConfig> = steps
            .iter()
            .map(|step| {
                let use_type = step
                    .as_hash()
                    .ok_or_else(|| anyhow!("expect step to be a hashmap"))?;
                let use_type = use_type
                    .get(&Yaml::String("use".to_string()))
                    .ok_or_else(|| anyhow!("expect `use` in step"))?;
                let use_type = use_type
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `use` to be a string"))?
                    .to_string();
                let mut out_str = String::new();
                let mut emitter = YamlEmitter::new(&mut out_str);
                emitter.dump(step)?;
                let result = match use_type.as_str() {
                    "minio" => ServiceConfig::Minio(serde_yaml::from_str(&out_str)?),
                    "etcd" => ServiceConfig::Etcd(serde_yaml::from_str(&out_str)?),
                    "frontend" => ServiceConfig::Frontend(serde_yaml::from_str(&out_str)?),
                    "compactor" => ServiceConfig::Compactor(serde_yaml::from_str(&out_str)?),
                    "compute-node" => ServiceConfig::ComputeNode(serde_yaml::from_str(&out_str)?),
                    "meta-node" => ServiceConfig::MetaNode(serde_yaml::from_str(&out_str)?),
                    "prometheus" => ServiceConfig::Prometheus(serde_yaml::from_str(&out_str)?),
                    "grafana" => ServiceConfig::Grafana(serde_yaml::from_str(&out_str)?),
                    "jaeger" => ServiceConfig::Jaeger(serde_yaml::from_str(&out_str)?),
                    "aws-s3" => ServiceConfig::AwsS3(serde_yaml::from_str(&out_str)?),
                    "kafka" => ServiceConfig::Kafka(serde_yaml::from_str(&out_str)?),
                    "redis" => ServiceConfig::Redis(serde_yaml::from_str(&out_str)?),
                    "zookeeper" => ServiceConfig::ZooKeeper(serde_yaml::from_str(&out_str)?),
                    "redpanda" => ServiceConfig::RedPanda(serde_yaml::from_str(&out_str)?),
                    other => return Err(anyhow!("unsupported use type: {}", other)),
                };
                Ok(result)
            })
            .try_collect()?;

        Ok((
            config.iter().map(|x| x.id().to_string()).collect_vec(),
            config
                .into_iter()
                .map(|x| (x.id().to_string(), x))
                .collect(),
        ))
    }
}
