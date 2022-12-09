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
use yaml_rust::{Yaml, YamlEmitter, YamlLoader};

use crate::ServiceConfig;

mod dollar_expander;
mod id_expander;
mod provide_expander;
mod use_expander;
use dollar_expander::DollarExpander;
use id_expander::IdExpander;
use provide_expander::ProvideExpander;
use use_expander::UseExpander;

pub struct ConfigExpander;

impl ConfigExpander {
    /// Transforms `risedev.yml` to a fully expanded yaml file.
    ///
    /// * `config` is the full content of `risedev.yml`.
    /// * `profile` is the selected config profile called by `risedev dev <profile>`. It is one of
    ///   the keys in the `risedev` section.
    pub fn expand(config: &str, profile: &str) -> Result<(String, Yaml)> {
        Self::expand_with_extra_info(config, profile, HashMap::new())
    }

    /// Expand user config into full config.
    /// See [`ConfigExpander::expand`] for other information.
    ///
    /// # Arguments
    /// - `profile` is the name of user-specified profile
    /// - `extra_info` is additional variables for variable expansion by [`DollarExpander`].
    ///
    /// # Returns
    /// A pair of config_path and expanded steps (items in `steps` section in YAML)
    pub fn expand_with_extra_info(
        config: &str,
        profile: &str,
        extra_info: HashMap<String, String>,
    ) -> Result<(String, Yaml)> {
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

        let profile_section = risedev_section
            .get(&Yaml::String(profile.to_string()))
            .ok_or_else(|| anyhow!("profile '{}' not found", profile))?;
        let profile_map = profile_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect `risedev` section to be a hashmap"))?;

        let config_path = profile_map
            .get(&Yaml::String("config-path".to_string()))
            .and_then(|s| s.as_str())
            .unwrap_or("src/config/risingwave.toml")
            .to_string();

        let steps = profile_map
            .get(&Yaml::String("steps".to_string()))
            .ok_or_else(|| anyhow!("expect `steps` section"))?
            .clone();

        let steps = UseExpander::new(template_section)?.visit(steps)?;
        let steps = DollarExpander::new(extra_info.clone()).visit(steps)?;
        let steps = IdExpander::new(&steps)?.visit(steps)?;
        let steps = ProvideExpander::new(&steps)?.visit(steps)?;

        Ok((config_path, steps))
    }

    /// Parses the expanded yaml into [`ServiceConfig`]s.
    /// The order is the same as the original array's order.
    pub fn deserialize(expanded_config: &Yaml) -> Result<Vec<ServiceConfig>> {
        let steps = expanded_config
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
                    "pubsub" => ServiceConfig::Pubsub(serde_yaml::from_str(&out_str)?),
                    "redis" => ServiceConfig::Redis(serde_yaml::from_str(&out_str)?),
                    "connector-node" => {
                        ServiceConfig::ConnectorNode(serde_yaml::from_str(&out_str)?)
                    }
                    "zookeeper" => ServiceConfig::ZooKeeper(serde_yaml::from_str(&out_str)?),
                    "redpanda" => ServiceConfig::RedPanda(serde_yaml::from_str(&out_str)?),
                    other => return Err(anyhow!("unsupported use type: {}", other)),
                };
                Ok(result)
            })
            .try_collect()?;

        let mut services = HashMap::new();
        for x in &config {
            let id = x.id().to_string();
            if services.insert(id.clone(), x).is_some() {
                return Err(anyhow!("duplicate id: {}", id));
            }
        }
        Ok(config)
    }
}
