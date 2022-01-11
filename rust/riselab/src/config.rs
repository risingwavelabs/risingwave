use std::collections::HashMap;

use anyhow::{anyhow, Result};
use itertools::Itertools;
use yaml_rust::{yaml, Yaml, YamlEmitter, YamlLoader};

use self::dollar_expander::DollarExpander;
use self::id_expander::IdExpander;
use self::use_expander::UseExpander;

mod dollar_expander;
mod id_expander;
pub mod service_config;
mod use_expander;
pub use service_config::*;
mod provide_expander;
pub use provide_expander::*;

pub struct ConfigExpander;

impl ConfigExpander {
    pub fn expand(config: &str) -> Result<Yaml> {
        let mut config = YamlLoader::load_from_str(config)?;
        if config.len() != 1 {
            return Err(anyhow!("expect yaml config to have only one section"));
        }
        let config = config.remove(0);
        let global_config = config
            .as_hash()
            .ok_or_else(|| anyhow!("expect config to be a hashmap"))?;
        let riselab_section = global_config
            .get(&Yaml::String("riselab".to_string()))
            .ok_or_else(|| anyhow!("expect `riselab` section"))?;
        let riselab_section = riselab_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect `riselab` section to be a hashmap"))?;
        let template_section = global_config
            .get(&Yaml::String("template".to_string()))
            .ok_or_else(|| anyhow!("expect `riselab` section"))?;
        let riselab_section: Vec<(Yaml, Yaml)> = riselab_section
            .iter()
            .map(|(k, v)| {
                let k = k
                    .as_str()
                    .ok_or_else(|| anyhow!("expect `riselab` section to use string key"))?;
                let mut use_expander = UseExpander::new(template_section)?;
                let v = use_expander.visit(v.clone())?;
                let mut dollar_expander = DollarExpander::new();
                let v = dollar_expander.visit(v)?;
                let mut id_expander = IdExpander::new(&v)?;
                let v = id_expander.visit(v)?;
                let mut provide_expander = ProvideExpander::new(&v)?;
                let v = provide_expander.visit(v)?;
                Ok::<_, anyhow::Error>((Yaml::String(k.to_string()), v))
            })
            .try_collect()?;
        let riselab_section = yaml::Hash::from_iter(riselab_section.into_iter());

        Ok(Yaml::Hash(riselab_section))
    }

    pub fn select(
        riselab_section: &Yaml,
        name: &str,
    ) -> Result<(Vec<String>, HashMap<String, ServiceConfig>)> {
        let riselab_section = riselab_section
            .as_hash()
            .ok_or_else(|| anyhow!("expect riselab section to be a hashmap"))?;
        let scene = riselab_section
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
                    "frontend" => ServiceConfig::Frontend(serde_yaml::from_str(&out_str)?),
                    "compute-node" => ServiceConfig::ComputeNode(serde_yaml::from_str(&out_str)?),
                    "meta-node" => ServiceConfig::MetaNode(serde_yaml::from_str(&out_str)?),
                    "prometheus" => ServiceConfig::Prometheus(serde_yaml::from_str(&out_str)?),
                    "grafana" => ServiceConfig::Grafana(serde_yaml::from_str(&out_str)?),
                    "jaeger" => ServiceConfig::Jaeger(serde_yaml::from_str(&out_str)?),
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
