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

//! Generate docker compose yaml files for risedev components.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::{
    CompactorConfig, CompactorService, ComputeNodeConfig, ComputeNodeService, EtcdConfig,
    EtcdService, FrontendConfig, FrontendService, GrafanaConfig, GrafanaGen,
    HummockInMemoryStrategy, MetaNodeConfig, MetaNodeService, MinioConfig, MinioService,
    PrometheusConfig, PrometheusGen, PrometheusService, RedPandaConfig,
};

#[serde_with::skip_serializing_none]
#[derive(Debug, Clone, Serialize, Default)]
pub struct ComposeService {
    pub image: String,
    pub command: Vec<String>,
    pub expose: Vec<String>,
    pub ports: Vec<String>,
    pub depends_on: Vec<String>,
    pub volumes: Vec<String>,
    pub entrypoint: Option<String>,
    pub environment: BTreeMap<String, String>,
    pub user: Option<String>,
    pub container_name: String,
    pub network_mode: Option<String>,
    pub healthcheck: Option<HealthCheck>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct HealthCheck {
    test: Vec<String>,
    interval: String,
    timeout: String,
    retries: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ComposeFile {
    pub version: String,
    pub services: BTreeMap<String, ComposeService>,
    pub volumes: BTreeMap<String, ComposeVolume>,
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct DockerImageConfig {
    pub risingwave: String,
    pub prometheus: String,
    pub grafana: String,
    pub minio: String,
    pub redpanda: String,
    pub etcd: String,
}

pub struct ComposeConfig {
    /// Docker compose image config
    pub image: DockerImageConfig,

    /// The directory to output all configs. If disabled, all config files will be embedded into
    /// the docker-compose file.
    pub config_directory: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ComposeVolume {
    pub external: bool,
}

/// Generate compose yaml files for a component.
pub trait Compose {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService>;
}

fn get_cmd_args(cmd: &Command, with_argv_0: bool) -> Result<Vec<String>> {
    let mut result = vec![];
    if with_argv_0 {
        result.push(
            cmd.get_program()
                .to_str()
                .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                .to_string(),
        );
    }
    for arg in cmd.get_args() {
        result.push(
            arg.to_str()
                .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                .to_string(),
        );
    }
    Ok(result)
}

fn get_cmd_envs(cmd: &Command) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (k, v) in cmd.get_envs() {
        let k = k
            .to_str()
            .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
            .to_string();
        let v = if let Some(v) = v {
            Some(
                v.to_str()
                    .ok_or_else(|| anyhow!("Cannot convert to UTF-8 string"))?
                    .to_string(),
            )
        } else {
            None
        };
        result.insert(k, v.unwrap_or_default());
    }
    Ok(result)
}

fn health_check_port(port: u16) -> HealthCheck {
    HealthCheck {
        test: vec![
            "CMD".into(),
            "printf".into(),
            "".into(),
            format!("/dev/tcp/127.0.0.1/{}", port),
        ],
        interval: "1s".to_string(),
        timeout: "5s".to_string(),
        retries: 5,
    }
}

impl Compose for ComputeNodeConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("compute-node");
        ComputeNodeService::apply_command_args(
            &mut command,
            self,
            HummockInMemoryStrategy::Disallowed,
        )?;
        command.arg("--config-path").arg("/risingwave.toml");
        if self.enable_tiered_cache {
            command.arg("--file-cache-dir").arg("/filecache");
        }

        std::fs::copy(
            Path::new("src").join("config").join("risingwave.toml"),
            Path::new(&config.config_directory).join("risingwave.toml"),
        )?;

        let command = get_cmd_args(&command, true)?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();
        let provide_minio = self.provide_minio.as_ref().unwrap();

        Ok(ComposeService {
            image: config.image.risingwave.clone(),
            environment: [("RUST_BACKTRACE".to_string(), "1".to_string())]
                .into_iter()
                .collect(),
            volumes: [
                ("./risingwave.toml:/risingwave.toml".to_string()),
                format!("{}:/filecache", self.id),
            ]
            .into_iter()
            .collect(),
            command,
            expose: vec![self.port.to_string(), self.exporter_port.to_string()],
            depends_on: provide_meta_node
                .iter()
                .map(|x| x.id.clone())
                .chain(provide_minio.iter().map(|x| x.id.clone()))
                .collect(),
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        })
    }
}

impl Compose for MetaNodeConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("meta-node");
        MetaNodeService::apply_command_args(&mut command, self)?;
        command.arg("--config-path").arg("/risingwave.toml");
        let command = get_cmd_args(&command, true)?;

        std::fs::copy(
            Path::new("src").join("config").join("risingwave.toml"),
            Path::new(&config.config_directory).join("risingwave.toml"),
        )?;

        Ok(ComposeService {
            image: config.image.risingwave.clone(),
            environment: [("RUST_BACKTRACE".to_string(), "1".to_string())]
                .into_iter()
                .collect(),
            volumes: [("./risingwave.toml:/risingwave.toml".to_string())]
                .into_iter()
                .collect(),
            command,
            expose: vec![
                self.port.to_string(),
                self.exporter_port.to_string(),
                self.dashboard_port.to_string(),
            ],
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        })
    }
}

impl Compose for FrontendConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("frontend-node");
        FrontendService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();

        Ok(ComposeService {
            image: config.image.risingwave.clone(),
            environment: [("RUST_BACKTRACE".to_string(), "1".to_string())]
                .into_iter()
                .collect(),
            command,
            ports: vec![format!("{}:{}", self.port, self.port)],
            expose: vec![self.port.to_string()],
            depends_on: provide_meta_node.iter().map(|x| x.id.clone()).collect(),
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        })
    }
}

impl Compose for CompactorConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("compactor-node");
        CompactorService::apply_command_args(&mut command, self)?;
        command.arg("--config-path").arg("/risingwave.toml");
        let command = get_cmd_args(&command, true)?;

        std::fs::copy(
            Path::new("src").join("config").join("risingwave.toml"),
            Path::new(&config.config_directory).join("risingwave.toml"),
        )?;

        let provide_meta_node = self.provide_meta_node.as_ref().unwrap();
        let provide_minio = self.provide_minio.as_ref().unwrap();

        Ok(ComposeService {
            image: config.image.risingwave.clone(),
            environment: [("RUST_BACKTRACE".to_string(), "1".to_string())]
                .into_iter()
                .collect(),
            volumes: [("./risingwave.toml:/risingwave.toml".to_string())]
                .into_iter()
                .collect(),
            command,
            expose: vec![self.port.to_string(), self.exporter_port.to_string()],
            depends_on: provide_meta_node
                .iter()
                .map(|x| x.id.clone())
                .chain(provide_minio.iter().map(|x| x.id.clone()))
                .collect(),
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        })
    }
}

impl Compose for MinioConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("minio");
        MinioService::apply_command_args(&mut command, self)?;
        command.arg("/data");

        let env = get_cmd_envs(&command)?;
        let command = get_cmd_args(&command, false)?;
        let bucket_name = &self.hummock_bucket;

        let entrypoint = format!(
            r#"
/bin/sh -c '
set -e
mkdir -p "/data/{bucket_name}"
/usr/bin/docker-entrypoint.sh "$$0" "$$@"
'"#
        );

        Ok(ComposeService {
            image: config.image.minio.clone(),
            command,
            environment: env,
            entrypoint: Some(entrypoint),
            ports: vec![
                format!("{}:{}", self.port, self.port),
                format!("{}:{}", self.console_port, self.console_port),
            ],
            volumes: vec![format!("{}:/data", self.id)],
            expose: vec![self.port.to_string(), self.console_port.to_string()],
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        })
    }
}

impl Compose for RedPandaConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("redpanda");

        command
            .arg("start")
            .arg("--smp")
            .arg(self.cpus.to_string())
            .arg("--reserve-memory")
            .arg("0")
            .arg("--memory")
            .arg(&self.memory)
            .arg("--overprovisioned")
            .arg("--node-id")
            .arg("0")
            .arg("--check=false");

        command.arg("--kafka-addr").arg(format!(
            "PLAINTEXT://0.0.0.0:{},OUTSIDE://0.0.0.0:{}",
            self.internal_port, self.outside_port
        ));

        command.arg("--advertise-kafka-addr").arg(format!(
            "PLAINTEXT://{}:{},OUTSIDE://localhost:{}",
            self.address, self.internal_port, self.outside_port
        ));

        let command = get_cmd_args(&command, true)?;

        Ok(ComposeService {
            image: config.image.redpanda.clone(),
            command,
            expose: vec![
                self.internal_port.to_string(),
                self.outside_port.to_string(),
            ],
            volumes: vec![format!("{}:/var/lib/redpanda/data", self.id)],
            ports: vec![
                format!("{}:{}", self.outside_port, self.outside_port),
                // Redpanda admin port
                "9644:9644".to_string(),
            ],
            healthcheck: Some(health_check_port(self.outside_port)),
            ..Default::default()
        })
    }
}

impl Compose for PrometheusConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("prometheus");
        command
            .arg("--config.file=/etc/prometheus/prometheus.yml")
            .arg("--storage.tsdb.path=/prometheus")
            .arg("--web.console.libraries=/usr/share/prometheus/console_libraries")
            .arg("--web.console.templates=/usr/share/prometheus/consoles");
        PrometheusService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, false)?;

        let prometheus_config = PrometheusGen.gen_prometheus_yml(self);

        let mut service = ComposeService {
            image: config.image.prometheus.clone(),
            command,
            expose: vec![self.port.to_string()],
            ports: vec![format!("{}:{}", self.port, self.port)],
            volumes: vec![format!("{}:/prometheus", self.id)],
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        };

        std::fs::write(
            Path::new(&config.config_directory).join("prometheus.yaml"),
            prometheus_config,
        )?;
        service
            .volumes
            .push("./prometheus.yaml:/etc/prometheus/prometheus.yml".into());

        Ok(service)
    }
}

impl Compose for GrafanaConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let config_root = Path::new(&config.config_directory);
        std::fs::write(
            config_root.join("grafana.ini"),
            &GrafanaGen.gen_custom_ini(self),
        )?;

        std::fs::write(
            config_root.join("grafana-risedev-datasource.yml"),
            &GrafanaGen.gen_datasource_yml(self)?,
        )?;

        std::fs::write(
            config_root.join("grafana-risedev-dashboard.yml"),
            &GrafanaGen.gen_dashboard_yml(self, config_root, "/")?,
        )?;

        let service = ComposeService {
            image: config.image.grafana.clone(),
            expose: vec![self.port.to_string()],
            ports: vec![format!("{}:{}", self.port, self.port)],
            volumes: vec![
                format!("{}:/var/lib/grafana", self.id),
                "./grafana.ini:/etc/grafana/grafana.ini".to_string(),
                "./grafana-risedev-datasource.yml:/etc/grafana/provisioning/datasources/grafana-risedev-datasource.yml".to_string(),
                "./grafana-risedev-dashboard.yml:/etc/grafana/provisioning/dashboards/grafana-risedev-dashboard.yml".to_string(),
                "./risingwave-dashboard.json:/risingwave-dashboard.json".to_string()
            ],
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        };

        Ok(service)
    }
}

impl Compose for EtcdConfig {
    fn compose(&self, config: &ComposeConfig) -> Result<ComposeService> {
        let mut command = Command::new("/usr/local/bin/etcd");
        EtcdService::apply_command_args(&mut command, self)?;
        let command = get_cmd_args(&command, true)?;

        let service = ComposeService {
            image: config.image.etcd.clone(),
            command,
            expose: vec![self.port.to_string()],
            ports: vec![
                format!("{}:{}", self.port, self.port),
                format!("{}:{}", self.peer_port, self.peer_port),
            ],
            volumes: vec![format!("{}:/etcd-data", self.id)],
            healthcheck: Some(health_check_port(self.port)),
            ..Default::default()
        };

        Ok(service)
    }
}
