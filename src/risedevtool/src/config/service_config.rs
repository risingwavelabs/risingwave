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

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ComputeNodeConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub exporter_address: String,
    pub exporter_port: u16,
    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_aws_s3: Option<Vec<AwsS3Config>>,
    pub provide_jaeger: Option<Vec<JaegerConfig>>,
    pub user_managed: bool,
    pub enable_in_memory_kv_state_backend: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetaNodeConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub dashboard_address: String,
    pub dashboard_port: u16,
    pub exporter_address: String,
    pub exporter_port: u16,
    pub user_managed: bool,
    pub provide_etcd_backend: Option<Vec<EtcdConfig>>,
    pub enable_dashboard_v2: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FrontendConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MinioConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub console_address: String,
    pub console_port: u16,
    pub root_user: String,
    pub root_password: String,
    pub hummock_user: String,
    pub hummock_password: String,
    pub hummock_bucket: String,
    pub provide_prometheus: Option<Vec<PrometheusConfig>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EtcdConfig {
    pub id: String,
    // TODO: only one node etcd is supported.
    pub address: String,
    pub port: u16,
    pub peer_port: u16,
    pub unsafe_no_fsync: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PrometheusConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GrafanaConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub provide_prometheus: Option<Vec<PrometheusConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct JaegerConfig {
    pub id: String,
    pub dashboard_address: String,
    pub dashboard_port: u16,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct AwsS3Config {
    pub id: String,
    pub bucket: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct KafkaConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub provide_zookeeper: Option<Vec<ZooKeeperConfig>>,
    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ZooKeeperConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub persist_data: bool,
}

/// All service configuration
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceConfig {
    ComputeNode(ComputeNodeConfig),
    MetaNode(MetaNodeConfig),
    Frontend(FrontendConfig),
    FrontendV2(FrontendConfig),
    Minio(MinioConfig),
    Etcd(EtcdConfig),
    Prometheus(PrometheusConfig),
    Grafana(GrafanaConfig),
    Jaeger(JaegerConfig),
    AwsS3(AwsS3Config),
    Kafka(KafkaConfig),
    ZooKeeper(ZooKeeperConfig),
}

impl ServiceConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::ComputeNode(c) => &c.id,
            Self::MetaNode(c) => &c.id,
            Self::Frontend(c) => &c.id,
            Self::FrontendV2(c) => &c.id,
            Self::Minio(c) => &c.id,
            Self::Etcd(c) => &c.id,
            Self::Prometheus(c) => &c.id,
            Self::Grafana(c) => &c.id,
            Self::Jaeger(c) => &c.id,
            Self::AwsS3(c) => &c.id,
            Self::ZooKeeper(c) => &c.id,
            Self::Kafka(c) => &c.id,
        }
    }
}
