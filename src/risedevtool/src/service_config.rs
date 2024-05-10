// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct ComputeNodeConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,
    pub async_stack_trace: String,
    pub enable_tiered_cache: bool,

    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_opendal: Option<Vec<OpendalConfig>>,
    pub provide_aws_s3: Option<Vec<AwsS3Config>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,
    pub user_managed: bool,

    pub total_memory_bytes: usize,
    pub parallelism: usize,
    pub role: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MetaNodeConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub dashboard_port: u16,
    pub exporter_port: u16,

    pub user_managed: bool,

    pub provide_etcd_backend: Option<Vec<EtcdConfig>>,
    pub provide_sqlite_backend: Option<Vec<SqliteConfig>>,
    pub provide_prometheus: Option<Vec<PrometheusConfig>>,

    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_compactor: Option<Vec<CompactorConfig>>,

    pub provide_tempo: Option<Vec<TempoConfig>>,

    pub provide_aws_s3: Option<Vec<AwsS3Config>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_opendal: Option<Vec<OpendalConfig>>,
    pub enable_in_memory_kv_state_backend: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct FrontendConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,
    pub health_check_port: u16,

    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,

    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct CompactorConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,
    pub exporter_port: u16,

    pub provide_minio: Option<Vec<MinioConfig>>,

    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,

    pub user_managed: bool,
    pub compaction_worker_threads_number: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MinioConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,

    pub console_address: String,
    #[serde(with = "string")]
    pub console_port: u16,

    pub root_user: String,
    pub root_password: String,
    pub hummock_bucket: String,

    pub provide_prometheus: Option<Vec<PrometheusConfig>>,

    // For rate limiting minio in a test environment.
    pub api_requests_max: usize,
    pub api_requests_deadline: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct EtcdConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    // TODO: only one node etcd is supported.
    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,

    pub peer_port: u16,
    pub unsafe_no_fsync: bool,

    pub exporter_port: u16,

    pub provide_etcd: Option<Vec<EtcdConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub file: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    pub listen_address: String,

    pub remote_write: bool,
    pub remote_write_region: String,
    pub remote_write_url: String,

    pub scrape_interval: String,

    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
    pub provide_meta_node: Option<Vec<MetaNodeConfig>>,
    pub provide_minio: Option<Vec<MinioConfig>>,
    pub provide_compactor: Option<Vec<CompactorConfig>>,
    pub provide_etcd: Option<Vec<EtcdConfig>>,
    pub provide_redpanda: Option<Vec<RedPandaConfig>>,
    pub provide_frontend: Option<Vec<FrontendConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct GrafanaConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    pub address: String,
    pub listen_address: String,
    pub port: u16,

    pub provide_prometheus: Option<Vec<PrometheusConfig>>,
    pub provide_tempo: Option<Vec<TempoConfig>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct TempoConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub listen_address: String,
    pub address: String,
    pub port: u16,
    pub otlp_port: u16,
    pub max_bytes_per_trace: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct AwsS3Config {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    pub bucket: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct OpendalConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,

    pub id: String,
    pub engine: String,
    pub namenode: String,
    pub bucket: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub address: String,
    #[serde(with = "string")]
    pub port: u16,
    #[serde(with = "string")]
    pub controller_port: u16,
    pub listen_address: String,

    pub persist_data: bool,
    pub node_id: u32,

    pub user_managed: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct PubsubConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    #[serde(with = "string")]
    pub port: u16,
    pub address: String,

    pub persist_data: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct RedPandaConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,
    pub internal_port: u16,
    pub outside_port: u16,
    pub address: String,
    pub cpus: usize,
    pub memory: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
pub struct MySqlConfig {
    #[serde(rename = "use")]
    phantom_use: Option<String>,
    pub id: String,

    pub port: u16,
    pub address: String,

    pub user: String,
    pub password: String,
    pub database: String,

    pub image: String,
    pub user_managed: bool,
    pub persist_data: bool,
}

/// All service configuration
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceConfig {
    ComputeNode(ComputeNodeConfig),
    MetaNode(MetaNodeConfig),
    Frontend(FrontendConfig),
    Compactor(CompactorConfig),
    Minio(MinioConfig),
    Etcd(EtcdConfig),
    Sqlite(SqliteConfig),
    Prometheus(PrometheusConfig),
    Grafana(GrafanaConfig),
    Tempo(TempoConfig),
    Opendal(OpendalConfig),
    AwsS3(AwsS3Config),
    Kafka(KafkaConfig),
    Pubsub(PubsubConfig),
    Redis(RedisConfig),
    RedPanda(RedPandaConfig),
    MySql(MySqlConfig),
}

impl ServiceConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::ComputeNode(c) => &c.id,
            Self::MetaNode(c) => &c.id,
            Self::Frontend(c) => &c.id,
            Self::Compactor(c) => &c.id,
            Self::Minio(c) => &c.id,
            Self::Etcd(c) => &c.id,
            Self::Sqlite(c) => &c.id,
            Self::Prometheus(c) => &c.id,
            Self::Grafana(c) => &c.id,
            Self::Tempo(c) => &c.id,
            Self::AwsS3(c) => &c.id,
            Self::Kafka(c) => &c.id,
            Self::Pubsub(c) => &c.id,
            Self::Redis(c) => &c.id,
            Self::RedPanda(c) => &c.id,
            Self::Opendal(c) => &c.id,
            Self::MySql(c) => &c.id,
        }
    }

    pub fn port(&self) -> Option<u16> {
        match self {
            Self::ComputeNode(c) => Some(c.port),
            Self::MetaNode(c) => Some(c.port),
            Self::Frontend(c) => Some(c.port),
            Self::Compactor(c) => Some(c.port),
            Self::Minio(c) => Some(c.port),
            Self::Etcd(c) => Some(c.port),
            Self::Sqlite(_) => None,
            Self::Prometheus(c) => Some(c.port),
            Self::Grafana(c) => Some(c.port),
            Self::Tempo(c) => Some(c.port),
            Self::AwsS3(_) => None,
            Self::Kafka(c) => Some(c.port),
            Self::Pubsub(c) => Some(c.port),
            Self::Redis(c) => Some(c.port),
            Self::RedPanda(_c) => None,
            Self::Opendal(_) => None,
            Self::MySql(c) => Some(c.port),
        }
    }

    pub fn user_managed(&self) -> bool {
        match self {
            Self::ComputeNode(c) => c.user_managed,
            Self::MetaNode(c) => c.user_managed,
            Self::Frontend(c) => c.user_managed,
            Self::Compactor(c) => c.user_managed,
            Self::Minio(_c) => false,
            Self::Etcd(_c) => false,
            Self::Sqlite(_c) => false,
            Self::Prometheus(_c) => false,
            Self::Grafana(_c) => false,
            Self::Tempo(_c) => false,
            Self::AwsS3(_c) => false,
            Self::Kafka(c) => c.user_managed,
            Self::Pubsub(_c) => false,
            Self::Redis(_c) => false,
            Self::RedPanda(_c) => false,
            Self::Opendal(_c) => false,
            Self::MySql(c) => c.user_managed,
        }
    }
}

mod string {
    use std::fmt::Display;
    use std::str::FromStr;

    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        T: FromStr,
        T::Err: Display,
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}
