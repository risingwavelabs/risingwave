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
    pub provide_jaeger: Option<Vec<JaegerConfig>>,
    pub user_managed: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetaNodeConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub dashboard_address: String,
    pub dashboard_port: u16,
    pub user_managed: bool,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PrometheusConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
    pub provide_compute_node: Option<Vec<ComputeNodeConfig>>,
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

/// All service configuration
#[derive(Clone, Debug, PartialEq)]
pub enum ServiceConfig {
    ComputeNode(ComputeNodeConfig),
    MetaNode(MetaNodeConfig),
    Frontend(FrontendConfig),
    FrontendV2(FrontendConfig),
    Minio(MinioConfig),
    Prometheus(PrometheusConfig),
    Grafana(GrafanaConfig),
    Jaeger(JaegerConfig),
}

impl ServiceConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::ComputeNode(c) => &c.id,
            Self::MetaNode(c) => &c.id,
            Self::Frontend(c) => &c.id,
            Self::FrontendV2(c) => &c.id,
            Self::Minio(c) => &c.id,
            Self::Prometheus(c) => &c.id,
            Self::Grafana(c) => &c.id,
            Self::Jaeger(c) => &c.id,
        }
    }
}
