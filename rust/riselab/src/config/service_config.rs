use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ComputeNodeConfig {
    id: String,
    address: String,
    port: u16,
    exporter_address: String,
    exporter_port: u16,
    provide_minio: Vec<String>,
    user_managed: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MetaNodeConfig {
    id: String,
    address: String,
    port: u16,
    user_managed: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct FrontendConfig {
    id: String,
    address: String,
    port: u16,
    provide_compute_node: Vec<String>,
    provide_meta_node: Vec<String>,
    user_managed: bool,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MinioConfig {
    id: String,
    address: String,
    port: u16,
    console_address: String,
    console_port: u16,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct PrometheusConfig {
    id: String,
    address: String,
    port: u16,
    provide_compute_node: Vec<String>,
}

/// All service configuration
#[derive(Debug, PartialEq)]
pub enum ServiceConfig {
    ComputeNode(ComputeNodeConfig),
    MetaNode(MetaNodeConfig),
    Frontend(FrontendConfig),
    Minio(MinioConfig),
    Prometheus(PrometheusConfig),
}

impl ServiceConfig {
    pub fn id(&self) -> &str {
        match self {
            Self::ComputeNode(c) => &c.id,
            Self::MetaNode(c) => &c.id,
            Self::Frontend(c) => &c.id,
            Self::Minio(c) => &c.id,
            Self::Prometheus(c) => &c.id,
        }
    }
}
