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

use std::collections::BTreeMap;

use anyhow::Context;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, Method, Url};
use risingwave_common::bail;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::error::ConnectorResult;
use crate::schema::schema_registry::handle_sr_list;
use crate::source::pulsar::topic::parse_topic;

#[derive(Debug, Clone)]
pub struct PulsarSchemaRegistryConfig {
    pub admin_url: String,
    pub topic: String,
    pub auth_token: Option<String>,
}

impl PulsarSchemaRegistryConfig {
    pub fn from_source_options(
        admin_url: String,
        options: &BTreeMap<String, String>,
    ) -> ConnectorResult<Self> {
        let topic = options
            .get("pulsar.topic")
            .or_else(|| options.get("topic"))
            .context("Must specify 'pulsar.topic' or 'topic'")?
            .clone();
        let auth_token = options
            .get("auth.token")
            .or_else(|| options.get("pulsar.auth.token"))
            .cloned();
        Ok(Self {
            admin_url,
            topic,
            auth_token,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PulsarSchemaClient {
    inner: Client,
    admin_url: Url,
    schema_path: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct PulsarSchema {
    pub version: i64,
    #[serde(rename = "type")]
    pub schema_type: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PulsarSchemaType {
    Avro,
    Json,
    Protobuf,
    ProtobufNative,
    Other(String),
}

impl std::fmt::Display for PulsarSchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Avro => f.write_str("AVRO"),
            Self::Json => f.write_str("JSON"),
            Self::Protobuf => f.write_str("PROTOBUF"),
            Self::ProtobufNative => f.write_str("PROTOBUF_NATIVE"),
            Self::Other(schema_type) => f.write_str(schema_type),
        }
    }
}

impl PulsarSchema {
    pub fn schema_type(&self) -> PulsarSchemaType {
        match self.schema_type.to_ascii_uppercase().as_str() {
            "AVRO" => PulsarSchemaType::Avro,
            "JSON" => PulsarSchemaType::Json,
            "PROTOBUF" => PulsarSchemaType::Protobuf,
            "PROTOBUF_NATIVE" => PulsarSchemaType::ProtobufNative,
            _ => PulsarSchemaType::Other(self.schema_type.clone()),
        }
    }
}

impl PulsarSchemaClient {
    pub fn new(config: PulsarSchemaRegistryConfig) -> ConnectorResult<Self> {
        let admin_url = handle_sr_list(&config.admin_url)?
            .into_iter()
            .next()
            .context("pulsar schema registry URL is empty")?;
        if admin_url.cannot_be_a_base() {
            bail!("Pulsar admin URL must be a base URL");
        }
        let topic = parse_topic(&config.topic)?;
        let mut headers = HeaderMap::new();
        if let Some(token) = config.auth_token {
            let token = token.strip_prefix("token:").unwrap_or(&token);
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid Pulsar auth token header")?;
            headers.insert(AUTHORIZATION, value);
        }
        let inner = Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build Pulsar schema client")?;
        let topic_name = topic.topic_str_without_partition()?;

        Ok(Self {
            inner,
            admin_url,
            schema_path: vec![topic.tenant, topic.namespace, topic_name],
        })
    }

    pub async fn get_latest_schema(&self) -> ConnectorResult<PulsarSchema> {
        self.request(&["schema"]).await
    }

    pub async fn get_schema_by_version(&self, version: i64) -> ConnectorResult<PulsarSchema> {
        let version = version.to_string();
        self.request(&["schema", version.as_str()]).await
    }

    fn schema_url(&self, suffix: &[&str]) -> Url {
        let mut url = self.admin_url.clone();
        url.path_segments_mut()
            .expect("constructor validates Pulsar admin URL can be a base")
            .extend(["admin", "v2", "schemas"])
            .extend(self.schema_path.iter().map(String::as_str))
            .extend(suffix);
        url
    }

    async fn request<T>(&self, suffix: &[&str]) -> ConnectorResult<T>
    where
        T: DeserializeOwned,
    {
        let url = self.schema_url(suffix);
        let response = self
            .inner
            .request(Method::GET, url.clone())
            .send()
            .await
            .with_context(|| format!("failed to fetch Pulsar schema from {url}"))?
            .error_for_status()
            .with_context(|| format!("Pulsar schema request failed for {url}"))?;
        Ok(response
            .json()
            .await
            .with_context(|| format!("failed to parse Pulsar schema response from {url}"))?)
    }
}

pub fn pulsar_schema_version_to_i64(version: &[u8]) -> ConnectorResult<Option<i64>> {
    if version.is_empty() {
        return Ok(None);
    }
    let bytes: [u8; 8] = version.try_into().with_context(|| {
        format!(
            "expected 8-byte Pulsar LongSchemaVersion, got {} bytes",
            version.len()
        )
    })?;
    Ok(Some(i64::from_be_bytes(bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(topic: &str) -> PulsarSchemaRegistryConfig {
        PulsarSchemaRegistryConfig {
            admin_url: "http://localhost:8080".to_owned(),
            topic: topic.to_owned(),
            auth_token: Some("token:test-token".to_owned()),
        }
    }

    #[test]
    fn test_pulsar_schema_url_from_full_topic() {
        let client = PulsarSchemaClient::new(test_config("persistent://tenant/ns/events")).unwrap();

        assert_eq!(
            client.schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client.schema_url(&["schema", "42"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema/42"
        );
    }

    #[test]
    fn test_pulsar_schema_url_from_short_topic() {
        let client = PulsarSchemaClient::new(test_config("events")).unwrap();

        assert_eq!(
            client.schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/public/default/events/schema"
        );
    }

    #[test]
    fn test_pulsar_schema_url_from_partition_topic() {
        let client =
            PulsarSchemaClient::new(test_config("persistent://tenant/ns/events-partition-1"))
                .unwrap();

        assert_eq!(
            client.schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
    }

    #[test]
    fn test_pulsar_schema_version_to_i64() {
        assert_eq!(pulsar_schema_version_to_i64(&[]).unwrap(), None);
        assert_eq!(
            pulsar_schema_version_to_i64(&1_i64.to_be_bytes()).unwrap(),
            Some(1)
        );
        assert_eq!(
            pulsar_schema_version_to_i64(&(-1_i64).to_be_bytes()).unwrap(),
            Some(-1)
        );
        assert!(pulsar_schema_version_to_i64(&[1, 2, 3]).is_err());
    }

    #[test]
    fn test_pulsar_schema_type() {
        let schema = |schema_type: &str| PulsarSchema {
            version: 0,
            schema_type: schema_type.to_owned(),
            data: String::new(),
        };

        assert_eq!(schema("Avro").schema_type(), PulsarSchemaType::Avro);
        assert_eq!(
            schema("PROTOBUF_NATIVE").schema_type(),
            PulsarSchemaType::ProtobufNative
        );
        assert_eq!(
            schema("STRING").schema_type(),
            PulsarSchemaType::Other("STRING".to_owned())
        );
    }
}
