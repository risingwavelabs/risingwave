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

use anyhow::Context;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, Url};
use risingwave_common::bail;

use super::{PulsarSchemaInfo, PulsarSchemaRegistryConfig};
use crate::error::ConnectorResult;
use crate::source::pulsar::topic::parse_topic;

#[derive(Debug, Clone)]
pub struct PulsarSchemaSupplier {
    http_client: Client,
    admin_url: Url,
    schema_path: Vec<String>,
}

impl PulsarSchemaSupplier {
    pub fn new(config: PulsarSchemaRegistryConfig) -> ConnectorResult<Self> {
        let admin_url = Url::parse(&config.admin_url).context("invalid Pulsar admin URL")?;
        if admin_url.cannot_be_a_base() {
            bail!("Pulsar admin URL must be a base URL");
        }
        if !matches!(admin_url.scheme(), "http" | "https") {
            bail!("Pulsar admin URL must use HTTP or HTTPS");
        }
        let topic = parse_topic(&config.topic)?;
        let mut headers = HeaderMap::new();
        if let Some(token) = config.bearer_token {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .context("invalid Pulsar auth token header")?;
            headers.insert(AUTHORIZATION, value);
        }
        let http_client = Client::builder()
            .default_headers(headers)
            .build()
            .context("failed to build Pulsar schema supplier")?;
        let topic_name = topic.topic_str_without_partition()?;

        Ok(Self {
            http_client,
            admin_url,
            schema_path: vec![topic.tenant, topic.namespace, topic_name],
        })
    }

    fn build_schema_url(&self, suffix: &[&str]) -> Url {
        let mut url = self.admin_url.clone();
        url.path_segments_mut()
            .expect("constructor validates Pulsar admin URL can be a base")
            .extend(["admin", "v2", "schemas"])
            .extend(self.schema_path.iter().map(String::as_str))
            .extend(suffix);
        url
    }

    async fn fetch_schema(&self, suffix: &[&str]) -> ConnectorResult<PulsarSchemaInfo> {
        let url = self.build_schema_url(suffix);
        let response = self
            .http_client
            .get(url.clone())
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

    pub async fn get_latest_schema(&self) -> ConnectorResult<PulsarSchemaInfo> {
        self.fetch_schema(&["schema"]).await
    }

    pub async fn get_schema_by_version(&self, version: i64) -> ConnectorResult<PulsarSchemaInfo> {
        let version = version.to_string();
        self.fetch_schema(&["schema", version.as_str()]).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(topic: &str) -> PulsarSchemaRegistryConfig {
        PulsarSchemaRegistryConfig {
            admin_url: "http://localhost:8080".to_owned(),
            topic: topic.to_owned(),
            bearer_token: Some("test-token".to_owned()),
        }
    }

    #[test]
    fn test_pulsar_schema_url_from_full_topic() {
        let supplier =
            PulsarSchemaSupplier::new(test_config("persistent://tenant/ns/events")).unwrap();

        assert_eq!(
            supplier.build_schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            supplier.build_schema_url(&["schema", "42"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema/42"
        );
    }

    #[test]
    fn test_pulsar_schema_url_from_short_topic() {
        let supplier = PulsarSchemaSupplier::new(test_config("events")).unwrap();

        assert_eq!(
            supplier.build_schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/public/default/events/schema"
        );
    }

    #[test]
    fn test_pulsar_schema_url_from_partition_topic() {
        let supplier =
            PulsarSchemaSupplier::new(test_config("persistent://tenant/ns/events-partition-1"))
                .unwrap();

        assert_eq!(
            supplier.build_schema_url(&["schema"]).as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
    }
}
