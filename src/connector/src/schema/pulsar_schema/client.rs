// Copyright 2026 RisingWave Labs
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

use std::fmt;
use std::time::Duration;

use anyhow::Context;
use reqwest::{Client, StatusCode, Url};
use risingwave_common::bail;

use super::PulsarSchemaInfo;
use crate::Get;
use crate::error::ConnectorResult;
use crate::schema::schema_registry::{
    SCHEMA_REGISTRY_BACKOFF_DURATION_KEY, SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY,
    SCHEMA_REGISTRY_CA_PEM_PATH, SCHEMA_REGISTRY_MAX_DELAY_KEY, SCHEMA_REGISTRY_PASSWORD,
    SCHEMA_REGISTRY_RETRIES_MAX_KEY, SCHEMA_REGISTRY_USERNAME,
};
use crate::source::pulsar::topic::parse_topic;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_DELAY_SEC: u64 = 3;
const DEFAULT_BACKOFF_DURATION_MS: u64 = 100;
const DEFAULT_BACKOFF_FACTOR: u64 = 2;
const DEFAULT_RETRIES_MAX: usize = 3;

#[derive(Clone)]
pub struct PulsarSchemaClientConfig {
    pub admin_url: String,
    pub bearer_token: Option<String>,
    ca_pem_path: Option<String>,
    max_delay_sec: u64,
    backoff_duration_ms: u64,
    backoff_factor: u64,
    retries_max: usize,
}

impl fmt::Debug for PulsarSchemaClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarSchemaClientConfig")
            .field("admin_url", &self.admin_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("ca_pem_path", &self.ca_pem_path)
            .field("max_delay_sec", &self.max_delay_sec)
            .field("backoff_duration_ms", &self.backoff_duration_ms)
            .field("backoff_factor", &self.backoff_factor)
            .field("retries_max", &self.retries_max)
            .finish()
    }
}

impl PulsarSchemaClientConfig {
    pub fn from_options(
        admin_url: String,
        bearer_token: Option<String>,
        format_options: &impl Get,
    ) -> ConnectorResult<Self> {
        if bearer_token.as_ref().is_some_and(String::is_empty) {
            bail!("Pulsar auth token must not be empty");
        }
        for option in [SCHEMA_REGISTRY_USERNAME, SCHEMA_REGISTRY_PASSWORD] {
            if format_options.get(option).is_some() {
                bail!("`{option}` is not supported by the Pulsar schema client");
            }
        }

        fn parse<T: std::str::FromStr>(
            options: &impl Get,
            key: &str,
            default: T,
        ) -> ConnectorResult<T> {
            let value = options
                .get(key)
                .map(|value| {
                    value
                        .parse()
                        .map_err(|_| anyhow::anyhow!("invalid value `{value}` for `{key}`"))
                })
                .transpose()?;
            Ok(value.unwrap_or(default))
        }

        Ok(Self {
            admin_url,
            bearer_token,
            ca_pem_path: format_options.get(SCHEMA_REGISTRY_CA_PEM_PATH).cloned(),
            max_delay_sec: parse(
                format_options,
                SCHEMA_REGISTRY_MAX_DELAY_KEY,
                DEFAULT_MAX_DELAY_SEC,
            )?,
            backoff_duration_ms: parse(
                format_options,
                SCHEMA_REGISTRY_BACKOFF_DURATION_KEY,
                DEFAULT_BACKOFF_DURATION_MS,
            )?,
            backoff_factor: parse(
                format_options,
                SCHEMA_REGISTRY_BACKOFF_FACTOR_KEY,
                DEFAULT_BACKOFF_FACTOR,
            )?,
            retries_max: parse(
                format_options,
                SCHEMA_REGISTRY_RETRIES_MAX_KEY,
                DEFAULT_RETRIES_MAX,
            )?,
        })
    }
}

#[derive(Clone)]
pub struct PulsarSchemaClient {
    http_client: Client,
    admin_url: Url,
    bearer_token: Option<String>,
    max_delay: Duration,
    backoff_duration: Duration,
    backoff_factor: u64,
    retries_max: usize,
}

impl fmt::Debug for PulsarSchemaClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PulsarSchemaClient")
            .field("admin_url", &self.admin_url)
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("max_delay", &self.max_delay)
            .field("backoff_duration", &self.backoff_duration)
            .field("backoff_factor", &self.backoff_factor)
            .field("retries_max", &self.retries_max)
            .finish_non_exhaustive()
    }
}

impl PulsarSchemaClient {
    pub fn new(config: PulsarSchemaClientConfig) -> ConnectorResult<Self> {
        let admin_url = Url::parse(&config.admin_url).context("invalid Pulsar admin URL")?;
        if admin_url.cannot_be_a_base() {
            bail!("Pulsar admin URL must be a base URL");
        }
        if !matches!(admin_url.scheme(), "http" | "https") {
            bail!("Pulsar admin URL must use HTTP or HTTPS");
        }

        let mut builder = Client::builder().timeout(DEFAULT_REQUEST_TIMEOUT);
        if let Some(ca_path) = config.ca_pem_path.as_ref() {
            if ca_path.eq_ignore_ascii_case("ignore") {
                builder = builder.danger_accept_invalid_certs(true);
            } else {
                let pem = std::fs::read(ca_path).with_context(|| {
                    format!("failed to read schema registry CA file `{ca_path}`")
                })?;
                let certificate = reqwest::Certificate::from_pem(&pem).with_context(|| {
                    format!("failed to parse schema registry CA file `{ca_path}`")
                })?;
                builder = builder.add_root_certificate(certificate);
            }
        }

        let http_client = builder
            .build()
            .context("failed to build Pulsar schema client")?;
        Ok(Self {
            http_client,
            admin_url,
            bearer_token: config.bearer_token,
            max_delay: Duration::from_secs(config.max_delay_sec),
            backoff_duration: Duration::from_millis(config.backoff_duration_ms),
            backoff_factor: config.backoff_factor,
            retries_max: config.retries_max,
        })
    }

    fn build_schema_url(&self, topic: &str, version: Option<i64>) -> ConnectorResult<Url> {
        let topic = parse_topic(topic)?;
        let topic_name = topic.topic_str_without_partition()?;
        let mut url = self.admin_url.clone();
        let mut path = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Pulsar admin URL must be a base URL"))?;
        path.extend([
            "admin",
            "v2",
            "schemas",
            topic.tenant.as_str(),
            topic.namespace.as_str(),
            topic_name.as_str(),
            "schema",
        ]);
        if let Some(version) = version {
            path.push(version.to_string().as_str());
        }
        drop(path);
        Ok(url)
    }

    fn retry_delay(&self, retry: usize) -> Duration {
        let exponent = u32::try_from(retry).unwrap_or(u32::MAX);
        let factor = self.backoff_factor.saturating_pow(exponent);
        self.backoff_duration
            .saturating_mul(u32::try_from(factor).unwrap_or(u32::MAX))
            .min(self.max_delay)
    }

    pub async fn get_schema(
        &self,
        topic: &str,
        version: Option<i64>,
    ) -> ConnectorResult<PulsarSchemaInfo> {
        let url = self.build_schema_url(topic, version)?;
        for retry in 0..=self.retries_max {
            let mut request = self.http_client.get(url.clone());
            if let Some(token) = self.bearer_token.as_ref() {
                request = request.bearer_auth(token);
            }

            match request.send().await {
                Ok(response)
                    if retry < self.retries_max
                        && (response.status() == StatusCode::TOO_MANY_REQUESTS
                            || response.status().is_server_error()) =>
                {
                    tokio::time::sleep(self.retry_delay(retry)).await;
                }
                Ok(response) => {
                    return Ok(response
                        .error_for_status()
                        .with_context(|| format!("Pulsar schema request failed for {url}"))?
                        .json()
                        .await
                        .with_context(|| {
                            format!("failed to parse Pulsar schema response from {url}")
                        })?);
                }
                Err(error) if retry < self.retries_max => {
                    tracing::debug!(
                        retry,
                        error = %error,
                        "retrying Pulsar schema request"
                    );
                    tokio::time::sleep(self.retry_delay(retry)).await;
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to fetch Pulsar schema from {url}"))
                        .map_err(Into::into);
                }
            }
        }
        unreachable!("Pulsar schema retry loop always returns on its final attempt")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn client() -> PulsarSchemaClient {
        PulsarSchemaClient::new(
            PulsarSchemaClientConfig::from_options(
                "http://localhost:8080".to_owned(),
                Some("test-token".to_owned()),
                &BTreeMap::new(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn schema_url_from_full_topic() {
        let client = client();
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events", Some(42))
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema/42"
        );
    }

    #[test]
    fn schema_url_from_short_and_partitioned_topics() {
        let client = client();
        assert_eq!(
            client.build_schema_url("events", None).unwrap().as_str(),
            "http://localhost:8080/admin/v2/schemas/public/default/events/schema"
        );
        assert_eq!(
            client
                .build_schema_url("persistent://tenant/ns/events-partition-1", None)
                .unwrap()
                .as_str(),
            "http://localhost:8080/admin/v2/schemas/tenant/ns/events/schema"
        );
    }

    #[test]
    fn config_debug_redacts_token() {
        let config = PulsarSchemaClientConfig::from_options(
            "http://localhost:8080".to_owned(),
            Some("secret-token".to_owned()),
            &BTreeMap::new(),
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[REDACTED]"));
    }
}
