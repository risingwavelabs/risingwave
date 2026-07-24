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

use risingwave_common::bail;

use crate::error::ConnectorResult;
use crate::schema::schema_registry::{SCHEMA_REGISTRY_PASSWORD, SCHEMA_REGISTRY_USERNAME};

pub const PULSAR_SCHEMA_REGISTRY_AUTH_TYPE_TOKEN: &str = "token";

#[derive(Debug, Clone)]
pub struct PulsarSchemaRegistryConfig {
    pub admin_url: String,
    pub topic: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl PulsarSchemaRegistryConfig {
    pub fn new(
        admin_url: String,
        format_options: &BTreeMap<String, String>,
        source_options: &BTreeMap<String, String>,
    ) -> ConnectorResult<Self> {
        let topic = source_options
            .get("pulsar.topic")
            .or_else(|| source_options.get("topic"))
            .ok_or_else(|| anyhow::anyhow!("Must specify 'pulsar.topic' or 'topic'"))?
            .clone();
        let (username, password) = Self::credentials(format_options)?;
        Ok(Self {
            admin_url,
            topic,
            username,
            password,
        })
    }

    fn credentials(
        format_options: &BTreeMap<String, String>,
    ) -> ConnectorResult<(Option<String>, Option<String>)> {
        match format_options.get(SCHEMA_REGISTRY_USERNAME) {
            Some(auth_type)
                if auth_type.eq_ignore_ascii_case(PULSAR_SCHEMA_REGISTRY_AUTH_TYPE_TOKEN) =>
            {
                let token = format_options
                    .get(SCHEMA_REGISTRY_PASSWORD)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Must specify '{}' when '{}' is '{}'",
                            SCHEMA_REGISTRY_PASSWORD,
                            SCHEMA_REGISTRY_USERNAME,
                            PULSAR_SCHEMA_REGISTRY_AUTH_TYPE_TOKEN
                        )
                    })?;
                return Ok((Some(auth_type.clone()), Some(token.clone())));
            }
            Some(auth_type) => {
                bail!(
                    "unsupported Pulsar schema registry auth type `{}`, expected `{}`",
                    auth_type,
                    PULSAR_SCHEMA_REGISTRY_AUTH_TYPE_TOKEN
                );
            }
            None => {
                if format_options.contains_key(SCHEMA_REGISTRY_PASSWORD) {
                    bail!(
                        "Must specify '{}' when '{}' is set for Pulsar schema registry",
                        SCHEMA_REGISTRY_USERNAME,
                        SCHEMA_REGISTRY_PASSWORD
                    );
                }
            }
        }

        Ok((None, None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options<const N: usize>(opts: &[(&str, &str); N]) -> BTreeMap<String, String> {
        opts.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn pulsar_schema_registry_token_auth_uses_registry_password() {
        let format_options = options(&[
            (
                SCHEMA_REGISTRY_USERNAME,
                PULSAR_SCHEMA_REGISTRY_AUTH_TYPE_TOKEN,
            ),
            (SCHEMA_REGISTRY_PASSWORD, "registry-token"),
        ]);

        let config = PulsarSchemaRegistryConfig::new(
            "http://localhost:8080".to_owned(),
            &format_options,
            &options(&[("pulsar.topic", "persistent://tenant/ns/events")]),
        )
        .unwrap();

        assert_eq!(config.topic, "persistent://tenant/ns/events");
        assert_eq!(config.username, Some("token".to_owned()));
        assert_eq!(config.password, Some("registry-token".to_owned()));

        let config = PulsarSchemaRegistryConfig::new(
            "http://localhost:8080".to_owned(),
            &BTreeMap::new(),
            &options(&[("topic", "events")]),
        )
        .unwrap();

        assert_eq!(config.topic, "events");
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
    }

    #[test]
    fn pulsar_schema_registry_rejects_unknown_auth_type() {
        let format_options = options(&[(SCHEMA_REGISTRY_USERNAME, "basic")]);

        assert!(
            PulsarSchemaRegistryConfig::new(
                "http://localhost:8080".to_owned(),
                &format_options,
                &options(&[("pulsar.topic", "persistent://tenant/ns/events")]),
            )
            .is_err()
        );
    }
}
