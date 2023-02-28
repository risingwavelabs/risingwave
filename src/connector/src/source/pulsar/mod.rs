// Copyright 2023 RisingWave Labs
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

pub mod enumerator;
pub mod source;
pub mod split;
pub mod topic;

use anyhow::{anyhow, Result};
pub use enumerator::*;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Authentication, Pulsar, TokioExecutor};
use serde::Deserialize;
pub use split::*;
use url::Url;

pub const PULSAR_CONNECTOR: &str = "pulsar";

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarOauth {
    #[serde(rename = "oauth.issuer.url")]
    pub issuer_url: String,

    #[serde(rename = "oauth.credentials.url")]
    pub credentials_url: String,

    #[serde(rename = "oauth.audience")]
    pub audience: Option<String>,

    #[serde(rename = "oauth.scope")]
    pub scope: Option<String>,
    // #[serde(flatten)]
    // pub s3_cridentials: Option<>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarProperties {
    #[serde(rename = "topic", alias = "pulsar.topic")]
    pub topic: String,

    #[serde(rename = "service.url", alias = "pulsar.service.url")]
    pub service_url: String,

    #[serde(rename = "scan.startup.mode", alias = "pulsar.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(rename = "scan.startup.timestamp_millis", alias = "pulsar.time.offset")]
    pub time_offset: Option<String>,

    #[serde(rename = "auth.token")]
    pub auth_token: Option<String>,

    #[serde(flatten)]
    pub oauth: Option<PulsarOauth>,
}

impl PulsarProperties {
    pub async fn build_pulsar_client(&mut self) -> Result<Pulsar<TokioExecutor>> {
        let mut pulsar_builder = Pulsar::builder(&self.service_url, TokioExecutor);
        if let Some(oauth) = self.oauth.take() {
            let url = Url::parse(&oauth.credentials_url)?;
            if url.scheme() == "s3" {
                todo!("s3 oauth credentials not supported yet");
            }

            let auth_params = OAuth2Params {
                issuer_url: oauth.issuer_url,
                credentials_url: oauth.credentials_url,
                audience: oauth.audience,
                scope: oauth.scope,
            };
            let oauth2 = OAuth2Authentication::client_credentials(auth_params);
            pulsar_builder = pulsar_builder.with_auth_provider(oauth2);
        } else if let Some(auth_token) = &self.auth_token {
            pulsar_builder = pulsar_builder.with_auth(Authentication {
                name: "token".to_string(),
                data: Vec::from(auth_token.as_str()),
            });
        }

        pulsar_builder.build().await.map_err(|e| anyhow!(e))
    }
}
