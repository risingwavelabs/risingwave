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

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;

use anyhow::{anyhow, Result};
pub use enumerator::*;
use pulsar::authentication::oauth2::{OAuth2Authentication, OAuth2Params};
use pulsar::{Authentication, Pulsar, TokioExecutor};
use serde::Deserialize;
pub use split::*;
use tempfile::{tempfile, NamedTempFile, TempPath};
use url::Url;

use crate::aws_utils::{load_file_descriptor_from_s3, AWS_DEFAULT_CONFIG};

pub const PULSAR_CONNECTOR: &str = "pulsar";

#[derive(Clone, Debug, Deserialize)]
pub struct PulsarOauth {
    #[serde(rename = "oauth.issuer.url")]
    pub issuer_url: String,

    #[serde(rename = "oauth.credentials.url")]
    pub credentials_url: String,

    #[serde(rename = "oauth.audience")]
    pub audience: String,

    #[serde(rename = "oauth.scope")]
    pub scope: Option<String>,

    #[serde(flatten)]
    /// required keys refer to [`AWS_DEFAULT_CONFIG`]
    pub s3_credentials: HashMap<String, String>,
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
    pub async fn build_pulsar_client(&self) -> Result<Pulsar<TokioExecutor>> {
        let mut pulsar_builder = Pulsar::builder(&self.service_url, TokioExecutor);
        let mut temp_file = None;
        if let Some(oauth) = &self.oauth {
            let url = Url::parse(&oauth.credentials_url)?;
            if url.scheme() == "s3" {
                let credentials = load_file_descriptor_from_s3(&url, &oauth.s3_credentials).await?;
                let mut f = NamedTempFile::new()?;
                f.write_all(&credentials)?;
                f.as_file().sync_all()?;
                temp_file = Some(f);
            }

            let auth_params = OAuth2Params {
                issuer_url: oauth.issuer_url.clone(),
                credentials_url: if temp_file.is_none() {
                    oauth.credentials_url.clone()
                } else {
                    let mut raw_path = temp_file
                        .as_ref()
                        .unwrap()
                        .path()
                        .to_str()
                        .unwrap()
                        .to_string();
                    raw_path.insert_str(0, "file:");
                    raw_path
                },
                audience: Some(oauth.audience.clone()),
                scope: oauth.scope.clone(),
            };

            pulsar_builder = pulsar_builder
                .with_auth_provider(OAuth2Authentication::client_credentials(auth_params));
        } else if let Some(auth_token) = &self.auth_token {
            pulsar_builder = pulsar_builder.with_auth(Authentication {
                name: "token".to_string(),
                data: Vec::from(auth_token.as_str()),
            });
        }

        let res = pulsar_builder.build().await.map_err(|e| anyhow!(e))?;
        drop(temp_file);
        Ok(res)
    }
}
