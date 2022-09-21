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

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::anyhow;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_s3::{client as s3_client, config as s3_config};
use aws_smithy_http::endpoint::Endpoint;
use aws_smithy_types::timeout::Http;
use aws_smithy_types::tristate::TriState;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use http::Uri;
use serde::{Deserialize, Serialize};

pub const AWS_DEFAULT_CONFIG: [&str; 7] = [
    "region",
    "arn",
    "profile",
    "access_key",
    "secret_access",
    "session_token",
    "endpoint_url",
];
pub const AWS_CUSTOM_CONFIG_KEY: [&str; 3] = ["retry_times", "conn_timeout", "read_timeout"];

pub fn default_conn_config() -> HashMap<String, u64> {
    let mut default_conn_config = HashMap::new();
    default_conn_config.insert("retry_times".to_owned(), 3_u64);
    default_conn_config.insert("conn_timeout".to_owned(), 3_u64);
    default_conn_config.insert("read_timeout".to_owned(), 5_u64);
    default_conn_config
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AwsCustomConfig {
    pub read_timeout: Duration,
    pub conn_timeout: Duration,
    pub retry_times: u32,
}

impl Default for AwsCustomConfig {
    fn default() -> Self {
        let map = default_conn_config();
        AwsCustomConfig::from(map)
    }
}

impl From<HashMap<String, u64>> for AwsCustomConfig {
    fn from(input_config: HashMap<String, u64>) -> Self {
        let mut config = AwsCustomConfig {
            read_timeout: Duration::from_secs(3),
            conn_timeout: Duration::from_secs(3),
            retry_times: 0,
        };
        for key in AWS_CUSTOM_CONFIG_KEY {
            let value = input_config.get(key);
            if let Some(config_value) = value {
                match key {
                    "retry_times" => {
                        config.retry_times = *config_value as u32;
                    }
                    "conn_timeout" => {
                        config.conn_timeout = Duration::from_secs(*config_value);
                    }
                    "read_timeout" => {
                        config.read_timeout = Duration::from_secs(*config_value);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                continue;
            }
        }
        config
    }
}

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsConfigV2 {
    pub region: Option<String>,
    pub arn: Option<String>,
    pub credential: AwsCredentialV2,
    pub endpoint: Option<EndpointWrapper>,
}

impl From<HashMap<String, String>> for AwsConfigV2 {
    fn from(props: HashMap<String, String>) -> Self {
        let mut credential = AwsCredentialV2::default();
        let mut static_credential: (String, String, Option<String>) =
            ("".to_string(), "".to_string(), None);
        let mut config = AwsConfigV2 {
            region: None,
            arn: None,
            credential: credential.clone(),
            endpoint: None,
        };

        for config_key in AWS_DEFAULT_CONFIG {
            let value = props.get(config_key);
            if let Some(config_value) = value {
                match config_key {
                    "region" => {
                        config.region = Some(config_value.to_string());
                    }
                    "arn" => {
                        config.arn = Some(config_value.to_string());
                    }
                    "profile" => {
                        credential = AwsCredentialV2::ProfileName(config_value.to_string());
                    }
                    "access_key" => {
                        static_credential.0 = config_value.to_string();
                    }
                    "secret_access" => {
                        static_credential.1 = config_value.to_string();
                    }
                    "session_token" => {
                        static_credential.2 = Some(config_value.to_string());
                    }
                    "endpoint_url" => {
                        config.endpoint = Some(EndpointWrapper {
                            uri: Uri::from_str(config_value.as_str())
                                .map_err(|e| {
                                    anyhow!("failed to parse url ({}): {}", config_value, e)
                                })
                                .unwrap(),
                        });
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                continue;
            }
        }
        let mut session_token: Option<String> = None;
        if let Some(session_value) = static_credential.2 {
            session_token = Some(session_value);
        }
        if !static_credential.0.trim().is_empty() && !static_credential.1.trim().is_empty() {
            credential = AwsCredentialV2::Static {
                access_key: static_credential.0.clone(),
                secret_access: static_credential.1.clone(),
                session_token,
            }
        }
        config.credential = credential;
        config
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointWrapper {
    #[serde(with = "http_serde::uri")]
    pub uri: Uri,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AwsCredentialV2 {
    Static {
        access_key: String,
        secret_access: String,
        session_token: Option<String>,
    },
    ProfileName(String),
    None,
}

impl Default for AwsCredentialV2 {
    fn default() -> Self {
        AwsCredentialV2::ProfileName("default".to_string())
    }
}

pub fn s3_client(
    sdk_config: &aws_types::SdkConfig,
    config_pairs: Option<HashMap<String, u64>>,
) -> aws_sdk_s3::Client {
    let s3_config_obj = if let Some(config) = config_pairs {
        let s3_config = AwsCustomConfig::from(config);
        let retry_conf =
            aws_config::RetryConfig::standard().with_max_attempts(s3_config.retry_times);
        let timeout_conf = aws_config::timeout::Config::new().with_http_timeouts(
            Http::new()
                .with_connect_timeout(TriState::Set(s3_config.conn_timeout))
                .with_read_timeout(TriState::Set(s3_config.read_timeout)),
        );

        s3_config::Builder::from(&sdk_config.clone())
            .retry_config(retry_conf)
            .timeout_config(timeout_conf)
            .build()
    } else {
        s3_config::Config::new(sdk_config)
    };
    s3_client::Client::from_conf(s3_config_obj)
}

impl AwsConfigV2 {
    /// Load configuration from environment vars by default. The use of the authentication part is
    /// divided into the next cases:
    /// - Connection is the external data source. for example kinesis Source, S3 Source.
    ///  At this time `RisingWave` is a third party. It is reasonable to use `session_token` or
    ///  `external_id` for security reasons.
    /// - For `RisingWave` storage, internal storage formats are un visible to the client.
    ///  Just follow by default way.
    pub async fn load_config(&self, external_id: Option<String>) -> aws_types::SdkConfig {
        let region = self.build_region().await.unwrap();
        let mut cred_provider = self.build_credential_provider(region.clone()).await;
        let role_provider = self
            .build_role_provider(external_id, region.clone(), cred_provider.clone())
            .await;
        if let Some(role) = role_provider {
            cred_provider = SharedCredentialsProvider::new(role);
        }
        let mut config_loader = aws_config::from_env()
            .region(region.clone())
            .credentials_provider(cred_provider);

        if let Some(endpoint_uri) = &self.endpoint {
            let endpoint_val = Endpoint::immutable(endpoint_uri.uri.clone());
            config_loader = config_loader.endpoint_resolver(endpoint_val);
        }
        config_loader.load().await
    }

    pub async fn build_region(&self) -> Option<Region> {
        if let Some(region_name) = &self.region {
            Some(Region::new(region_name.clone()))
        } else {
            let mut region_chain = DefaultRegionChain::builder();
            if let AwsCredentialV2::ProfileName(profile_name) = &self.credential {
                region_chain = region_chain.profile_name(profile_name);
            }
            region_chain.build().region().await
        }
    }

    #[expect(clippy::unused_async)]
    async fn build_role_provider(
        &self,
        external_id: Option<String>,
        region: Region,
        credential: SharedCredentialsProvider,
    ) -> Option<AssumeRoleProvider> {
        if let Some(role_name) = &self.arn {
            let mut role = AssumeRoleProvider::builder(role_name)
                .session_name("RisingWave")
                .region(region);
            if let Some(e_id) = external_id {
                role = role.external_id(e_id.as_str());
            }
            Some(role.build(credential))
        } else {
            None
        }
    }

    async fn build_credential_provider(&self, region: Region) -> SharedCredentialsProvider {
        match &self.credential {
            AwsCredentialV2::Static {
                access_key,
                secret_access,
                session_token,
            } => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
                access_key,
                secret_access,
                session_token.as_ref().cloned(),
            )),
            AwsCredentialV2::ProfileName(profile_name) => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .profile_name(profile_name)
                    .region(region.clone())
                    .build()
                    .await,
            ),
            AwsCredentialV2::None => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .region(region.clone())
                    .build()
                    .await,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::aws_utils::AwsConfigV2;

    #[tokio::test]
    #[ignore]
    pub async fn test_default_load() {
        let aws_config = AwsConfigV2::default().load_config(None).await;
        println!("aws_config = {:?}", aws_config);
    }
}
