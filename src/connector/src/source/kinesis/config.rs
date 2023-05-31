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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_types::region::Region;
use maplit::hashmap;
use serde::{Deserialize, Serialize};

use crate::common::KinesisCommon;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsAssumeRole {
    pub(crate) arn: String,
    pub(crate) external_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct AwsConfigInfo {
    pub stream_name: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub credentials: Option<AwsCredentials>,
    pub assume_role: Option<AwsAssumeRole>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl AwsConfigInfo {
    pub async fn load(&self) -> Result<aws_types::SdkConfig> {
        let region = self
            .region
            .as_ref()
            .ok_or_else(|| anyhow::Error::msg("region should be provided"))?;
        let region = Some(Region::new(region.clone()));

        let mut credentials_provider = match &self.credentials {
            Some(AwsCredentials {
                access_key_id,
                secret_access_key,
                session_token,
            }) => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
                access_key_id,
                secret_access_key,
                session_token.clone(),
            )),
            None => SharedCredentialsProvider::new(
                DefaultCredentialsChain::builder()
                    .region(region.clone())
                    .build()
                    .await,
            ),
        };

        if let Some(AwsAssumeRole { arn, external_id }) = &self.assume_role {
            let mut role = AssumeRoleProvider::builder(arn).session_name("RisingWave");
            if let Some(region) = &region {
                role = role.region(region.clone());
            }
            if let Some(external_id) = external_id {
                role = role.external_id(external_id);
            }
            credentials_provider = SharedCredentialsProvider::new(role.build(credentials_provider));
        }

        let config_loader = aws_config::from_env()
            .region(region)
            .credentials_provider(credentials_provider);
        Ok(config_loader.load().await)
    }

    pub fn build(properties: KinesisCommon) -> Result<Self> {
        let stream_name = properties.stream_name;
        let region = properties.stream_region;

        let mut credentials: Option<AwsCredentials> = None;
        let mut assume_role: Option<AwsAssumeRole> = None;

        let (access_key, secret_key) = (
            properties.credentials_access_key,
            properties.credentials_secret_access_key,
        );
        if access_key.is_some() ^ secret_key.is_some() {
            return Err(
                anyhow!("Both Kinesis credential access key and Kinesis secret key should be provided or not provided at the same time.")
            );
        } else if let (Some(access), Some(secret)) = (access_key, secret_key) {
            credentials = Some(AwsCredentials {
                access_key_id: access,
                secret_access_key: secret,
                session_token: properties.session_token.clone(),
            });
        }

        if let Some(assume_role_arn) = properties.assume_role_arn {
            assume_role = Some(AwsAssumeRole {
                arn: assume_role_arn,
                external_id: properties.assume_role_external_id.clone(),
            })
        }

        Ok(Self {
            stream_name,
            region: Some(region),
            endpoint: properties.endpoint.clone(),
            assume_role,
            credentials,
        })
    }
}

/// This function provides a minimum configuration for testing kinesis
pub fn kinesis_demo_properties() -> HashMap<String, String> {
    let properties: HashMap<String, String> = hashmap! {
    "kinesis.stream.name".to_string() => "kinesis_test_stream".to_string(),
    "kinesis.stream.region".to_string() => "cn-north-1".to_string(),
    "connector".to_string() => "kinesis".to_string()};

    properties
}
