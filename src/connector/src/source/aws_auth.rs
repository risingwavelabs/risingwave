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

use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_types::region::Region;
use aws_types::SdkConfig;

/// A flatten cofig map for aws auth.
pub struct AwsAuthProps {
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub session_token: Option<String>,
    pub arn: Option<String>,
    pub external_id: Option<String>,
    pub profile: Option<String>,
}

impl Default for AwsAuthProps {
    fn default() -> Self {
        Self {
            region: Default::default(),
            endpoint: Default::default(),
            access_key: Default::default(),
            secret_key: Default::default(),
            session_token: Default::default(),
            arn: Default::default(),
            external_id: Default::default(),
            profile: Some("default".to_string()),
        }
    }
}

impl AwsAuthProps {
    pub fn from_pairs<'a>(iter: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut this = Self::default();
        for (key, value) in iter {
            if value.is_empty() {
                continue;
            }
            match key {
                "region" => this.region = Some(value.to_string()),
                "endpoint" => this.endpoint = Some(value.to_string()),
                "access_key" => this.access_key = Some(value.to_string()),
                "secret_access" => this.secret_key = Some(value.to_string()),
                "arn" => this.arn = Some(value.to_string()),
                "session_token" => this.session_token = Some(value.to_string()),
                "endpoint_url" => this.endpoint = Some(value.to_string()),
                _ => {}
            }
        }

        this
    }

    async fn build_region(&self) -> anyhow::Result<Region> {
        if let Some(region_name) = &self.region {
            Ok(Region::new(region_name.clone()))
        } else {
            let mut region_chain = DefaultRegionChain::builder();
            if let Some(profile_name) = &self.profile {
                region_chain = region_chain.profile_name(profile_name);
            }

            Ok(region_chain
                .build()
                .region()
                .await
                .ok_or_else(|| anyhow::format_err!("region should be provided"))?)
        }
    }

    async fn build_credential_provider(&self) -> anyhow::Result<SharedCredentialsProvider> {
        if self.access_key.is_some() && self.secret_key.is_some() {
            Ok(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    self.access_key.as_ref().unwrap(),
                    self.secret_key.as_ref().unwrap(),
                    self.session_token.clone(),
                ),
            ))
        } else {
            let region = self.build_region().await?;
            let mut chain = DefaultCredentialsChain::builder().region(region);

            if let Some(profile_name) = self.profile.as_ref() {
                chain = chain.profile_name(profile_name)
            }
            Ok(SharedCredentialsProvider::new(chain.build().await))
        }
    }

    async fn with_role_provider(
        &self,
        credential: SharedCredentialsProvider,
    ) -> anyhow::Result<SharedCredentialsProvider> {
        if let Some(role_name) = &self.arn {
            let region = self.build_region().await?;
            let mut role = AssumeRoleProvider::builder(role_name)
                .session_name("RisingWave")
                .region(region);
            if let Some(id) = &self.external_id {
                role = role.external_id(id);
            }
            Ok(SharedCredentialsProvider::new(role.build(credential)))
        } else {
            Ok(credential)
        }
    }

    pub async fn build_config(&self) -> anyhow::Result<SdkConfig> {
        let region = self.build_region().await?;
        let credentials_provider = self
            .with_role_provider(self.build_credential_provider().await?)
            .await?;
        let config_loader = aws_config::from_env()
            .region(region)
            .credentials_provider(credentials_provider);
        Ok(config_loader.load().await)
    }
}
