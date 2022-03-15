use std::collections::HashMap;

use anyhow::Result;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::sts::AssumeRoleProvider;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::RwError;
use serde::{Deserialize, Serialize};

use crate::ConnectorConfig;

const KINESIS_STREAM_NAME: &str = "kinesis.stream.name";
const KINESIS_STREAM_REGION: &str = "kinesis.stream.region";
const KINESIS_ENDPOINT: &str = "kinesis.endpoint";
const KINESIS_CREDENTIALS_ACCESS_KEY: &str = "kinesis.credentials.access";
const KINESIS_CREDENTIALS_SECRET_ACCESS_KEY: &str = "kinesis.credentials.secret";
const KINESIS_CREDENTIALS_SESSION_TOKEN: &str = "kinesis.credentials.session_token";
const KINESIS_ASSUMEROLE_ARN: &str = "kinesis.assumerole.arn";
const KINESIS_ASSUMEROLE_EXTERNAL_ID: &str = "kinesis.assumerole.external_id";

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
    pub async fn load(&self) -> Result<aws_config::Config> {
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

    pub fn build(
        properties: &HashMap<String, String>,
    ) -> risingwave_common::error::Result<ConnectorConfig> {
        let stream_name =
            properties
                .get(KINESIS_STREAM_NAME)
                .ok_or(RwError::from(ProtocolError(
                    "Kinesis stream name should be provided.".into(),
                )))?;
        let region = properties
            .get(KINESIS_STREAM_REGION)
            .ok_or(RwError::from(ProtocolError(
                "Kinesis region should be provided.".into(),
            )))?;

        let mut credentials: Option<AwsCredentials> = None;
        let mut assume_role: Option<AwsAssumeRole> = None;

        let (access_key, secret_key) = (
            properties.get(KINESIS_CREDENTIALS_ACCESS_KEY),
            properties.get(KINESIS_CREDENTIALS_SECRET_ACCESS_KEY),
        );
        if access_key.is_some() ^ secret_key.is_some() {
            return Err(
                RwError::from(ProtocolError("Both Kinesis credential access key and Kinesis secret key should be provided or not provided at the same time.".into()))
            );
        } else if let (Some(access), Some(secret)) = (access_key, secret_key) {
            let x = properties.get(KINESIS_CREDENTIALS_SESSION_TOKEN).cloned();
            credentials = Some(AwsCredentials {
                access_key_id: access.clone(),
                secret_access_key: secret.clone(),
                session_token: properties.get(KINESIS_CREDENTIALS_SESSION_TOKEN).cloned(),
            });
        }

        if let Some(assume_role_arn) = properties.get(KINESIS_ASSUMEROLE_ARN) {
            assume_role = Some(AwsAssumeRole {
                arn: assume_role_arn.clone(),
                external_id: properties.get(KINESIS_ASSUMEROLE_EXTERNAL_ID).cloned(),
            })
        }

        Ok(ConnectorConfig::Kinesis(Self {
            stream_name: stream_name.clone(),
            region: Some(region.clone()),
            endpoint: properties.get(KINESIS_ENDPOINT).cloned(),
            assume_role,
            credentials,
        }))
    }
}
