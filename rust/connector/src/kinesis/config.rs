use anyhow::Result;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::sts::AssumeRoleProvider;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]

pub struct AwsAssumeRole {
    pub(crate) arn: String,
    pub(crate) external_id: Option<String>,
}

pub struct AwsConfigInfo {
    pub(crate) stream_name: String,
    pub(crate) region: Option<String>,
    pub(crate) credentials: Option<AwsCredentials>,
    pub(crate) assume_role: Option<AwsAssumeRole>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl AwsConfigInfo {
    pub async fn load(&self) -> Result<aws_config::Config> {
        let region = match &self.region {
            Some(region) => Some(Region::new(region.clone())),
            None => {
                return Err(anyhow::Error::msg("region should be provided"));
            }
        };

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
}

#[cfg(test)]
mod tests {
    use super::*;

    const STREAM_NAME: &str = "kinesis_test_stream";
    const KINESIS_ROLE_ARN: &str = "";

    #[tokio::test]
    #[ignore]
    async fn test_config_default() {
        let demo_config = AwsConfigInfo {
            stream_name: STREAM_NAME.into(),
            region: Some("cn-north-1".to_string()),
            credentials: None,
            assume_role: None,
        };
        demo_config.load().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_config_without_region() {
        let demo_config = AwsConfigInfo {
            stream_name: STREAM_NAME.to_string(),
            region: None,
            credentials: None,
            assume_role: None,
        };

        demo_config.load().await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test() {
        let demo_config = AwsConfigInfo {
            stream_name: STREAM_NAME.into(),
            region: Some("cn-north-1".to_string()),
            credentials: None,
            assume_role: Some(AwsAssumeRole {
                arn: KINESIS_ROLE_ARN.into(),
                external_id: None,
            }),
        };
        let config = demo_config.load().await.unwrap();
        println!("config {:#?}", config);
        let client = aws_sdk_kinesis::Client::new(&config);

        let resp = client
            .describe_stream()
            .stream_name(&demo_config.stream_name)
            .send()
            .await;
        println!("{:#?}", resp);
    }
}
