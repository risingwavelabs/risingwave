use anyhow::Result;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::default_provider::region::DefaultRegionChain;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use aws_types::Credentials;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsConfigInfo {
    pub stream_name: String,
    pub region: Option<String>,
    pub credentials: Option<AwsCredentials>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

// pub struct AwsConfig {
//   pub endpoint: Option<Endpoint>,
//   pub config: Arc<aws_config::Config>,
// }

impl AwsConfigInfo {
    pub async fn load(&self) -> Result<aws_config::Config> {
        let region = match &self.region {
            Some(region) => Some(Region::new(region.clone())),
            None => {
                let region_chain = DefaultRegionChain::builder();
                region_chain.build().region().await
            }
        };

        let credentials_provider = match &self.credentials {
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

    #[tokio::test]
    #[ignore]
    async fn test_config_default() {
        let demo_config = AwsConfigInfo {
            stream_name: STREAM_NAME.to_string(),
            region: Some("cn-north-1".to_string()),
            credentials: None,
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
        };

        demo_config.load().await.unwrap();
    }
}
