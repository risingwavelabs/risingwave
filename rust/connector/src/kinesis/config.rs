use anyhow::Result;
use aws_types::credentials::SharedCredentialsProvider;
use aws_types::region::Region;
use aws_types::Credentials;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AwsConfigInfo {
    pub region: String,
    pub endpoint: Option<String>,
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
        let mut config_loader = aws_config::from_env().region(Region::new(self.region.clone()));
        if let Some(credentials) = &self.credentials {
            config_loader = config_loader.credentials_provider(SharedCredentialsProvider::new(
                Credentials::from_keys(
                    &credentials.access_key_id,
                    &credentials.secret_access_key,
                    credentials.session_token.clone(),
                ),
            ));
        }
        // let mut config = AwsConfig {
        //   endpoint: None,
        //   config: Arc::new(config_loader.load().await),
        // };
        // if let Some(ep) = &self.endpoint {
        //   let endpoint = ep.parse::<Uri>().context("aws endpoint")?;
        //   config.endpoint = Some(Endpoint::immutable(endpoint));
        // }

        Ok(config_loader.load().await)
    }
}
