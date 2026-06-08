// Copyright 2026 RisingWave Labs
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

use std::env;

use anyhow::{Context, Result, anyhow, bail};
use aws_config::BehaviorVersion;
use aws_config::default_provider::region::DefaultRegionChain;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_secretsmanager::Client;
use aws_types::SdkConfig;
use aws_types::region::Region;
use risingwave_pb::secret;
use risingwave_pb::secret::PbSecretRef;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::LocalSecretManager;

/// The environment variable to disable using default AWS credentials from the environment.
///
/// Keep this in sync with the connector AWS auth behavior. It is expected to be set to `true` in
/// RisingWave Cloud so that AWS credentials must be provided explicitly instead of being loaded
/// from the process environment, shared config, container credentials, or EC2 IMDS.
const DISABLE_DEFAULT_CREDENTIAL: &str = "DISABLE_DEFAULT_CREDENTIAL";
const AWS_SECRETS_MANAGER_SESSION_NAME: &str = "RisingWave";

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsSecretsManagerConfig {
    pub secret_id: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub version_stage: Option<String>,
    #[serde(flatten)]
    pub aws_config: AwsSecretsManagerAwsConfig,
}

#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsSecretsManagerAwsConfig {
    #[serde(rename = "s3.region_name", alias = "aws.region", alias = "region")]
    pub region_name: Option<String>,
    #[serde(default, rename = "endpoint_url", alias = "aws.endpoint_url")]
    #[serde(alias = "s3.endpoint_url", alias = "s3.endpoint")]
    pub endpoint_url: Option<String>,
    #[serde(rename = "s3.credentials.access")]
    #[serde(alias = "access_key_id", alias = "aws.credentials.access_key_id")]
    pub access: Option<String>,
    #[serde(rename = "s3.credentials.secret")]
    #[serde(
        alias = "secret_access_key",
        alias = "aws.credentials.secret_access_key"
    )]
    pub secret: Option<String>,
    #[serde(
        default,
        rename = "session_token",
        alias = "aws.credentials.session_token"
    )]
    pub session_token: Option<String>,
    #[serde(rename = "s3.assume_role")]
    #[serde(alias = "role_arn", alias = "aws.credentials.role.arn")]
    pub assume_role: Option<String>,
    #[serde(default, alias = "aws.credentials.role.external_id")]
    pub external_id: Option<String>,
    #[serde(default, alias = "aws.profile")]
    pub profile: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_bool_from_string")]
    pub enable_config_load: Option<bool>,
    #[serde(skip)]
    pub access_ref: Option<PbSecretRef>,
    #[serde(skip)]
    pub secret_ref: Option<PbSecretRef>,
    #[serde(skip)]
    pub session_token_ref: Option<PbSecretRef>,
}

pub struct AwsSecretsManagerClient {
    client: Client,
    config: AwsSecretsManagerConfig,
}

impl AwsSecretsManagerConfig {
    pub fn from_protobuf(backend: &secret::SecretAwsSecretsManagerBackend) -> Self {
        Self {
            secret_id: backend.secret_id.clone(),
            field: non_empty_string(&backend.field),
            version_id: non_empty_string(&backend.version_id),
            version_stage: non_empty_string(&backend.version_stage),
            aws_config: AwsSecretsManagerAwsConfig::from_protobuf(backend),
        }
    }

    pub fn to_protobuf(&self) -> secret::SecretAwsSecretsManagerBackend {
        secret::SecretAwsSecretsManagerBackend {
            secret_id: self.secret_id.clone(),
            field: self.field.clone().unwrap_or_default(),
            version_id: self.version_id.clone().unwrap_or_default(),
            version_stage: self.version_stage.clone().unwrap_or_default(),
            region_name: self.aws_config.region_name.clone().unwrap_or_default(),
            endpoint_url: self.aws_config.endpoint_url.clone().unwrap_or_default(),
            access: self.aws_config.access.clone().unwrap_or_default(),
            secret: self.aws_config.secret.clone().unwrap_or_default(),
            session_token: self.aws_config.session_token.clone().unwrap_or_default(),
            assume_role: self.aws_config.assume_role.clone().unwrap_or_default(),
            external_id: self.aws_config.external_id.clone().unwrap_or_default(),
            profile: self.aws_config.profile.clone().unwrap_or_default(),
            access_ref: self.aws_config.access_ref,
            secret_ref: self.aws_config.secret_ref,
            session_token_ref: self.aws_config.session_token_ref,
            enable_config_load: self.aws_config.enable_config_load.unwrap_or(false),
        }
    }

    pub fn set_credential_refs(
        &mut self,
        access_key_id_ref: Option<PbSecretRef>,
        secret_access_key_ref: Option<PbSecretRef>,
        session_token_ref: Option<PbSecretRef>,
    ) {
        self.aws_config.access_ref = access_key_id_ref;
        self.aws_config.secret_ref = secret_access_key_ref;
        self.aws_config.session_token_ref = session_token_ref;
    }

    async fn build_sdk_config(&self) -> Result<SdkConfig> {
        if self.secret_id.is_empty() {
            bail!("secret_id must not be empty");
        }
        if !self.aws_config.enable_config_load() && self.aws_config.profile.is_some() {
            bail!("profile is not allowed when AWS config loading is disabled");
        }
        let region = self.build_region().await?;
        let explicit_credentials = self.build_explicit_credentials()?;
        if explicit_credentials.is_none() && !self.aws_config.enable_config_load() {
            bail!(
                "AWS config loading is disabled; please provide both s3.credentials.access and s3.credentials.secret, use SECRET references for them, or set enable_config_load = true"
            );
        }

        let mut config_loader =
            aws_config::defaults(BehaviorVersion::latest()).region(region.clone());
        if let Some(profile) = &self.aws_config.profile {
            config_loader = config_loader.profile_name(profile);
        }
        if let Some(endpoint_url) = &self.aws_config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }
        if let Some(credentials) = explicit_credentials {
            config_loader = config_loader.credentials_provider(credentials);
        }

        let mut sdk_config = config_loader.load().await;
        if let Some(role_arn) = &self.aws_config.assume_role {
            let source_credentials = sdk_config
                .credentials_provider()
                .ok_or_else(|| anyhow!("missing AWS credentials_provider for assume role"))?;
            let role_region = region.clone();
            let mut role_builder = AssumeRoleProvider::builder(role_arn)
                .session_name(AWS_SECRETS_MANAGER_SESSION_NAME)
                .region(role_region);
            if let Some(external_id) = &self.aws_config.external_id {
                role_builder = role_builder.external_id(external_id);
            }
            let role_provider = role_builder.build_from_provider(source_credentials).await;
            let mut role_config_loader = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .credentials_provider(SharedCredentialsProvider::new(role_provider));
            if let Some(endpoint_url) = &self.aws_config.endpoint_url {
                role_config_loader = role_config_loader.endpoint_url(endpoint_url);
            }
            sdk_config = role_config_loader.load().await;
        }

        Ok(sdk_config)
    }

    async fn build_region(&self) -> Result<Region> {
        if let Some(region) = &self.aws_config.region_name {
            return Ok(Region::new(region.clone()));
        }
        if !self.aws_config.enable_config_load() {
            bail!("s3.region_name must be provided when AWS config loading is disabled");
        }
        let mut region_chain = DefaultRegionChain::builder();
        if let Some(profile) = &self.aws_config.profile {
            region_chain = region_chain.profile_name(profile);
        }
        region_chain
            .build()
            .region()
            .await
            .context("region should be provided")
    }

    fn build_explicit_credentials(&self) -> Result<Option<SharedCredentialsProvider>> {
        let access = self.resolve_optional_secret_ref(&self.aws_config.access_ref)?;
        let secret = self.resolve_optional_secret_ref(&self.aws_config.secret_ref)?;
        let session_token = self.resolve_optional_secret_ref(&self.aws_config.session_token_ref)?;

        let access = access.or_else(|| self.aws_config.access.clone());
        let secret = secret.or_else(|| self.aws_config.secret.clone());
        let session_token = session_token.or_else(|| self.aws_config.session_token.clone());

        match (access, secret) {
            (Some(access), Some(secret)) => Ok(Some(SharedCredentialsProvider::new(
                Credentials::from_keys(access, secret, session_token),
            ))),
            (None, None) => Ok(None),
            _ => bail!("s3.credentials.access and s3.credentials.secret must be provided together"),
        }
    }

    fn resolve_optional_secret_ref(
        &self,
        secret_ref: &Option<PbSecretRef>,
    ) -> Result<Option<String>> {
        secret_ref
            .map(|secret_ref| LocalSecretManager::global().fill_secret(secret_ref))
            .transpose()
            .map_err(Into::into)
    }
}

impl AwsSecretsManagerAwsConfig {
    fn from_protobuf(backend: &secret::SecretAwsSecretsManagerBackend) -> Self {
        Self {
            region_name: non_empty_string(&backend.region_name),
            endpoint_url: non_empty_string(&backend.endpoint_url),
            access: non_empty_string(&backend.access),
            secret: non_empty_string(&backend.secret),
            session_token: non_empty_string(&backend.session_token),
            assume_role: non_empty_string(&backend.assume_role),
            external_id: non_empty_string(&backend.external_id),
            profile: non_empty_string(&backend.profile),
            enable_config_load: backend.enable_config_load.then_some(true),
            access_ref: backend.access_ref,
            secret_ref: backend.secret_ref,
            session_token_ref: backend.session_token_ref,
        }
    }

    fn enable_config_load(&self) -> bool {
        if default_credential_disabled() {
            return false;
        }
        self.enable_config_load.unwrap_or(false)
    }
}

impl AwsSecretsManagerClient {
    pub async fn new(config: AwsSecretsManagerConfig) -> Result<Self> {
        let sdk_config = config.build_sdk_config().await?;
        let client = Client::new(&sdk_config);
        Ok(Self { client, config })
    }

    pub async fn get_secret(&self) -> Result<Vec<u8>> {
        let mut request = self
            .client
            .get_secret_value()
            .secret_id(self.config.secret_id.clone());
        if let Some(version_id) = &self.config.version_id {
            request = request.version_id(version_id);
        }
        if let Some(version_stage) = &self.config.version_stage {
            request = request.version_stage(version_stage);
        }
        let response = request.send().await.with_context(|| {
            format!(
                "failed to get AWS Secrets Manager secret `{}`",
                self.config.secret_id
            )
        })?;

        let secret_binary = response.secret_binary().map(|blob| blob.as_ref());
        extract_secret_value(
            response.secret_string(),
            secret_binary,
            self.config.field.as_deref(),
        )
    }
}

fn extract_secret_value(
    secret_string: Option<&str>,
    secret_binary: Option<&[u8]>,
    field: Option<&str>,
) -> Result<Vec<u8>> {
    let secret_bytes = if let Some(secret_string) = secret_string {
        secret_string.as_bytes().to_vec()
    } else if let Some(secret_binary) = secret_binary {
        secret_binary.to_vec()
    } else {
        bail!("AWS Secrets Manager response contains neither SecretString nor SecretBinary");
    };

    let Some(field) = field else {
        return Ok(secret_bytes);
    };

    let value: Value = serde_json::from_slice(&secret_bytes).with_context(|| {
        format!("field `{field}` requires the secret value to be a JSON object")
    })?;
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("field `{field}` requires the secret value to be a JSON object"))?;
    let field_value = object
        .get(field)
        .ok_or_else(|| anyhow!("Field '{}' not found in secret", field))?;

    match field_value {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        _ => serde_json::to_vec(field_value).context("failed to serialize field value to bytes"),
    }
}

fn non_empty_string(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

fn default_credential_disabled() -> bool {
    env::var(DISABLE_DEFAULT_CREDENTIAL)
        .map(|value| {
            ["1", "t", "true"]
                .iter()
                .any(|s| value.eq_ignore_ascii_case(s))
        })
        .unwrap_or(false)
}

fn deserialize_optional_bool_from_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    value
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}
