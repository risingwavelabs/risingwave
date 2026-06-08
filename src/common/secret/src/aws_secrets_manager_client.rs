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
use serde::Deserialize;
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
    #[serde(default, alias = "aws.region")]
    pub region: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub version_stage: Option<String>,
    #[serde(default, rename = "endpoint_url", alias = "aws.endpoint_url")]
    pub endpoint_url: Option<String>,
    #[serde(
        default,
        rename = "access_key_id",
        alias = "aws.credentials.access_key_id"
    )]
    pub access_key_id: Option<String>,
    #[serde(
        default,
        rename = "secret_access_key",
        alias = "aws.credentials.secret_access_key"
    )]
    pub secret_access_key: Option<String>,
    #[serde(
        default,
        rename = "session_token",
        alias = "aws.credentials.session_token"
    )]
    pub session_token: Option<String>,
    #[serde(default, rename = "role_arn", alias = "aws.credentials.role.arn")]
    pub role_arn: Option<String>,
    #[serde(default, alias = "aws.credentials.role.external_id")]
    pub external_id: Option<String>,
    #[serde(default, alias = "aws.profile")]
    pub profile: Option<String>,
    #[serde(skip)]
    pub access_key_id_ref: Option<PbSecretRef>,
    #[serde(skip)]
    pub secret_access_key_ref: Option<PbSecretRef>,
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
            region: non_empty_string(&backend.region),
            field: non_empty_string(&backend.field),
            version_id: non_empty_string(&backend.version_id),
            version_stage: non_empty_string(&backend.version_stage),
            endpoint_url: non_empty_string(&backend.endpoint_url),
            access_key_id: non_empty_string(&backend.access_key_id),
            secret_access_key: non_empty_string(&backend.secret_access_key),
            session_token: non_empty_string(&backend.session_token),
            role_arn: non_empty_string(&backend.role_arn),
            external_id: non_empty_string(&backend.external_id),
            profile: non_empty_string(&backend.profile),
            access_key_id_ref: backend.access_key_id_ref,
            secret_access_key_ref: backend.secret_access_key_ref,
            session_token_ref: backend.session_token_ref,
        }
    }

    pub fn to_protobuf(&self) -> secret::SecretAwsSecretsManagerBackend {
        secret::SecretAwsSecretsManagerBackend {
            secret_id: self.secret_id.clone(),
            region: self.region.clone().unwrap_or_default(),
            field: self.field.clone().unwrap_or_default(),
            version_id: self.version_id.clone().unwrap_or_default(),
            version_stage: self.version_stage.clone().unwrap_or_default(),
            endpoint_url: self.endpoint_url.clone().unwrap_or_default(),
            access_key_id: self.access_key_id.clone().unwrap_or_default(),
            secret_access_key: self.secret_access_key.clone().unwrap_or_default(),
            session_token: self.session_token.clone().unwrap_or_default(),
            role_arn: self.role_arn.clone().unwrap_or_default(),
            external_id: self.external_id.clone().unwrap_or_default(),
            profile: self.profile.clone().unwrap_or_default(),
            access_key_id_ref: self.access_key_id_ref,
            secret_access_key_ref: self.secret_access_key_ref,
            session_token_ref: self.session_token_ref,
        }
    }

    pub fn set_credential_refs(
        &mut self,
        access_key_id_ref: Option<PbSecretRef>,
        secret_access_key_ref: Option<PbSecretRef>,
        session_token_ref: Option<PbSecretRef>,
    ) {
        self.access_key_id_ref = access_key_id_ref;
        self.secret_access_key_ref = secret_access_key_ref;
        self.session_token_ref = session_token_ref;
    }

    async fn build_sdk_config(&self) -> Result<SdkConfig> {
        if self.secret_id.is_empty() {
            bail!("secret_id must not be empty");
        }
        if default_credential_disabled() && self.profile.is_some() {
            bail!("profile is not allowed when default AWS credential loading is disabled");
        }
        let region = self.build_region().await?;
        let explicit_credentials = self.build_explicit_credentials()?;
        if explicit_credentials.is_none() && default_credential_disabled() {
            bail!(
                "default AWS credential loading is disabled in this environment; please provide both access_key_id and secret_access_key, or use SECRET references for them"
            );
        }

        let mut config_loader =
            aws_config::defaults(BehaviorVersion::latest()).region(region.clone());
        if let Some(profile) = &self.profile {
            config_loader = config_loader.profile_name(profile);
        }
        if let Some(endpoint_url) = &self.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint_url);
        }
        if let Some(credentials) = explicit_credentials {
            config_loader = config_loader.credentials_provider(credentials);
        }

        let mut sdk_config = config_loader.load().await;
        if let Some(role_arn) = &self.role_arn {
            let source_credentials = sdk_config
                .credentials_provider()
                .ok_or_else(|| anyhow!("missing AWS credentials_provider for assume role"))?
                .clone();
            let role_region = region.clone();
            let mut role_builder = AssumeRoleProvider::builder(role_arn)
                .session_name(AWS_SECRETS_MANAGER_SESSION_NAME)
                .region(role_region);
            if let Some(external_id) = &self.external_id {
                role_builder = role_builder.external_id(external_id);
            }
            let role_provider = role_builder.build_from_provider(source_credentials).await;
            let mut role_config_loader = aws_config::defaults(BehaviorVersion::latest())
                .region(region)
                .credentials_provider(SharedCredentialsProvider::new(role_provider));
            if let Some(endpoint_url) = &self.endpoint_url {
                role_config_loader = role_config_loader.endpoint_url(endpoint_url);
            }
            sdk_config = role_config_loader.load().await;
        }

        Ok(sdk_config)
    }

    async fn build_region(&self) -> Result<Region> {
        if let Some(region) = &self.region {
            return Ok(Region::new(region.clone()));
        }
        if default_credential_disabled() {
            bail!("region must be provided when default AWS config loading is disabled");
        }
        let mut region_chain = DefaultRegionChain::builder();
        if let Some(profile) = &self.profile {
            region_chain = region_chain.profile_name(profile);
        }
        region_chain
            .build()
            .region()
            .await
            .context("region should be provided")
    }

    fn build_explicit_credentials(&self) -> Result<Option<SharedCredentialsProvider>> {
        let access_key_id = self.resolve_optional_secret_ref(&self.access_key_id_ref)?;
        let secret_access_key = self.resolve_optional_secret_ref(&self.secret_access_key_ref)?;
        let session_token = self.resolve_optional_secret_ref(&self.session_token_ref)?;

        let access_key_id = access_key_id.or_else(|| self.access_key_id.clone());
        let secret_access_key = secret_access_key.or_else(|| self.secret_access_key.clone());
        let session_token = session_token.or_else(|| self.session_token.clone());

        match (access_key_id, secret_access_key) {
            (Some(access_key_id), Some(secret_access_key)) => {
                Ok(Some(SharedCredentialsProvider::new(
                    Credentials::from_keys(access_key_id, secret_access_key, session_token),
                )))
            }
            (None, None) => Ok(None),
            _ => bail!("access_key_id and secret_access_key must be provided together"),
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
    let field_value = value
        .as_object()
        .and_then(|object| object.get(field))
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

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        old_value: Option<OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let old_value = env::var_os(key);
            unsafe {
                env::set_var(key, value);
            }
            Self { key, old_value }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(old_value) = &self.old_value {
                    env::set_var(self.key, old_value);
                } else {
                    env::remove_var(self.key);
                }
            }
        }
    }

    #[test]
    fn test_aws_secrets_manager_config_roundtrip() {
        let mut config: AwsSecretsManagerConfig = serde_json::from_value(json!({
            "secret_id": "arn:aws:secretsmanager:us-east-1:123456789012:secret:test-AbCdEf",
            "region": "us-east-1",
            "field": "password",
            "version_stage": "AWSCURRENT",
            "endpoint_url": "http://localhost:4566",
            "access_key_id": "test",
            "secret_access_key": "test",
            "session_token": "token",
            "role_arn": "arn:aws:iam::123456789012:role/test",
            "external_id": "rw",
            "profile": "dev"
        }))
        .unwrap();
        config.set_credential_refs(
            Some(PbSecretRef {
                secret_id: 1.into(),
                ref_as: 1,
            }),
            Some(PbSecretRef {
                secret_id: 2.into(),
                ref_as: 1,
            }),
            Some(PbSecretRef {
                secret_id: 3.into(),
                ref_as: 1,
            }),
        );

        let pb = config.to_protobuf();
        let restored = AwsSecretsManagerConfig::from_protobuf(&pb);
        assert_eq!(restored.secret_id, config.secret_id);
        assert_eq!(restored.region, config.region);
        assert_eq!(restored.field, config.field);
        assert_eq!(restored.version_stage, config.version_stage);
        assert_eq!(restored.endpoint_url, config.endpoint_url);
        assert_eq!(restored.access_key_id, config.access_key_id);
        assert_eq!(restored.secret_access_key, config.secret_access_key);
        assert_eq!(restored.session_token, config.session_token);
        assert_eq!(restored.role_arn, config.role_arn);
        assert_eq!(restored.external_id, config.external_id);
        assert_eq!(restored.profile, config.profile);
        assert_eq!(restored.access_key_id_ref, config.access_key_id_ref);
        assert_eq!(restored.secret_access_key_ref, config.secret_access_key_ref);
        assert_eq!(restored.session_token_ref, config.session_token_ref);
    }

    #[test]
    fn test_extract_secret_string_without_field() {
        assert_eq!(
            extract_secret_value(Some("raw-secret"), None, None).unwrap(),
            b"raw-secret"
        );
    }

    #[test]
    fn test_extract_secret_string_json_field() {
        assert_eq!(
            extract_secret_value(
                Some(r#"{"password":"rw","port":4566}"#),
                None,
                Some("password")
            )
            .unwrap(),
            b"rw"
        );
        assert_eq!(
            extract_secret_value(Some(r#"{"password":"rw","port":4566}"#), None, Some("port"))
                .unwrap(),
            b"4566"
        );
    }

    #[test]
    fn test_extract_missing_field() {
        let error = extract_secret_value(Some(r#"{"password":"rw"}"#), None, Some("username"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("Field 'username' not found in secret"));
    }

    #[test]
    fn test_extract_secret_binary() {
        assert_eq!(
            extract_secret_value(None, Some(b"binary-secret"), None).unwrap(),
            b"binary-secret"
        );
    }

    #[test]
    fn test_aws_region_alias() {
        let config: AwsSecretsManagerConfig = serde_json::from_value(json!({
            "secret_id": "test",
            "aws.region": "us-east-1"
        }))
        .unwrap();
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn test_unknown_option_is_rejected() {
        let result = serde_json::from_value::<AwsSecretsManagerConfig>(json!({
            "secret_id": "test",
            "unexpected": "value"
        }));
        let error = match result {
            Ok(_) => panic!("unknown AWS Secrets Manager option should be rejected"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("unknown field"));
    }

    #[test]
    fn test_cloud_safe_mode_rejects_default_credential_loading() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env_guard = EnvGuard::set(DISABLE_DEFAULT_CREDENTIAL, "true");
        let config: AwsSecretsManagerConfig = serde_json::from_value(json!({
            "secret_id": "test",
            "region": "us-east-1"
        }))
        .unwrap();

        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(config.build_sdk_config())
            .unwrap_err()
            .to_string();

        assert!(error.contains("default AWS credential loading is disabled"));
    }

    #[test]
    fn test_cloud_safe_mode_rejects_profile() {
        let _guard = ENV_LOCK.lock().unwrap();
        let _env_guard = EnvGuard::set(DISABLE_DEFAULT_CREDENTIAL, "true");
        let config: AwsSecretsManagerConfig = serde_json::from_value(json!({
            "secret_id": "test",
            "region": "us-east-1",
            "profile": "dev",
            "access_key_id": "ak",
            "secret_access_key": "sk"
        }))
        .unwrap();

        let error = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(config.build_sdk_config())
            .unwrap_err()
            .to_string();

        assert!(error.contains("profile is not allowed"));
    }
}
