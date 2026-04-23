// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Amazon S3 storage configuration.
//!
//! This module provides configuration constants and types for Amazon S3 storage.
//! These are based on the [Iceberg S3 FileIO configuration](https://py.iceberg.apache.org/configuration/#s3).

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::io::is_truthy;
use crate::{Error, ErrorKind, Result};

/// S3 endpoint URL.
pub const S3_ENDPOINT: &str = "s3.endpoint";
/// S3 access key ID.
pub const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
/// S3 secret access key.
pub const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
/// S3 session token (required when using temporary credentials).
pub const S3_SESSION_TOKEN: &str = "s3.session-token";
/// S3 region.
pub const S3_REGION: &str = "s3.region";
/// Region to use for the S3 client (takes precedence over [`S3_REGION`]).
pub const CLIENT_REGION: &str = "client.region";
/// S3 Path Style Access.
pub const S3_PATH_STYLE_ACCESS: &str = "s3.path-style-access";
/// S3 Server Side Encryption Type.
pub const S3_SSE_TYPE: &str = "s3.sse.type";
/// S3 Server Side Encryption Key.
/// If S3 encryption type is kms, input is a KMS Key ID.
/// In case this property is not set, default key "aws/s3" is used.
/// If encryption type is custom, input is a custom base-64 AES256 symmetric key.
pub const S3_SSE_KEY: &str = "s3.sse.key";
/// S3 Server Side Encryption MD5.
pub const S3_SSE_MD5: &str = "s3.sse.md5";
/// If set, all AWS clients will assume a role of the given ARN, instead of using the default
/// credential chain.
pub const S3_ASSUME_ROLE_ARN: &str = "client.assume-role.arn";
/// Optional external ID used to assume an IAM role.
pub const S3_ASSUME_ROLE_EXTERNAL_ID: &str = "client.assume-role.external-id";
/// Optional session name used to assume an IAM role.
pub const S3_ASSUME_ROLE_SESSION_NAME: &str = "client.assume-role.session-name";
/// Option to skip signing requests (e.g. for public buckets/folders).
pub const S3_ALLOW_ANONYMOUS: &str = "s3.allow-anonymous";
/// Option to skip loading the credential from EC2 metadata (typically used in conjunction with
/// `S3_ALLOW_ANONYMOUS`).
pub const S3_DISABLE_EC2_METADATA: &str = "s3.disable-ec2-metadata";
/// Option to skip loading configuration from config file and the env.
pub const S3_DISABLE_CONFIG_LOAD: &str = "s3.disable-config-load";

/// Amazon S3 storage configuration.
///
/// This struct contains all the configuration options for connecting to Amazon S3.
/// Use the builder pattern via `S3Config::builder()` to construct instances.
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct S3Config {
    /// S3 endpoint URL.
    #[builder(default, setter(strip_option, into))]
    pub endpoint: Option<String>,
    /// S3 access key ID.
    #[builder(default, setter(strip_option, into))]
    pub access_key_id: Option<String>,
    /// S3 secret access key.
    #[builder(default, setter(strip_option, into))]
    pub secret_access_key: Option<String>,
    /// S3 session token.
    #[builder(default, setter(strip_option, into))]
    pub session_token: Option<String>,
    /// S3 region.
    #[builder(default, setter(strip_option, into))]
    pub region: Option<String>,
    /// Enable virtual host style (opposite of path style access).
    #[builder(default)]
    pub enable_virtual_host_style: bool,
    /// Server side encryption type.
    #[builder(default, setter(strip_option, into))]
    pub server_side_encryption: Option<String>,
    /// Server side encryption AWS KMS key ID.
    #[builder(default, setter(strip_option, into))]
    pub server_side_encryption_aws_kms_key_id: Option<String>,
    /// Server side encryption customer algorithm.
    #[builder(default, setter(strip_option, into))]
    pub server_side_encryption_customer_algorithm: Option<String>,
    /// Server side encryption customer key.
    #[builder(default, setter(strip_option, into))]
    pub server_side_encryption_customer_key: Option<String>,
    /// Server side encryption customer key MD5.
    #[builder(default, setter(strip_option, into))]
    pub server_side_encryption_customer_key_md5: Option<String>,
    /// Role ARN for assuming a role.
    #[builder(default, setter(strip_option, into))]
    pub role_arn: Option<String>,
    /// External ID for assuming a role.
    #[builder(default, setter(strip_option, into))]
    pub external_id: Option<String>,
    /// Session name for assuming a role.
    #[builder(default, setter(strip_option, into))]
    pub role_session_name: Option<String>,
    /// Allow anonymous access.
    #[builder(default)]
    pub allow_anonymous: bool,
    /// Disable EC2 metadata.
    #[builder(default)]
    pub disable_ec2_metadata: bool,
    /// Disable config load.
    #[builder(default)]
    pub disable_config_load: bool,
}

impl TryFrom<&StorageConfig> for S3Config {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();

        let mut cfg = S3Config::default();

        if let Some(endpoint) = props.get(S3_ENDPOINT) {
            cfg.endpoint = Some(endpoint.clone());
        }
        if let Some(access_key_id) = props.get(S3_ACCESS_KEY_ID) {
            cfg.access_key_id = Some(access_key_id.clone());
        }
        if let Some(secret_access_key) = props.get(S3_SECRET_ACCESS_KEY) {
            cfg.secret_access_key = Some(secret_access_key.clone());
        }
        if let Some(session_token) = props.get(S3_SESSION_TOKEN) {
            cfg.session_token = Some(session_token.clone());
        }
        if let Some(region) = props.get(S3_REGION) {
            cfg.region = Some(region.clone());
        }
        // CLIENT_REGION takes precedence over S3_REGION
        if let Some(region) = props.get(CLIENT_REGION) {
            cfg.region = Some(region.clone());
        }
        if let Some(path_style_access) = props.get(S3_PATH_STYLE_ACCESS) {
            cfg.enable_virtual_host_style = !is_truthy(path_style_access.to_lowercase().as_str());
        }
        if let Some(arn) = props.get(S3_ASSUME_ROLE_ARN) {
            cfg.role_arn = Some(arn.clone());
        }
        if let Some(external_id) = props.get(S3_ASSUME_ROLE_EXTERNAL_ID) {
            cfg.external_id = Some(external_id.clone());
        }
        if let Some(session_name) = props.get(S3_ASSUME_ROLE_SESSION_NAME) {
            cfg.role_session_name = Some(session_name.clone());
        }

        // Handle SSE configuration
        let s3_sse_key = props.get(S3_SSE_KEY).cloned();
        if let Some(sse_type) = props.get(S3_SSE_TYPE) {
            match sse_type.to_lowercase().as_str() {
                // No Server Side Encryption
                "none" => {}
                // S3 SSE-S3 encryption (S3 managed keys). https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
                "s3" => {
                    cfg.server_side_encryption = Some("AES256".to_string());
                }
                // S3 SSE KMS, either using default or custom KMS key. https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html
                "kms" => {
                    cfg.server_side_encryption = Some("aws:kms".to_string());
                    cfg.server_side_encryption_aws_kms_key_id = s3_sse_key;
                }
                // S3 SSE-C, using customer managed keys. https://docs.aws.amazon.com/AmazonS3/latest/dev/ServerSideEncryptionCustomerKeys.html
                "custom" => {
                    cfg.server_side_encryption_customer_algorithm = Some("AES256".to_string());
                    cfg.server_side_encryption_customer_key = s3_sse_key;
                    cfg.server_side_encryption_customer_key_md5 = props.get(S3_SSE_MD5).cloned();
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "Invalid {S3_SSE_TYPE}: {sse_type}. Expected one of (custom, kms, s3, none)"
                        ),
                    ));
                }
            }
        }

        if let Some(allow_anonymous) = props.get(S3_ALLOW_ANONYMOUS)
            && is_truthy(allow_anonymous.to_lowercase().as_str())
        {
            cfg.allow_anonymous = true;
        }
        if let Some(disable_ec2_metadata) = props.get(S3_DISABLE_EC2_METADATA)
            && is_truthy(disable_ec2_metadata.to_lowercase().as_str())
        {
            cfg.disable_ec2_metadata = true;
        }
        if let Some(disable_config_load) = props.get(S3_DISABLE_CONFIG_LOAD)
            && is_truthy(disable_config_load.to_lowercase().as_str())
        {
            cfg.disable_config_load = true;
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_builder() {
        let config = S3Config::builder()
            .region("us-east-1")
            .access_key_id("my-access-key")
            .secret_access_key("my-secret-key")
            .endpoint("http://localhost:9000")
            .build();

        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.access_key_id.as_deref(), Some("my-access-key"));
        assert_eq!(config.secret_access_key.as_deref(), Some("my-secret-key"));
        assert_eq!(config.endpoint.as_deref(), Some("http://localhost:9000"));
    }

    #[test]
    fn test_s3_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(S3_ACCESS_KEY_ID, "my-access-key")
            .with_prop(S3_SECRET_ACCESS_KEY, "my-secret-key")
            .with_prop(S3_ENDPOINT, "http://localhost:9000");

        let s3_config = S3Config::try_from(&storage_config).unwrap();

        assert_eq!(s3_config.region.as_deref(), Some("us-east-1"));
        assert_eq!(s3_config.access_key_id.as_deref(), Some("my-access-key"));
        assert_eq!(
            s3_config.secret_access_key.as_deref(),
            Some("my-secret-key")
        );
        assert_eq!(s3_config.endpoint.as_deref(), Some("http://localhost:9000"));
    }

    #[test]
    fn test_s3_config_client_region_precedence() {
        let storage_config = StorageConfig::new()
            .with_prop(S3_REGION, "us-east-1")
            .with_prop(CLIENT_REGION, "eu-west-1");

        let s3_config = S3Config::try_from(&storage_config).unwrap();

        // CLIENT_REGION should take precedence
        assert_eq!(s3_config.region.as_deref(), Some("eu-west-1"));
    }

    #[test]
    fn test_s3_config_path_style_access() {
        let storage_config = StorageConfig::new().with_prop(S3_PATH_STYLE_ACCESS, "true");

        let s3_config = S3Config::try_from(&storage_config).unwrap();

        // path style access = true means virtual host style = false
        assert!(!s3_config.enable_virtual_host_style);
    }

    #[test]
    fn test_s3_config_sse_kms() {
        let storage_config = StorageConfig::new()
            .with_prop(S3_SSE_TYPE, "kms")
            .with_prop(S3_SSE_KEY, "my-kms-key-id");

        let s3_config = S3Config::try_from(&storage_config).unwrap();

        assert_eq!(s3_config.server_side_encryption.as_deref(), Some("aws:kms"));
        assert_eq!(
            s3_config.server_side_encryption_aws_kms_key_id.as_deref(),
            Some("my-kms-key-id")
        );
    }

    #[test]
    fn test_s3_config_allow_anonymous() {
        let storage_config = StorageConfig::new().with_prop(S3_ALLOW_ANONYMOUS, "true");

        let s3_config = S3Config::try_from(&storage_config).unwrap();

        assert!(s3_config.allow_anonymous);
    }
}
