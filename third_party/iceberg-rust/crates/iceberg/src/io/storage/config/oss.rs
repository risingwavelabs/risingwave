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

//! Alibaba Cloud OSS storage configuration.
//!
//! This module provides configuration constants and types for Alibaba Cloud OSS storage.

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use super::StorageConfig;
use crate::Result;

/// Aliyun OSS endpoint.
pub const OSS_ENDPOINT: &str = "oss.endpoint";
/// Aliyun OSS access key ID.
pub const OSS_ACCESS_KEY_ID: &str = "oss.access-key-id";
/// Aliyun OSS access key secret.
pub const OSS_ACCESS_KEY_SECRET: &str = "oss.access-key-secret";

/// Alibaba Cloud OSS storage configuration.
///
/// This struct contains all the configuration options for connecting to Alibaba Cloud OSS.
/// Use the builder pattern via `OssConfig::builder()` to construct instances.
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TypedBuilder)]
pub struct OssConfig {
    /// OSS endpoint URL.
    #[builder(default, setter(strip_option, into))]
    pub endpoint: Option<String>,
    /// OSS access key ID.
    #[builder(default, setter(strip_option, into))]
    pub access_key_id: Option<String>,
    /// OSS access key secret.
    #[builder(default, setter(strip_option, into))]
    pub access_key_secret: Option<String>,
}

impl TryFrom<&StorageConfig> for OssConfig {
    type Error = crate::Error;

    fn try_from(config: &StorageConfig) -> Result<Self> {
        let props = config.props();

        let mut cfg = OssConfig::default();
        if let Some(endpoint) = props.get(OSS_ENDPOINT) {
            cfg.endpoint = Some(endpoint.clone());
        }
        if let Some(access_key_id) = props.get(OSS_ACCESS_KEY_ID) {
            cfg.access_key_id = Some(access_key_id.clone());
        }
        if let Some(access_key_secret) = props.get(OSS_ACCESS_KEY_SECRET) {
            cfg.access_key_secret = Some(access_key_secret.clone());
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oss_config_builder() {
        let config = OssConfig::builder()
            .endpoint("https://oss-cn-hangzhou.aliyuncs.com")
            .access_key_id("my-access-key")
            .access_key_secret("my-secret-key")
            .build();

        assert_eq!(
            config.endpoint.as_deref(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(config.access_key_id.as_deref(), Some("my-access-key"));
        assert_eq!(config.access_key_secret.as_deref(), Some("my-secret-key"));
    }

    #[test]
    fn test_oss_config_from_storage_config() {
        let storage_config = StorageConfig::new()
            .with_prop(OSS_ENDPOINT, "https://oss-cn-hangzhou.aliyuncs.com")
            .with_prop(OSS_ACCESS_KEY_ID, "my-access-key")
            .with_prop(OSS_ACCESS_KEY_SECRET, "my-secret-key");

        let oss_config = OssConfig::try_from(&storage_config).unwrap();

        assert_eq!(
            oss_config.endpoint.as_deref(),
            Some("https://oss-cn-hangzhou.aliyuncs.com")
        );
        assert_eq!(oss_config.access_key_id.as_deref(), Some("my-access-key"));
        assert_eq!(
            oss_config.access_key_secret.as_deref(),
            Some("my-secret-key")
        );
    }

    #[test]
    fn test_oss_config_empty() {
        let storage_config = StorageConfig::new();

        let oss_config = OssConfig::try_from(&storage_config).unwrap();

        assert_eq!(oss_config.endpoint, None);
        assert_eq!(oss_config.access_key_id, None);
        assert_eq!(oss_config.access_key_secret, None);
    }
}
