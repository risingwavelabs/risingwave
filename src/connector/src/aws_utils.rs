// Copyright 2025 RisingWave Labs
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
use std::time::Duration;

use anyhow::{Context, anyhow};
use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::{client as s3_client, config as s3_config};
use url::Url;

use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;

const AWS_CUSTOM_CONFIG_KEY: [&str; 3] = ["retry_times", "conn_timeout", "read_timeout"];

pub fn default_conn_config() -> HashMap<String, u64> {
    let mut default_conn_config = HashMap::new();
    default_conn_config.insert("retry_times".to_owned(), 3_u64);
    default_conn_config.insert("conn_timeout".to_owned(), 3_u64);
    default_conn_config.insert("read_timeout".to_owned(), 5_u64);
    default_conn_config
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AwsCustomConfig {
    pub read_timeout: Duration,
    pub conn_timeout: Duration,
    pub retry_times: u32,
}

impl Default for AwsCustomConfig {
    fn default() -> Self {
        let map = default_conn_config();
        AwsCustomConfig::from(map)
    }
}

impl From<HashMap<String, u64>> for AwsCustomConfig {
    fn from(input_config: HashMap<String, u64>) -> Self {
        let mut config = AwsCustomConfig {
            read_timeout: Duration::from_secs(3),
            conn_timeout: Duration::from_secs(3),
            retry_times: 0,
        };
        for key in AWS_CUSTOM_CONFIG_KEY {
            let value = input_config.get(key);
            if let Some(config_value) = value {
                match key {
                    "retry_times" => {
                        config.retry_times = *config_value as u32;
                    }
                    "conn_timeout" => {
                        config.conn_timeout = Duration::from_secs(*config_value);
                    }
                    "read_timeout" => {
                        config.read_timeout = Duration::from_secs(*config_value);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                continue;
            }
        }
        config
    }
}

pub fn s3_client(
    sdk_config: &aws_types::SdkConfig,
    config_pairs: Option<HashMap<String, u64>>,
) -> aws_sdk_s3::Client {
    let s3_config_obj = if let Some(config) = config_pairs {
        let s3_config = AwsCustomConfig::from(config);
        let retry_conf =
            aws_config::retry::RetryConfig::standard().with_max_attempts(s3_config.retry_times);
        let timeout_conf = TimeoutConfig::builder()
            .connect_timeout(s3_config.conn_timeout)
            .read_timeout(s3_config.read_timeout)
            .build();

        s3_config::Builder::from(&sdk_config.clone())
            .retry_config(retry_conf)
            .timeout_config(timeout_conf)
            .force_path_style(true)
            .build()
    } else {
        s3_config::Config::new(sdk_config)
    };
    s3_client::Client::from_conf(s3_config_obj)
}

// TODO(Tao): Probably we should never allow to use S3 URI.
pub async fn load_file_descriptor_from_s3(
    location: &Url,
    config: &AwsAuthProps,
) -> ConnectorResult<Vec<u8>> {
    let bucket = location
        .domain()
        .with_context(|| format!("illegal file path {}", location))?;
    let key = location
        .path()
        .strip_prefix('/')
        .ok_or_else(|| anyhow!("s3 url {location} should have a '/' at the start of path."))?;
    let sdk_config = config.build_config().await?;
    let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
    let response = s3_client
        .get_object()
        .bucket(bucket.to_owned())
        .key(key)
        .send()
        .await
        .with_context(|| format!("failed to get file from s3 at `{}`", location))?;

    let body = response
        .body
        .collect()
        .await
        .with_context(|| format!("failed to read file from s3 at `{}`", location))?;
    Ok(body.into_bytes().to_vec())
}
