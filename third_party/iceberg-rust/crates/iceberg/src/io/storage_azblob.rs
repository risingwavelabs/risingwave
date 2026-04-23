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
//! Azure blob storage properties
use std::collections::HashMap;

use opendal::Operator;
use opendal::services::AzblobConfig;
use url::Url;

use crate::{Error, ErrorKind, Result};

/// Azure blob account name.
pub const AZBLOB_ACCOUNT_NAME: &str = "azblob.account-name";
/// Azure blob account key.
pub const AZBLOB_ACCOUNT_KEY: &str = "azblob.account-key";
/// Azure blob account endpoint.
pub const AZBLOB_ENDPOINT: &str = "azblob.endpoint";

/// Parse iceberg properties to [`AzblobConfig`].
pub(crate) fn azblob_config_parse(mut m: HashMap<String, String>) -> Result<AzblobConfig> {
    let mut cfg = AzblobConfig::default();

    if let Some(account_name) = m.remove(AZBLOB_ACCOUNT_NAME) {
        cfg.account_name = Some(account_name);
    };
    if let Some(account_key) = m.remove(AZBLOB_ACCOUNT_KEY) {
        cfg.account_key = Some(account_key);
    };
    if let Some(endpoint) = m.remove(AZBLOB_ENDPOINT) {
        cfg.endpoint = Some(endpoint);
    };

    Ok(cfg)
}

/// Build a new OpenDAL [`Operator`] based on a provided [`AzblobConfig`].
pub(crate) fn azblob_config_build(cfg: &AzblobConfig, path: &str) -> Result<Operator> {
    let url = Url::parse(path)?;
    let container = url.host_str().ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("Invalid azblob url: {path}, container is required"),
        )
    })?;

    let mut cfg = cfg.clone();
    cfg.container = container.to_string();
    Ok(Operator::from_config(cfg)?.finish())
}
