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

cfg_if::cfg_if! {
    if #[cfg(feature = "sink-deltalake")] {
        mod imp;
        pub use imp::*;
    }
}

use std::collections::BTreeMap;

use anyhow::anyhow;
use phf::{Set, phf_set};
use serde_derive::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use with_options::WithOptions;

use crate::connector_common::AwsAuthProps;
use crate::enforce_secret::{EnforceSecret, EnforceSecretError};
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::prelude::*;

pub const DELTALAKE_SINK: &str = "deltalake";
pub const DEFAULT_REGION: &str = "us-east-1";
pub const GCS_SERVICE_ACCOUNT: &str = "service_account_key";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct DeltaLakeCommon {
    #[serde(rename = "location")]
    pub location: String,
    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(rename = "gcs.service.account")]
    pub gcs_service_account: Option<String>,
    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    pub commit_checkpoint_interval: u64,
}

impl EnforceSecret for DeltaLakeCommon {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "gcs.service.account",
    };

    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        AwsAuthProps::enforce_one(prop)?;
        if Self::ENFORCE_SECRET_PROPERTIES.contains(prop) {
            return Err(EnforceSecretError {
                key: prop.to_owned(),
            }
            .into());
        }

        Ok(())
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct DeltaLakeConfig {
    #[serde(flatten)]
    pub common: DeltaLakeCommon,

    pub r#type: String,
}

impl EnforceSecret for DeltaLakeConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        DeltaLakeCommon::enforce_one(prop)
    }
}

impl DeltaLakeConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<DeltaLakeConfig>(
            serde_json::to_value(properties).map_err(|e| SinkError::DeltaLake(e.into()))?,
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct DeltaLakeSink {
    pub config: DeltaLakeConfig,
    #[cfg_attr(not(feature = "sink-deltalake"), allow(dead_code))]
    param: SinkParam,
}

impl EnforceSecret for DeltaLakeSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            DeltaLakeCommon::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl DeltaLakeSink {
    pub fn new(config: DeltaLakeConfig, param: SinkParam) -> Result<Self> {
        Ok(Self { config, param })
    }
}

/// Dummy implementation
#[cfg(not(feature = "sink-deltalake"))]
mod dummy {
    use crate::sink::prelude::*;
    use crate::sink::utils::dummy::{FeatureNotEnabledSinkMarker, err_feature_not_enabled};

    impl FeatureNotEnabledSinkMarker for super::DeltaLakeSink {
        const SINK_NAME: &'static str = super::DELTALAKE_SINK;
    }

    impl TryFrom<SinkParam> for super::DeltaLakeSink {
        type Error = SinkError;

        fn try_from(_param: SinkParam) -> std::result::Result<Self, Self::Error> {
            Err(err_feature_not_enabled(super::DELTALAKE_SINK))
        }
    }
}
