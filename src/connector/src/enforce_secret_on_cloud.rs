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

use phf::{Set, phf_set};
use risingwave_common::util::deployment::Deployment;

use crate::error::ConnectorResult as Result;

#[derive(Debug, thiserror::Error)]
#[error("{key} is enforced to be a SECRET on RisingWave Cloud, please use `CREATE SECRET` first")]
pub struct EnforceSecretOnCloudError {
    key: String,
}

pub trait EnforceSecretOnCloud {
    const ENFORCE_SECRET_PROPERTIES_ON_CLOUD: Set<&'static str> = phf_set! {};

    fn enforce_secret_on_cloud<'a>(prop_iter: impl Iterator<Item = &'a str>) -> Result<()> {
        if !Deployment::is_cloud_hosted() {
            return Ok(());
        }
        for prop in prop_iter {
            if Self::ENFORCE_SECRET_PROPERTIES_ON_CLOUD.contains(prop) {
                return Err(EnforceSecretOnCloudError {
                    key: prop.to_owned(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn enforce_one(prop: &str) -> Result<()> {
        if !Deployment::is_cloud_hosted() {
            return Ok(());
        }
        if Self::ENFORCE_SECRET_PROPERTIES_ON_CLOUD.contains(prop) {
            return Err(EnforceSecretOnCloudError {
                key: prop.to_owned(),
            }
            .into());
        }
        Ok(())
    }
}
