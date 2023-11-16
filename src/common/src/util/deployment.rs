// Copyright 2023 RisingWave Labs
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

use enum_as_inner::EnumAsInner;

use super::env_var::env_var_is_true;

/// The deployment environment detected from environment variables.
#[derive(Debug, Clone, Copy, EnumAsInner)]
pub enum Deployment {
    /// Running in CI.
    Ci,
    /// Running in cloud.
    Cloud,
    /// Running in other environments.
    // TODO: may distinguish between local development and on-premises production.
    Other,
}

impl Deployment {
    /// Returns the deployment environment detected from current environment variables.
    pub fn current() -> Self {
        if env_var_is_true("RISINGWAVE_CI") {
            Self::Ci
        } else if env_var_is_true("RISINGWAVE_CLOUD") {
            Self::Cloud
        } else {
            Self::Other
        }
    }
}
