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

use super::hba::HbaConfig;
use super::*;

/// The section [`frontend`] in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FrontendConfig {
    /// Total memory constraints for running queries.
    #[serde(default = "default::frontend::max_total_query_size_bytes")]
    pub max_total_query_size_bytes: u64,

    /// A query of size under this threshold will never be rejected due to memory constraints.
    #[serde(default = "default::frontend::min_single_query_size_bytes")]
    pub min_single_query_size_bytes: u64,

    /// A query of size exceeding this threshold will always be rejected due to memory constraints.
    #[serde(default = "default::frontend::max_single_query_size_bytes")]
    pub max_single_query_size_bytes: u64,

    /// Host-based authentication configuration
    #[serde(default = "HbaConfig::default")]
    pub hba_config: HbaConfig,
}

pub mod default {

    pub mod frontend {
        pub fn max_total_query_size_bytes() -> u64 {
            1024 * 1024 * 1024
        }

        pub fn min_single_query_size_bytes() -> u64 {
            1024 * 1024
        }

        pub fn max_single_query_size_bytes() -> u64 {
            1024 * 1024 * 1024
        }
    }
}
