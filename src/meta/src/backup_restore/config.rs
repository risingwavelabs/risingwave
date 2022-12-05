// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use serde::{Deserialize, Serialize};

/// Configs for meta node backup
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupConfig {
    /// Remote storage url for storing snapshots.
    #[serde(default = "default::storage_url")]
    pub storage_url: String,
    /// Remote directory for storing snapshots.
    #[serde(default = "default::storage_directory")]
    pub storage_directory: String,
}

impl Default for BackupConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

mod default {
    pub fn storage_url() -> String {
        "memory".to_string()
    }
    pub fn storage_directory() -> String {
        "backup".to_string()
    }
}
