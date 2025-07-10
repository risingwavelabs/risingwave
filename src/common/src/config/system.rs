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

use risingwave_license::LicenseKey;
use risingwave_pb::meta::SystemParams;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;
use paste::paste;

use crate::{for_all_params, system_param::AdaptiveParallelismStrategy};

macro_rules! define_system_config {
    ($(
        {
            $field:ident,
            $type:ty,
            $default:expr,
            $is_mutable:expr,
            $doc:literal,
            $($rest:tt)*
        },
    )*) => {
        #[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
        pub struct SystemConfig {
            $(
                #[doc = $doc]
                #[serde(default = "default::system::" $field "_opt")]
                pub $field: Option<$type>,
            )*
        }
    };
}

for_all_params!(define_system_config);

impl SystemConfig {
    #![allow(deprecated)]
    pub fn into_init_system_params(self) -> SystemParams {
        macro_rules! fields {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                SystemParams {
                    $($field: self.$field.map(Into::into),)*
                    ..Default::default() // deprecated fields
                }
            };
        }

        let mut system_params = for_all_params!(fields);

        // Initialize backup_storage_url and backup_storage_directory if not set.
        if let Some(state_store) = &system_params.state_store
            && let Some(data_directory) = &system_params.data_directory
        {
            if system_params.backup_storage_url.is_none() {
                if let Some(hummock_state_store) = state_store.strip_prefix("hummock+") {
                    system_params.backup_storage_url = Some(hummock_state_store.to_owned());
                } else {
                    system_params.backup_storage_url = Some("memory".to_owned());
                }
                tracing::info!("initialize backup_storage_url based on state_store");
            }
            if system_params.backup_storage_directory.is_none() {
                system_params.backup_storage_directory = Some(format!("{data_directory}/backup"));
                tracing::info!("initialize backup_storage_directory based on data_directory");
            }
        }
        system_params
    }
}

mod default {
    use super::*;

    pub mod system {
        use super::*;

        macro_rules! system_param_default_fn {
            ($(
                {
                    $field:ident,
                    $type:ty,
                    $default:expr,
                    $is_mutable:expr,
                    $doc:literal,
                    $($rest:tt)*
                },
            )*) => {
                $(
                    paste! {
                        pub fn [<$field _opt>]() -> Option<$type> {
                            $default
                        }
                    }
                )*
            };
        }

        for_all_params!(system_param_default_fn);
    }
}