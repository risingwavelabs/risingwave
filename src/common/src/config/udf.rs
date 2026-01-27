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

use super::*;

/// The subsections `[udf]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct UdfConfig {
    /// Allow embedded Python UDFs to be created.
    #[serde(default = "default::udf::enable_embedded_python_udf")]
    pub enable_embedded_python_udf: bool,

    /// Allow embedded JS UDFs to be created.
    #[serde(default = "default::udf::enable_embedded_javascript_udf")]
    pub enable_embedded_javascript_udf: bool,

    /// Allow embedded WASM UDFs to be created.
    #[serde(default = "default::udf::enable_embedded_wasm_udf")]
    pub enable_embedded_wasm_udf: bool,
}

pub mod default {

    pub mod udf {
        pub fn enable_embedded_python_udf() -> bool {
            false
        }

        pub fn enable_embedded_javascript_udf() -> bool {
            true
        }

        pub fn enable_embedded_wasm_udf() -> bool {
            true
        }
    }
}
