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

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::server::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    /// The default number of the connections when connecting to a gRPC server.
    ///
    /// For the connections used in streaming or batch exchange, please refer to the entries in
    /// `[stream.developer]` and `[batch.developer]` sections. This value will be used if they
    /// are not specified.
    #[serde(default = "default::server::connection_pool_size")]
    // Intentionally made private to avoid abuse. Check the related methods on `RwConfig`.
    pub(super) connection_pool_size: u16,

    /// Used for control the metrics level, similar to log level.
    #[serde(default = "default::server::metrics_level")]
    pub metrics_level: MetricLevel,

    #[serde(default = "default::server::telemetry_enabled")]
    pub telemetry_enabled: bool,

    /// Enable heap profile dump when memory usage is high.
    #[serde(default)]
    pub heap_profiling: HeapProfilingConfig,

    // Number of max pending reset stream for grpc server.
    #[serde(default = "default::server::grpc_max_reset_stream_size")]
    pub grpc_max_reset_stream: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct HeapProfilingConfig {
    /// Enable to auto dump heap profile when memory usage is high
    #[serde(default = "default::heap_profiling::enable_auto")]
    pub enable_auto: bool,

    /// The proportion (number between 0 and 1) of memory usage to trigger heap profile dump
    #[serde(default = "default::heap_profiling::threshold_auto")]
    pub threshold_auto: f32,

    /// The directory to dump heap profile. If empty, the prefix in `MALLOC_CONF` will be used
    #[serde(default = "default::heap_profiling::dir")]
    pub dir: String,
}

pub mod default {

    pub mod server {
        use crate::config::MetricLevel;

        pub fn heartbeat_interval_ms() -> u32 {
            1000
        }

        pub fn connection_pool_size() -> u16 {
            16
        }

        pub fn metrics_level() -> MetricLevel {
            MetricLevel::Info
        }

        pub fn telemetry_enabled() -> bool {
            true
        }

        pub fn grpc_max_reset_stream_size() -> u32 {
            200
        }
    }

    pub mod heap_profiling {
        pub fn enable_auto() -> bool {
            true
        }

        pub fn threshold_auto() -> f32 {
            0.9
        }

        pub fn dir() -> String {
            "./".to_owned()
        }
    }
}
