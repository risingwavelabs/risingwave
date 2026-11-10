// Copyright 2026 RisingWave Labs
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

use risingwave_common_proc_macro::ConfigDoc;
use risingwave_pb::plan_common::AiDecideConfig as PbAiDecideConfig;
use serde::{Deserialize, Deserializer, Serialize};

use super::DefaultFromSerde;

/// HTTP execution settings shared by streaming jobs and batch expression contexts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
#[serde(deny_unknown_fields)]
pub struct AiDecideConfig {
    /// Timeout of each HTTP attempt in milliseconds. Must be positive.
    #[serde(
        default = "default_request_timeout_ms",
        deserialize_with = "positive_u32"
    )]
    pub request_timeout_ms: u32,
    /// Maximum HTTP attempts per logical request, including the first. Must be positive.
    #[serde(default = "default_max_attempts", deserialize_with = "positive_u32")]
    pub max_attempts: u32,
    /// Initial exponential retry delay in milliseconds. Zero disables the delay.
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u32,
    /// Maximum server Retry-After delay in milliseconds. Zero disables that delay.
    #[serde(default = "default_max_retry_after_ms")]
    pub max_retry_after_ms: u32,
    /// Maximum response body size in bytes before JSON decoding. Must be positive.
    #[serde(
        default = "default_max_response_bytes",
        deserialize_with = "positive_u32"
    )]
    pub max_response_bytes: u32,
    /// Maximum logical requests per expression instance, shared across chunks and held through retries. Zero means unlimited; actors do not share this limit.
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
}

fn default_request_timeout_ms() -> u32 {
    30_000
}
fn default_max_attempts() -> u32 {
    3
}
fn default_retry_delay_ms() -> u32 {
    100
}
fn default_max_retry_after_ms() -> u32 {
    5_000
}
fn default_max_response_bytes() -> u32 {
    16 * 1024 * 1024
}
fn default_max_concurrency() -> u32 {
    0
}

fn positive_u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = u32::deserialize(deserializer)?;
    if value == 0 {
        return Err(serde::de::Error::custom("must be greater than zero"));
    }
    Ok(value)
}

impl AiDecideConfig {
    pub const SESSION_OPTIONS: [&'static str; 6] = [
        "ai_decide_request_timeout_ms",
        "ai_decide_max_attempts",
        "ai_decide_retry_delay_ms",
        "ai_decide_max_retry_after_ms",
        "ai_decide_max_response_bytes",
        "ai_decide_max_concurrency",
    ];

    /// Also validate values received through protobuf or constructed directly in Rust.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.request_timeout_ms == 0 {
            return Err("ai_decide_request_timeout_ms must be positive");
        }
        if self.max_attempts == 0 {
            return Err("ai_decide_max_attempts must be positive");
        }
        if self.max_response_bytes == 0 {
            return Err("ai_decide_max_response_bytes must be positive");
        }
        Ok(())
    }
}

impl From<&AiDecideConfig> for PbAiDecideConfig {
    fn from(value: &AiDecideConfig) -> Self {
        Self {
            request_timeout_ms: value.request_timeout_ms,
            max_attempts: value.max_attempts,
            retry_delay_ms: value.retry_delay_ms,
            max_retry_after_ms: value.max_retry_after_ms,
            max_response_bytes: value.max_response_bytes,
            max_concurrency: value.max_concurrency,
        }
    }
}

impl From<PbAiDecideConfig> for AiDecideConfig {
    fn from(value: PbAiDecideConfig) -> Self {
        Self {
            request_timeout_ms: value.request_timeout_ms,
            max_attempts: value.max_attempts,
            retry_delay_ms: value.retry_delay_ms,
            max_retry_after_ms: value.max_retry_after_ms,
            max_response_bytes: value.max_response_bytes,
            max_concurrency: value.max_concurrency,
        }
    }
}
