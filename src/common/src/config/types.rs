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

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use clap::ValueEnum;
use educe::Educe;
use foyer::{LfuConfig, LruConfig, S3FifoConfig};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB

/// Constants for storage memory configuration
pub const MAX_META_CACHE_SHARD_BITS: usize = 4;
pub const MIN_BUFFER_SIZE_PER_SHARD: usize = 256;
pub const MAX_BLOCK_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.

/// Unrecognized fields in a config section. Generic over the config section type to provide better
/// error messages.
///
/// The current implementation will log warnings if there are unrecognized fields.
#[derive(Educe)]
#[educe(Clone, Default)]
pub struct Unrecognized<T: 'static> {
    inner: BTreeMap<String, Value>,
    _marker: std::marker::PhantomData<&'static T>,
}

impl<T> std::fmt::Debug for Unrecognized<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Unrecognized<T> {
    /// Returns all unrecognized fields as a map.
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.inner
    }
}

impl<'de, T> Deserialize<'de> for Unrecognized<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = BTreeMap::deserialize(deserializer)?;
        if !inner.is_empty() {
            tracing::warn!(
                "unrecognized fields in `{}`: {:?}",
                std::any::type_name::<T>(),
                inner.keys()
            );
        }
        Ok(Unrecognized {
            inner,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> Serialize for Unrecognized<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    #[default]
    Mem,
    Sql, // any database url
    Sqlite,
    Postgres,
    Mysql,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum DefaultParallelism {
    #[default]
    Full,
    Default(NonZeroUsize),
}

impl Serialize for DefaultParallelism {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        match self {
            DefaultParallelism::Full => Parallelism::Str("Full".to_owned()).serialize(serializer),
            DefaultParallelism::Default(val) => {
                Parallelism::Int(val.get() as _).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for DefaultParallelism {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        let p = Parallelism::deserialize(deserializer)?;
        match p {
            Parallelism::Str(s) => {
                if s.trim().eq_ignore_ascii_case("full") {
                    Ok(DefaultParallelism::Full)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid default parallelism: {}",
                        s
                    )))
                }
            }
            Parallelism::Int(i) => {
                if i == 0 {
                    Err(serde::de::Error::custom(
                        "default parallelism must be positive",
                    ))
                } else {
                    Ok(DefaultParallelism::Default(
                        NonZeroUsize::new(i).unwrap_or(NonZeroUsize::new(1).unwrap()),
                    ))
                }
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetricLevel {
    /// Close all metrics
    #[default]
    Info,
    /// Only opens critical metrics
    Debug,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RpcClientConfig {
    #[serde(default = "default::developer::rpc_client_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AsyncStackTraceOption {
    /// Disabled.
    Off,
    /// Enabled with basic instruments.
    On,
    /// Enabled with extra verbose instruments in release build.
    /// Behaves the same as `on` in debug build due to performance concern.
    #[default]
    #[clap(alias = "verbose")]
    ReleaseVerbose,
}

impl AsyncStackTraceOption {
    pub fn is_verbose(self) -> Option<bool> {
        match self {
            AsyncStackTraceOption::Off => None,
            AsyncStackTraceOption::On => Some(false),
            AsyncStackTraceOption::ReleaseVerbose => Some(true),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum CompactorMode {
    #[default]
    #[clap(alias = "dedicated")]
    Dedicated,

    #[clap(alias = "shared")]
    Shared,

    #[clap(alias = "dedicated_iceberg")]
    DedicatedIceberg,

    #[clap(alias = "shared_iceberg")]
    SharedIceberg,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum EvictionConfig {
    Lru(LruConfig),
    Lfu(LfuConfig),
    #[default]
    S3Fifo(S3FifoConfig),
}

impl EvictionConfig {
    pub fn for_test() -> Self {
        Self::Lru(LruConfig::default())
    }
}

impl From<EvictionConfig> for foyer::EvictionConfig {
    fn from(value: EvictionConfig) -> Self {
        match value {
            EvictionConfig::Lru(config) => foyer::EvictionConfig::Lru(config),
            EvictionConfig::Lfu(config) => foyer::EvictionConfig::Lfu(config),
            EvictionConfig::S3Fifo(config) => foyer::EvictionConfig::S3Fifo(config),
        }
    }
}

mod default {
    pub mod developer {
        pub fn rpc_client_connect_timeout_secs() -> u64 { 5 }
    }
}