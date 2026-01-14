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

use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use parse_display::Display;
use risingwave_pb::meta::PbCacheRefillPolicy;
use serde_with::{DeserializeFromStr, SerializeDisplay};

#[derive(
    Copy,
    Default,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Display,
    EnumAsInner,
    SerializeDisplay,
    DeserializeFromStr,
)]
#[display(style = "snake_case")]
pub enum CacheRefillPolicy {
    /// Use global default cache refilll policy.
    #[default]
    Default,
    /// Enable cache refill for streaming optimization.
    Streaming,
    /// Enable cache refill for serving optimization.
    Serving,
    /// Enable cache refill for both streaming and serving optimizations.
    Both,
}

impl FromStr for CacheRefillPolicy {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase().replace('-', "_");
        match s.as_str() {
            "default" => Ok(Self::Default),
            "streaming" => Ok(Self::Streaming),
            "serving" => Ok(Self::Serving),
            "both" => Ok(Self::Both),
            _ => Err("expect one of [default, streaming, serving, both]"),
        }
    }
}

impl CacheRefillPolicy {
    pub fn to_protobuf(self) -> PbCacheRefillPolicy {
        match self {
            Self::Default => PbCacheRefillPolicy::Default,
            Self::Streaming => PbCacheRefillPolicy::Streaming,
            Self::Serving => PbCacheRefillPolicy::Serving,
            Self::Both => PbCacheRefillPolicy::Both,
        }
    }

    pub fn from_protobuf(pb: PbCacheRefillPolicy) -> Self {
        match pb {
            PbCacheRefillPolicy::Default => Self::Default,
            PbCacheRefillPolicy::Streaming => Self::Streaming,
            PbCacheRefillPolicy::Serving => Self::Serving,
            PbCacheRefillPolicy::Both => Self::Both,
        }
    }
}
