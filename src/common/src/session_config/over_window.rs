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

use std::str::FromStr;

use enum_as_inner::EnumAsInner;
use parse_display::{Display, FromStr};
use risingwave_pb::stream_plan::PbOverWindowCachePolicy;

use super::{ConfigEntry, CONFIG_KEYS, STREAMING_OVER_WINDOW_CACHE_POLICY};
use crate::error::ErrorCode::{self, InvalidConfigValue};
use crate::error::RwError;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq, FromStr, Display, EnumAsInner)]
#[display(style = "snake_case")]
pub enum OverWindowCachePolicy {
    /// Cache all entries.
    #[default]
    Full,
    /// Cache only recently accessed range of entries.
    Recent,
    /// Cache only the first N entries in recently accessed range.
    RecentFirstN,
    /// Cache only the last N entries in recently accessed range.
    RecentLastN,
}

impl TryFrom<&[&str]> for OverWindowCachePolicy {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                Self::entry_name()
            ))
            .into());
        }

        let s = value[0].to_ascii_lowercase().replace('-', "_");
        OverWindowCachePolicy::from_str(&s).map_err(|_| {
            InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            }
            .into()
        })
    }
}

impl ConfigEntry for OverWindowCachePolicy {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[STREAMING_OVER_WINDOW_CACHE_POLICY]
    }
}

impl OverWindowCachePolicy {
    pub fn to_protobuf(self) -> PbOverWindowCachePolicy {
        match self {
            Self::Full => PbOverWindowCachePolicy::Full,
            Self::Recent => PbOverWindowCachePolicy::Recent,
            Self::RecentFirstN => PbOverWindowCachePolicy::RecentFirstN,
            Self::RecentLastN => PbOverWindowCachePolicy::RecentLastN,
        }
    }

    pub fn from_protobuf(pb: PbOverWindowCachePolicy) -> Self {
        match pb {
            PbOverWindowCachePolicy::Unspecified => Self::default(),
            PbOverWindowCachePolicy::Full => Self::Full,
            PbOverWindowCachePolicy::Recent => Self::Recent,
            PbOverWindowCachePolicy::RecentFirstN => Self::RecentFirstN,
            PbOverWindowCachePolicy::RecentLastN => Self::RecentLastN,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_over_window_cache_policy() {
        assert_eq!(
            OverWindowCachePolicy::try_from(["full"].as_slice()).unwrap(),
            OverWindowCachePolicy::Full
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["recent"].as_slice()).unwrap(),
            OverWindowCachePolicy::Recent
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["RECENT"].as_slice()).unwrap(),
            OverWindowCachePolicy::Recent
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["recent_first_n"].as_slice()).unwrap(),
            OverWindowCachePolicy::RecentFirstN
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["recent_last_n"].as_slice()).unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["recent-last-n"].as_slice()).unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert_eq!(
            OverWindowCachePolicy::try_from(["recent_last_N"].as_slice()).unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert!(OverWindowCachePolicy::try_from(["foo"].as_slice()).is_err());
    }
}
