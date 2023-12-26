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
use parse_display::Display;
use risingwave_pb::stream_plan::PbOverWindowCachePolicy;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq, Display, EnumAsInner)]
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

impl FromStr for OverWindowCachePolicy {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase().replace('-', "_");
        match s.as_str() {
            "full" => Ok(Self::Full),
            "recent" => Ok(Self::Recent),
            "recent_first_n" => Ok(Self::RecentFirstN),
            "recent_last_n" => Ok(Self::RecentLastN),
            _ => Err("expect one of [full, recent, recent_first_n, recent_last_n]"),
        }
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
            OverWindowCachePolicy::from_str("full").unwrap(),
            OverWindowCachePolicy::Full
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("recent").unwrap(),
            OverWindowCachePolicy::Recent
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("RECENT").unwrap(),
            OverWindowCachePolicy::Recent
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("recent_first_n").unwrap(),
            OverWindowCachePolicy::RecentFirstN
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("recent_last_n").unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("recent-last-n").unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert_eq!(
            OverWindowCachePolicy::from_str("recent_last_N").unwrap(),
            OverWindowCachePolicy::RecentLastN
        );
        assert!(OverWindowCachePolicy::from_str("foo").is_err());
    }
}
