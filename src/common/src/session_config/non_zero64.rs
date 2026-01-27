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

use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;

/// When set this config as `0`, the value is `None`, otherwise the value is
/// `Some(val)`
#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub struct ConfigNonZeroU64(pub Option<NonZeroU64>);

impl FromStr for ConfigNonZeroU64 {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parsed = s.parse::<u64>()?;
        if parsed == 0 {
            Ok(Self(None))
        } else {
            Ok(Self(NonZeroU64::new(parsed)))
        }
    }
}

impl std::fmt::Display for ConfigNonZeroU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let ConfigNonZeroU64(Some(inner)) = self {
            write!(f, "{}", inner)
        } else {
            write!(f, "0")
        }
    }
}

impl ConfigNonZeroU64 {
    pub fn map<U, F>(self, f: F) -> Option<U>
    where
        F: FnOnce(NonZeroU64) -> U,
    {
        self.0.map(f)
    }
}
