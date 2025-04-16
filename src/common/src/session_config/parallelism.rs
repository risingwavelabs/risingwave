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

use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;

const KEYWORD_DEFAULT: &str = "default";
const KEYWORD_ADAPTIVE: &str = "adaptive";
const KEYWORD_AUTO: &str = "auto";

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum ConfigParallelism {
    #[default]
    Default,
    Fixed(NonZeroU64),
    Adaptive,
}

impl FromStr for ConfigParallelism {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            KEYWORD_DEFAULT => Ok(ConfigParallelism::Default),
            KEYWORD_ADAPTIVE | KEYWORD_AUTO => Ok(ConfigParallelism::Adaptive),
            s => {
                let parsed = s.parse::<u64>()?;
                if parsed == 0 {
                    Ok(ConfigParallelism::Adaptive)
                } else {
                    Ok(ConfigParallelism::Fixed(NonZeroU64::new(parsed).unwrap()))
                }
            }
        }
    }
}

impl std::fmt::Display for ConfigParallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ConfigParallelism::Adaptive => {
                write!(f, "{}", KEYWORD_ADAPTIVE)
            }
            ConfigParallelism::Default => {
                write!(f, "{}", KEYWORD_DEFAULT)
            }
            ConfigParallelism::Fixed(n) => {
                write!(f, "{}", n)
            }
        }
    }
}
