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

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum SinkDecouple {
    // default sink couple config of specific sink
    #[default]
    Default,
    // enable sink decouple
    Enable,
    // disable sink decouple
    Disable,
}

impl FromStr for SinkDecouple {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "true" | "enable" => Ok(Self::Enable),
            "false" | "disable" => Ok(Self::Disable),
            "default" => Ok(Self::Default),
            _ => Err("expect one of [true, enable, false, disable, default]"),
        }
    }
}

impl ToString for SinkDecouple {
    fn to_string(&self) -> String {
        match self {
            Self::Default => "default".to_string(),
            Self::Enable => "enable".to_string(),
            Self::Disable => "disable".to_string(),
        }
    }
}
