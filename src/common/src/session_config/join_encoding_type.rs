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

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum JoinEncodingType {
    #[default]
    Memory = 1,
    Cpu = 2,
}

impl FromStr for JoinEncodingType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("memory_optimized") {
            Ok(Self::Memory)
        } else if s.eq_ignore_ascii_case("cpu_optimized") {
            Ok(Self::Cpu)
        } else {
            Err("expect one of [memory_optimized, cpu_optimized]")
        }
    }
}

impl std::fmt::Display for JoinEncodingType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Memory => write!(f, "memory_optimized"),
            Self::Cpu => write!(f, "cpu_optimized"),
        }
    }
}
