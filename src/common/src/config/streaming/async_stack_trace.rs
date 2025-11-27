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

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Copy, ValueEnum, Serialize, Deserialize)]
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
            Self::Off => None,
            Self::On => Some(false),
            Self::ReleaseVerbose => Some(!cfg!(debug_assertions)),
        }
    }
}
