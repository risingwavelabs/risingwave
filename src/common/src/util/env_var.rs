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

use std::env;
use std::ffi::OsStr;

/// Checks whether the environment variable `key` is set to `true` or `1` or `t`.
///
/// Returns `false` if the environment variable is not set, or contains invalid characters.
pub fn env_var_is_true(key: impl AsRef<OsStr>) -> bool {
    env_var_is_true_or(key, false)
}

/// Checks whether the environment variable `key` is set to `true` or `1` or `t`.
///
/// Returns `default` if the environment variable is not set, or contains invalid characters.
pub fn env_var_is_true_or(key: impl AsRef<OsStr>, default: bool) -> bool {
    env::var(key)
        .map(|value| {
            ["1", "t", "true"]
                .iter()
                .any(|&s| value.eq_ignore_ascii_case(s))
        })
        .unwrap_or(default)
}
