// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::ffi::OsStr;

pub fn env_var_is_true(key: impl AsRef<OsStr>) -> bool {
    env::var(key)
        .map(|value| {
            let value = value.to_lowercase();
            value == "1" || value == "true"
        })
        .unwrap_or(false)
}
