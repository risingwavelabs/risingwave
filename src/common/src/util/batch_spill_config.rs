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

use std::path::Path;
use std::sync::LazyLock;

const RW_BATCH_SPILL_DIR_ENV: &str = "RW_BATCH_SPILL_DIR";
const DEFAULT_SPILL_DIR: &str = "/tmp";

/// Returns the base directory for spill (env value or default), without trailing slash.
pub fn batch_spill_base_dir() -> &'static Path {
    static BASE_DIR: LazyLock<String> = LazyLock::new(|| {
        std::env::var(RW_BATCH_SPILL_DIR_ENV).unwrap_or_else(|_| DEFAULT_SPILL_DIR.to_owned())
    });
    Path::new(&*BASE_DIR)
}
