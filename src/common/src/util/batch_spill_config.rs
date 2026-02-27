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

use std::path::{Path, PathBuf};
use std::sync::LazyLock;

const RW_BATCH_SPILL_DIR_ENV: &str = "RW_BATCH_SPILL_DIR";
const DEFAULT_SPILL_DIR: &str = "/tmp";

/// Returns the base directory for spill (env value or default)
pub fn batch_spill_base_dir() -> &'static Path {
    static BASE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
        let raw =
            std::env::var(RW_BATCH_SPILL_DIR_ENV).unwrap_or_else(|_| DEFAULT_SPILL_DIR.to_owned());
        let mut dir = PathBuf::from(raw.trim());
        if dir.is_relative() {
            dir = std::env::current_dir().unwrap().join(dir);
        }
        dir
    });
    BASE_DIR.as_path()
}
