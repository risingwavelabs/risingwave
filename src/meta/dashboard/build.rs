// Copyright 2024 RisingWave Labs
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

use cargo_emit::{rerun_if_changed, rustc_cfg};
use npm_rs::NpmEnv;

fn env_var_is_true(key: &str) -> bool {
    cargo_emit::rerun_if_env_changed!(key);

    std::env::var(key)
        .map(|value| {
            ["1", "t", "true"]
                .iter()
                .any(|&s| value.eq_ignore_ascii_case(s))
        })
        .unwrap_or(false)
}

const DASHBOARD_DIR: &str = "../../../dashboard";

fn dest_dir() -> PathBuf {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    Path::new(&out_dir).join("assets")
}

fn build() -> anyhow::Result<()> {
    // TODO(bugen): we should include all files and subdirectories under `DASHBOARD_DIR`
    // while excluding the `out` directory. There's no elegant way to do this.
    rerun_if_changed!(format!("{DASHBOARD_DIR}/components"));

    let exit_status = NpmEnv::default()
        .set_path(DASHBOARD_DIR)
        .init_env()
        .install(None)
        .run("build")
        .exec()?;

    if !exit_status.success() {
        anyhow::bail!("dashboard build failed with status: {}", exit_status);
    }

    let dest = dest_dir();
    let src = Path::new(DASHBOARD_DIR).join("out");
    dircpy::copy_dir(src, dest)?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let should_build = env_var_is_true("ENABLE_BUILD_DASHBOARD");

    if should_build {
        build()?;
        // Once build succeeded, set a cfg flag to indicate that the embedded assets
        // are ready to be used.
        rustc_cfg!("dashboard_built");
    } else {
        // If we're not to build, create the destination directory but keep it empty.
        std::fs::create_dir_all(dest_dir())?;
    }

    Ok(())
}
