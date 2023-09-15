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

use std::path::Path;
use std::process::Command;
use std::{env, fs};

use anyhow::anyhow;

use crate::error::Result;

pub async fn run(profile_path: String, collapsed_path: String) -> Result<()> {
    let executable_path = env::current_exe()?;

    let prof_cmd = move || {
        Command::new("jeprof")
            .arg("--collapsed")
            .arg(executable_path)
            .arg(Path::new(&profile_path))
            .output()
    };
    match tokio::task::spawn_blocking(prof_cmd).await.unwrap() {
        Ok(output) => {
            if output.status.success() {
                fs::write(Path::new(&collapsed_path), &output.stdout)?;
                Ok(())
            } else {
                Err(anyhow!(
                    "jeprof exit with an error. stdout: {}, stderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )
                .into())
            }
        }
        Err(e) => Err(e.into()),
    }
}
