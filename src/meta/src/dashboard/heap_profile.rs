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

use std::process::Command;

use anyhow::anyhow;

use super::handlers::{err, DashboardError};

pub async fn run_jeprof(profile_path: String, binary_path: String) -> Result<(), DashboardError> {
    let collapsed_path = format!("{}.collapsed", profile_path);
    let prof_cmd = move || {
        Command::new("jeprof")
            .arg("--collapsed")
            .arg(binary_path)
            .arg(profile_path)
            .arg(">")
            .arg(collapsed_path)
            .output()
    };
    match tokio::task::spawn_blocking(prof_cmd).await.unwrap() {
        Ok(output) => {
            if output.status.success() {
                Ok(())
            } else {
                Err(err(anyhow!(
                    "jeprof exit with an error. stdout: {}, stderr: {}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                )))
            }
        }
        Err(e) => Err(err(e)),
    }
}
