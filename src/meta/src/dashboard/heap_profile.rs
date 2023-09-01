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

use std::{process::Command, path::Path};

use anyhow::anyhow;

use super::handlers::{err, DashboardError};

pub async fn run_jeprof(profile_path: String, collapsed_path: String, binary_path: String) -> Result<(), DashboardError> {
    let prof_cmd = move || {
        let mut cmd = Command::new("jeprof");
         let cmd =  cmd.arg("--collapsed")
            .arg(Path::new(&binary_path))
            .arg(Path::new(&profile_path));
            // .arg(">")
            // .arg(Path::new(&collapsed_path));

        dbg!(&cmd);
             cmd.output()
        
    };
    match prof_cmd() {
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
