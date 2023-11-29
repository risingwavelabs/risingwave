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
use std::result::Result;
use std::{env, fs};

/// Error type for running `jeprof`.
#[derive(thiserror::Error, Debug, thiserror_ext::ContextInto)]
pub enum JeprofError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("jeprof exit with an error (stdout: {stdout}, stderr: {stderr}): {inner}")]
    ExitError {
        #[source]
        inner: std::process::ExitStatusError,
        stdout: String,
        stderr: String,
    },
}

/// Run `jeprof --collapsed` on the given profile.
pub async fn run(profile_path: String, collapsed_path: String) -> Result<(), JeprofError> {
    let executable_path = env::current_exe()?;

    let prof_cmd = move || {
        Command::new("jeprof")
            .arg("--collapsed")
            .arg(executable_path)
            .arg(Path::new(&profile_path))
            .output()
    };

    let output = tokio::task::spawn_blocking(prof_cmd).await.unwrap()?;

    output.status.exit_ok().into_exit_error(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )?;

    fs::write(Path::new(&collapsed_path), &output.stdout)?;

    Ok(())
}
