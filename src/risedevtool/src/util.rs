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

use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;

pub fn get_program_name(cmd: &Command) -> String {
    let program_path = cmd.get_program().to_string_lossy();
    match program_path.rsplit_once('/') {
        Some((_, rest)) => rest.to_string(),
        None => program_path.to_string(),
    }
}

pub fn get_program_args(cmd: &Command) -> String {
    cmd.get_args().map(|x| x.to_string_lossy()).join(" \\\n  ")
}

pub fn get_program_env_cmd(cmd: &Command) -> String {
    cmd.get_envs()
        .map(|(k, v)| {
            format!(
                "export {}={}",
                k.to_string_lossy(),
                v.map(|v| v.to_string_lossy()).unwrap_or_default()
            )
        })
        .join("\n")
}

pub fn new_spinner() -> ProgressBar {
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {prefix}: {msg}")
            .unwrap(),
    );
    pb
}

pub fn complete_spin(pb: &ProgressBar) {
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("✅ {prefix}: {msg}")
            .unwrap(),
    );
}

pub fn fail_spin(pb: &ProgressBar) {
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("❗ {prefix}: {msg}")
            .unwrap(),
    );
}

pub fn is_env_set(var: &str) -> bool {
    if let Ok(val) = std::env::var(var) {
        if let Ok(true) = val.parse() {
            return true;
        } else if let Ok(x) = val.parse::<usize>() {
            if x != 0 {
                return true;
            }
        }
    }
    false
}
