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
    pb.set_style(ProgressStyle::default_spinner().template("{spinner} {prefix}: {msg}"));
    pb
}

pub fn complete_spin(pb: &ProgressBar) {
    pb.set_style(ProgressStyle::default_spinner().template("✅ {prefix}: {msg}"));
}
