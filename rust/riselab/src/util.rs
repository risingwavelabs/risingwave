use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use os_pipe::PipeReader;

use crate::RISELAB_SESSION_NAME;

pub fn get_program_name(cmd: &Command) -> String {
    let program_path = cmd.get_program().to_string_lossy();
    match program_path.rsplit_once('/') {
        Some((_, rest)) => rest.to_string(),
        None => program_path.to_string(),
    }
}

pub fn get_program_args(cmd: &Command) -> String {
    cmd.get_args().map(|x| x.to_string_lossy()).join(" ")
}

pub fn new_spinner() -> ProgressBar {
    let pb = ProgressBar::new(0);
    pb.set_style(ProgressStyle::default_spinner().template("{spinner} {prefix}: {msg}"));
    pb
}

pub fn pb_success(pb: &ProgressBar) {
    pb.set_style(ProgressStyle::default_spinner().template("✅ {prefix}: {msg}"));
}

pub fn pipe_output(cmd: &mut Command) -> Result<PipeReader> {
    let (reader, writer1) = os_pipe::pipe()?;
    let writer2 = writer1.try_clone()?;
    cmd.stdout(writer1).stderr(writer2);
    Ok(reader)
}

pub fn run_command(mut cmd: Command, f: &mut impl std::io::Write) -> Result<()> {
    let program_name = get_program_name(&cmd);

    writeln!(f, "> {} {}", program_name, get_program_args(&cmd))?;

    let output = cmd.output()?;

    let mut full_output = String::from_utf8_lossy(&output.stdout).to_string();
    full_output.extend(String::from_utf8_lossy(&output.stderr).chars());

    write!(f, "{}", full_output)?;

    writeln!(
        f,
        "({} exited with {:?})",
        program_name,
        output.status.code()
    )?;

    writeln!(f, "---")?;

    output.status.exit_ok()?;

    Ok(())
}

pub fn tmux_run(user_cmd: Command) -> anyhow::Result<Command> {
    let prefix_path = env::var("PREFIX_BIN")?;
    let mut cmd = Command::new("tmux");
    cmd.arg("new-window").arg("-t").arg(RISELAB_SESSION_NAME);
    cmd.arg(Path::new(&prefix_path).join("run_command.sh"));
    cmd.arg(user_cmd.get_program());
    for arg in user_cmd.get_args() {
        cmd.arg(arg);
    }
    Ok(cmd)
}
