use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::{run_command, tmux_run};

#[derive(Default)]
pub struct MinioService;

impl MinioService {
    pub fn new() -> Result<Self> {
        Ok(Self::default())
    }

    fn minio(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(Path::new(&prefix_bin).join("minio")))
    }

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        let mut cmd = self.minio()?;

        pb.enable_steady_tick(100);
        pb.set_message("starting...");

        cmd.arg("server")
            .arg(env::var("HUMMOCK_PATH")?)
            .arg("--address")
            .arg(env::var("HUMOOCK_MINIO_ADDRESS")?)
            .arg("--console-address")
            .arg(env::var("HUMOOCK_MINIO_CONSOLE_ADDRESS")?);

        run_command(tmux_run(cmd)?, f)?;

        pb.set_message("started");

        Ok(())
    }

    pub fn id(&self) -> String {
        "minio".to_string()
    }
}
