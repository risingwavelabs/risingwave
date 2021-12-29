use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};

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
}

impl Task for MinioService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.minio()?;

        cmd.arg("server")
            .arg(env::var("HUMMOCK_PATH")?)
            .arg("--address")
            .arg(env::var("HUMOOCK_MINIO_ADDRESS")?)
            .arg("--console-address")
            .arg(env::var("HUMOOCK_MINIO_CONSOLE_ADDRESS")?);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        "minio".to_string()
    }
}
