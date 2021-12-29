use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};

pub struct ComputeNodeService {
    port: u16,
}

impl ComputeNodeService {
    pub fn new(port: u16) -> Result<Self> {
        Ok(Self { port })
    }

    fn compute_node(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Command::new(Path::new(&prefix_bin).join("compute-node")))
    }
}

impl Task for ComputeNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compute_node()?;

        cmd.arg("--log4rs-config")
            .arg(Path::new(&prefix_config).join("log4rs.yaml"))
            .arg("--host")
            .arg(format!("127.0.0.1:{}", self.port))
            .arg("--state-store")
            .arg(format!(
                "hummock+minio://{}:{}@{}/{}",
                env::var("MINIO_HUMMOCK_USER")?,
                env::var("MINIO_HUMMOCK_PASSWORD")?,
                env::var("HUMOOCK_MINIO_ADDRESS")?,
                env::var("MINIO_BUCKET_NAME")?
            ));

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        format!("compute-node-{}", self.port)
    }
}
