use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::Result;
use indicatif::ProgressBar;

use crate::util::{run_command, tmux_run};

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

    pub fn execute(&mut self, f: &mut impl std::io::Write, pb: ProgressBar) -> Result<()> {
        pb.enable_steady_tick(100);
        pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compute_node()?;

        cmd.arg("--log4rs-config")
            .arg(Path::new(&prefix_config).join("log4rs.yaml"))
            .arg("--host")
            .arg(format!("127.0.0.1:{}", self.port))
            .arg("--state-store")
            .arg(format!(
                "hummock+minio://{}:{}@{}/{}",
                env::var("MINIO_ROOT_USER")?,
                env::var("MINIO_ROOT_PASSWORD")?,
                env::var("HUMOOCK_MINIO_ADDRESS")?,
                env::var("MINIO_BUCKET_NAME")?
            ));

        run_command(tmux_run(cmd)?, f)?;

        pb.set_message("started");

        Ok(())
    }

    pub fn id(&self) -> String {
        format!("compute-node({})", self.port)
    }
}
