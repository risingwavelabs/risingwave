use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use crate::{EtcdConfig, Task};

pub struct EtcdService {
    config: EtcdConfig,
}

impl EtcdService {
    pub fn new(config: EtcdConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn path() -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("etcd").join("etcd"))
    }

    fn etcd() -> Result<Command> {
        Ok(Command::new(Self::path()?))
    }
}

impl Task for EtcdService {
    fn execute(
        &mut self,
        ctx: &mut crate::ExecuteContext<impl std::io::Write>,
    ) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = Self::path()?;
        if !path.exists() {
            return Err(anyhow!("etcd binary not found in {:?}\nDid you enable etcd feature in `./risedev configure`?", path));
        }

        let mut cmd = Self::etcd()?;
        let listen_urls = format!("http://{}:{}", self.config.address, self.config.port);
        let peer_urls = format!("http://{}:{}", self.config.address, self.config.peer_port);

        cmd.arg("--data-dir")
            .arg(env::var("META_STORE_PATH")?)
            .arg("--listen-client-urls")
            .arg(&listen_urls)
            .arg("--advertise-client-urls")
            .arg(&listen_urls)
            .arg("--listen-peer-urls")
            .arg(&peer_urls)
            .arg("--name")
            .arg("risedev-meta");

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
