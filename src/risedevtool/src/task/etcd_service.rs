// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &EtcdConfig) -> Result<()> {
        let listen_urls = format!("http://{}:{}", config.listen_address, config.port);
        let advertise_urls = format!("http://{}:{}", config.address, config.port);
        let peer_urls = format!("http://{}:{}", config.listen_address, config.peer_port);
        let advertise_peer_urls = format!("http://{}:{}", config.address, config.peer_port);
        let exporter_urls = format!("http://{}:{}", config.listen_address, config.exporter_port);

        cmd.arg("--listen-client-urls")
            .arg(&listen_urls)
            .arg("--advertise-client-urls")
            .arg(&advertise_urls)
            .arg("--listen-peer-urls")
            .arg(&peer_urls)
            .arg("--initial-advertise-peer-urls")
            .arg(&advertise_peer_urls)
            .arg("--listen-metrics-urls")
            .arg(&exporter_urls)
            .arg("--name")
            .arg("risedev-meta")
            .arg("--max-txn-ops")
            .arg("999999")
            .arg("--auto-compaction-mode")
            .arg("periodic")
            .arg("--auto-compaction-retention")
            .arg("1m")
            .arg("--snapshot-count")
            .arg("10000");

        if config.unsafe_no_fsync {
            cmd.arg("--unsafe-no-fsync");
        }

        Ok(())
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
        Self::apply_command_args(&mut cmd, &self.config)?;

        let path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        std::fs::create_dir_all(&path)?;
        cmd.arg("--data-dir").arg(&path);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
