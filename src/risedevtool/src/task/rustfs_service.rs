// Copyright 2025 RisingWave Labs
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

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, anyhow};

use super::{ExecuteContext, Task};
use crate::RustfsConfig;
use crate::util::stylized_risedev_subcmd;

pub struct RustfsService {
    config: RustfsConfig,
}

impl RustfsService {
    pub fn new(config: RustfsConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn rustfs_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("rustfs"))
    }

    fn rustfs(&self) -> Result<Command> {
        Ok(Command::new(self.rustfs_path()?))
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &RustfsConfig) -> Result<()> {
        cmd.arg("server")
            .arg("--address")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .env("RUSTFS_ROOT_USER", &config.root_user)
            .env("RUSTFS_ROOT_PASSWORD", &config.root_password)
            .env("RUSTFS_BUCKET", &config.hummock_bucket);

        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        match provide_prometheus.len() {
            0 => {}
            1 => {
                let prometheus = &provide_prometheus[0];
                cmd.env(
                    "RUSTFS_PROMETHEUS_URL",
                    format!("http://{}:{}", prometheus.address, prometheus.port),
                );
            }
            other_length => {
                return Err(anyhow!("expected 0 or 1 prometheus, get {}", other_length));
            }
        }

        Ok(())
    }
}

impl Task for RustfsService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.rustfs_path()?;
        if !path.exists() {
            return Err(anyhow!(
                "rustfs binary not found in {:?}\nDid you enable rustfs feature in `{}`?",
                path,
                stylized_risedev_subcmd("configure")
            ));
        }

        let mut cmd = self.rustfs()?;

        Self::apply_command_args(&mut cmd, &self.config)?;

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let data_path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        fs_err::create_dir_all(&data_path)?;

        cmd.arg("--config-dir")
            .arg(Path::new(&prefix_config).join("rustfs"))
            .arg(&data_path);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
