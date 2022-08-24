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

use super::{ExecuteContext, Task};
use crate::MinioConfig;

pub struct MinioService {
    config: MinioConfig,
}

impl MinioService {
    pub fn new(config: MinioConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn minio_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("minio"))
    }

    fn minio(&self) -> Result<Command> {
        Ok(Command::new(self.minio_path()?))
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &MinioConfig) -> Result<()> {
        cmd.arg("server")
            .arg("--address")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--console-address")
            .arg(format!("{}:{}", config.listen_address, config.console_port))
            .env("MINIO_ROOT_USER", &config.root_user)
            .env("MINIO_ROOT_PASSWORD", &config.root_password)
            .env("MINIO_PROMETHEUS_AUTH_TYPE", "public")
            // Allow MinIO to be used on root disk, bypass restriction.
            // https://github.com/singularity-data/risingwave/pull/3012
            // https://docs.min.io/minio/baremetal/installation/deploy-minio-single-node-single-drive.html#id3
            .env("MINIO_CI_CD", "1");

        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        match provide_prometheus.len() {
            0 => {}
            1 => {
                let prometheus = &provide_prometheus[0];
                cmd.env(
                    "MINIO_PROMETHEUS_URL",
                    format!("http://{}:{}", prometheus.address, prometheus.port),
                );
            }
            other_length => return Err(anyhow!("expected 0 or 1 promethus, get {}", other_length)),
        }

        Ok(())
    }
}

impl Task for MinioService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.minio_path()?;
        if !path.exists() {
            return Err(anyhow!("minio binary not found in {:?}\nDid you enable minio feature in `./risedev configure`?", path));
        }

        let mut cmd = self.minio()?;

        Self::apply_command_args(&mut cmd, &self.config)?;

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let data_path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        std::fs::create_dir_all(&data_path)?;

        cmd.arg("--config-dir")
            .arg(Path::new(&prefix_config).join("minio"))
            .arg(&data_path);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
