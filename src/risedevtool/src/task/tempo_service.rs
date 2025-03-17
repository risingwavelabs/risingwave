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
use crate::util::stylized_risedev_subcmd;
use crate::{TempoConfig, TempoGen};

pub struct TempoService {
    config: TempoConfig,
}

impl TempoService {
    pub fn new(config: TempoConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn tempo_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("tempo").join("tempo"))
    }

    fn tempo(&self) -> Result<Command> {
        Ok(Command::new(self.tempo_path()?))
    }

    pub fn apply_command_args(
        cmd: &mut Command,
        data_path: impl AsRef<Path>,
        config_file_path: impl AsRef<Path>,
    ) -> Result<()> {
        cmd.arg("--storage.trace.backend")
            .arg("local")
            .arg("--storage.trace.local.path")
            .arg(data_path.as_ref().join("blocks"))
            .arg("--storage.trace.wal.path")
            .arg(data_path.as_ref().join("wal"))
            .arg("--config.file")
            .arg(config_file_path.as_ref());
        Ok(())
    }
}

impl Task for TempoService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.tempo_path()?;
        if !path.exists() {
            return Err(anyhow!(
                "tempo binary not found in {:?}\nDid you enable tracing feature in `{}`?",
                path,
                stylized_risedev_subcmd("configure")
            ));
        }

        let prefix_config = env::var("PREFIX_CONFIG")?;
        let prefix_data = env::var("PREFIX_DATA")?;

        let config_file_path = Path::new(&prefix_config).join("tempo.yml");
        fs_err::write(&config_file_path, TempoGen.gen_tempo_yml(&self.config))?;

        let data_path = Path::new(&prefix_data).join(self.id());
        fs_err::create_dir_all(&data_path)?;

        let mut cmd = self.tempo()?;
        Self::apply_command_args(&mut cmd, &data_path, &config_file_path)?;

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
