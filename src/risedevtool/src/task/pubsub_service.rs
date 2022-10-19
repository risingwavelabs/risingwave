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
use crate::PubsubConfig;

pub struct PubsubService {
    config: PubsubConfig,
}

impl PubsubService {
    pub fn new(config: PubsubConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn gcloud_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin)
            .join("gcloud")
            .join("bin")
            .join("gcloud"))
    }

    fn gcloud(&self) -> Result<Command> {
        Ok(Command::new(self.gcloud_path()?))
    }
}

impl Task for PubsubService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.gcloud_path()?;
        if !path.exists() {
            return Err(anyhow!("gcloud binary not found in {:?}\nDid you enable pubsub-emulator feature in `./risedev configure`?", path));
        }

        let path = if self.config.persist_data {
            Path::new(&env::var("PREFIX_DATA")?).join(self.id())
        } else {
            let path = Path::new("/tmp/risedev").join(self.id());
            std::fs::remove_dir_all(&path).ok();
            path
        };
        std::fs::create_dir_all(&path)?;

        let mut cmd = self.gcloud()?;

        // cmd.arg(config_path);
        cmd.args([
            "beta",
            "emulators",
            "pubsub",
            "start",
            format!("--host-port={}:{}", self.config.address, self.config.port).as_str(),
            format!("--data-dir={}", path.to_string_lossy()).as_str(),
        ]);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
