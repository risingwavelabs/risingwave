// Copyright 2023 RisingWave Labs
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
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use crate::{ExecuteContext, RedisConfig, Task};

pub struct RedisService {
    pub config: RedisConfig,
}

impl RedisService {
    pub fn new(config: RedisConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn redis_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin)
            .join("redis")
            .join("src")
            .join("redis-server"))
    }

    fn redis(&self) -> Result<Command> {
        Ok(Command::new(self.redis_path()?))
    }
}

impl Task for RedisService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting");
        let path = self.redis_path()?;
        if !path.exists() {
            return Err(anyhow!("Redis binary not found in {:?}\nDid you enable redis feature in `./risedev configure`?", path));
        }

        let mut cmd = self.redis()?;
        cmd.arg("--bind")
            .arg(&self.config.address)
            .arg("--port")
            .arg(self.config.port.to_string())
            .arg("--shutdown-on-sigint")
            .arg("nosave");

        ctx.run_command(ctx.tmux_run(cmd)?)?;
        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
