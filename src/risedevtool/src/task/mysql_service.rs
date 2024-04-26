// Copyright 2024 RisingWave Labs
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
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use super::{ExecuteContext, Task};
use crate::{DummyService, MySqlConfig};

// TODO: extract to a common docker-backed service
pub struct MysqlService {
    config: MySqlConfig,
}

impl MysqlService {
    pub fn new(config: MySqlConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn docker_pull(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("pull").arg(&self.config.image);
        cmd
    }

    fn docker_run_args(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("--rm")
            .arg("--name")
            .arg(format!("risedev-{}", self.config.id))
            .arg("-e")
            .arg("MYSQL_ALLOW_EMPTY_PASSWORD=1")
            .arg("-e")
            .arg(format!("MYSQL_USER={}", self.config.user))
            .arg("-e")
            .arg(format!("MYSQL_PASSWORD={}", self.config.password))
            .arg("-e")
            .arg(format!("MYSQL_DATABASE={}", self.config.database))
            .arg("-p")
            .arg(format!("{}:{}:3306", self.config.address, self.config.port));
        cmd
    }
}

impl Task for MysqlService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        if self.config.user_managed {
            return DummyService::new(&self.config.id).execute(ctx);
        }

        ctx.service(self);

        ctx.pb.set_message("pulling image...");
        ctx.run_command(self.docker_pull())?;

        ctx.pb.set_message("starting...");

        let mut run_cmd = self.docker_run_args();
        if self.config.persist_data {
            let path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
            fs_err::create_dir_all(&path)?;
            run_cmd
                .arg("-v")
                .arg(format!("{}:/var/lib/mysql", path.to_string_lossy()));
        }
        run_cmd.arg(&self.config.image);

        ctx.run_command(ctx.tmux_run(run_cmd)?)?;

        ctx.pb.set_message("started");
        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
