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
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::MySqlConfig;

pub struct MysqlService {
    config: MySqlConfig,
}

impl MysqlService {
    pub fn new(config: MySqlConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn docker_pull(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("pull").arg("mysql:8");
        cmd
    }

    fn docker_run(&self) -> Command {
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
            // ports
            .arg("-p")
            .arg(format!("{}:{}:3306", self.config.address, self.config.port))
            .arg("mysql:8");
        cmd
    }
}

impl Task for MysqlService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        // let path = self.kafka_path()?;
        // if !path.exists() {
        //     return Err(anyhow!("Kafka binary not found in {:?}\nDid you enable kafka feature in `./risedev configure`?", path));
        // }

        // let prefix_config = env::var("PREFIX_CONFIG")?;

        // let path = if self.config.persist_data {
        //     Path::new(&env::var("PREFIX_DATA")?).join(self.id())
        // } else {
        //     let path = Path::new("/tmp/risedev").join(self.id());
        //     fs_err::remove_dir_all(&path).ok();
        //     path
        // };
        // fs_err::create_dir_all(&path)?;

        // let config_path = Path::new(&prefix_config).join(format!("{}.properties", self.id()));
        // fs_err::write(
        //     &config_path,
        //     KafkaGen.gen_server_properties(&self.config, &path.to_string_lossy()),
        // )?;

        // let mut cmd = self.kafka()?;

        // cmd.arg(config_path);

        ctx.pb.set_message("pulling image...");
        ctx.run_command(self.docker_pull())?;
        ctx.pb.set_message("starting...");
        ctx.run_command(ctx.tmux_run(self.docker_run())?)?;

        // if !self.config.user_managed {
        //     ctx.run_command(ctx.tmux_run(cmd)?)?;
        // } else {
        //     ctx.pb.set_message("user managed");
        //     writeln!(
        //         &mut ctx.log,
        //         "Please start your MySQL at {}:{}\n\n",
        //         self.config.address, self.config.port
        //     )?;
        // }

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
