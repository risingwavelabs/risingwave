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
use crate::{KafkaConfig, KafkaGen};

pub struct KafkaService {
    config: KafkaConfig,
}

impl KafkaService {
    pub fn new(config: KafkaConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn kafka_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin)
            .join("kafka")
            .join("bin")
            .join("kafka-server-start.sh"))
    }

    fn kafka(&self) -> Result<Command> {
        Ok(Command::new(self.kafka_path()?))
    }

    /// Format kraft storage. This is a necessary step to start a fresh Kafka service.
    fn kafka_storage_format(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        let path = Path::new(&prefix_bin)
            .join("kafka")
            .join("bin")
            .join("kafka-storage.sh");

        let mut cmd = Command::new(path);
        cmd.arg("format").arg("-t").arg("risedev-kafka").arg("-c"); // the remaining arg is the path to the config file
        Ok(cmd)
    }
}

impl Task for KafkaService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        if self.config.user_managed {
            ctx.pb.set_message("user managed");
            writeln!(
                &mut ctx.log,
                "Please start your Kafka at {}:{}\n\n",
                self.config.address, self.config.port
            )?;
            return Ok(());
        }

        ctx.pb.set_message("starting...");

        let path = self.kafka_path()?;
        if !path.exists() {
            return Err(anyhow!("Kafka binary not found in {:?}\nDid you enable kafka feature in `./risedev configure`?", path));
        }

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let path = if self.config.persist_data {
            Path::new(&env::var("PREFIX_DATA")?).join(self.id())
        } else {
            let path = Path::new("/tmp/risedev").join(self.id());
            fs_err::remove_dir_all(&path).ok();
            path
        };
        fs_err::create_dir_all(&path)?;

        let config_path = Path::new(&prefix_config).join(format!("{}.properties", self.id()));
        fs_err::write(
            &config_path,
            KafkaGen.gen_server_properties(&self.config, &path.to_string_lossy()),
        )?;

        // Format storage if empty.
        if path.read_dir()?.next().is_none() {
            let mut cmd = self.kafka_storage_format()?;
            cmd.arg(&config_path);

            ctx.pb.set_message("formatting storage...");
            ctx.run_command(cmd)?;
        }

        let mut cmd = self.kafka()?;
        cmd.arg(config_path);
        ctx.pb.set_message("starting kafka...");
        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
