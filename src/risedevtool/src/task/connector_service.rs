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

use crate::{ConnectorNodeConfig, ExecuteContext, Task};
const CONNECTOR_MAIN_CLASS: &str = "com.risingwave.connector.ConnectorService";

pub struct ConnectorNodeService {
    pub config: ConnectorNodeConfig,
}

impl ConnectorNodeService {
    pub fn new(config: ConnectorNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn connector_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("connector-node"))
    }
}

impl Task for ConnectorNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting");
        let path = self.connector_path()?;
        if !path.exists() {
            return Err(anyhow!("RisingWave connector binary not found in {:?}\nDid you enable risingwave connector feature in `./risedev configure`?", path));
        }
        let prefix_bin = env::var("PREFIX_BIN")?;
        let classpath = Path::new(&prefix_bin).join("connector-node").join("libs/*");
        let mut cmd = Command::new("java");
        cmd.arg("-classpath")
            .arg(classpath)
            .arg(CONNECTOR_MAIN_CLASS)
            .arg("--port")
            .arg(self.config.port.to_string());
        ctx.run_command(ctx.tmux_run(cmd)?)?;
        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
