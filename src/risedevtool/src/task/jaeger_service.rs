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
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::JaegerConfig;

pub struct JaegerService {
    config: JaegerConfig,
}

impl JaegerService {
    pub fn new(config: JaegerConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn jaeger_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin)
            .join("jaeger")
            .join("jaeger-all-in-one"))
    }

    fn jaeger(&self) -> Result<Command> {
        Ok(Command::new(self.jaeger_path()?))
    }
}

impl Task for JaegerService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.jaeger_path()?;
        if !path.exists() {
            return Err(anyhow!("jeager-all-in-one binary not found in {:?}\nDid you enable tracing feature in `./risedev configure`?", path));
        }

        let mut cmd = self.jaeger()?;
        cmd.arg("--admin.http.host-port")
            .arg("127.0.0.1:14269")
            //     .arg("--collector.grpc-server.host-port")
            //     .arg("127.0.0.1:14250")
            //     .arg("--collector.http-server.host-port")
            //     .arg("127.0.0.1:14268")
            .arg("--collector.otlp.grpc.host-port")
            .arg("127.0.0.1:4317")
            // .arg("--collector.queue-size")
            // .arg("65536")
            .arg("--http-server.host-port")
            .arg("127.0.0.1:5778")
            // .arg("--processor.jaeger-binary.server-host-port")
            // .arg("127.0.0.1:6832")
            // .arg("--processor.jaeger-compact.server-host-port")
            // .arg("127.0.0.1:6831")
            // .arg("--processor.zipkin-compact.server-host-port")
            // .arg("127.0.0.1:5775")
            .arg("--query.grpc-server.host-port")
            .arg("127.0.0.1:16685")
            .arg("--query.http-server.host-port")
            .arg(format!(
                "{}:{}",
                self.config.dashboard_address, self.config.dashboard_port
            ));

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
