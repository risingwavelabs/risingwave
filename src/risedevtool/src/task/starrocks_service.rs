// Copyright 2026 RisingWave Labs
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

use std::process::{Command, Stdio};

use anyhow::{Context, Result, anyhow};

use crate::StarrocksConfig;
use crate::task::{ExecuteContext, Task};

pub struct StarrocksService {
    config: StarrocksConfig,
}
const STARROCKS_BE_HOSTNAME: &str = "starrocks-be.localhost";

impl StarrocksService {
    pub fn new(config: StarrocksConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn network_name(&self) -> String {
        format!("risedev-{}", self.config.id)
    }

    fn fe_container_name(&self) -> String {
        format!("risedev-{}-fe", self.config.id)
    }

    fn be_container_name(&self) -> String {
        format!("risedev-{}-be", self.config.id)
    }

    fn check_docker_installed(&self) -> Result<()> {
        Command::new("docker")
            .arg("--version")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(anyhow::Error::from)
            .and_then(|status| status.exit_ok().map_err(anyhow::Error::from))
    }

    fn check_image_exists(&self, image: &str) -> bool {
        Command::new("docker")
            .arg("image")
            .arg("inspect")
            .arg(image)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn docker_pull(&self, image: &str) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("pull").arg(image);
        cmd
    }

    fn docker_network_create(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("network").arg("create").arg(self.network_name());
        cmd
    }

    fn docker_fe_run(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("-d")
            .arg("--rm")
            .arg("--name")
            .arg(self.fe_container_name())
            .arg("--network")
            .arg(self.network_name())
            .arg("--network-alias")
            .arg("starrocks-fe-server")
            .arg("-p")
            .arg(format!("{}:8030", self.config.http_port))
            .arg("-p")
            .arg(format!("{}:9020", self.config.rpc_port))
            .arg("-p")
            .arg(format!("{}:9030", self.config.query_port))
            .arg(self.config.fe_image.clone())
            .arg("/opt/starrocks/fe/bin/start_fe.sh")
            .arg("--host_type")
            .arg("FQDN");
        cmd
    }

    fn docker_be_run(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("-d")
            .arg("--rm")
            .arg("--name")
            .arg(self.be_container_name())
            .arg("--hostname")
            .arg(STARROCKS_BE_HOSTNAME)
            .arg("--network")
            .arg(self.network_name())
            .arg("--network-alias")
            .arg("starrocks-be-server")
            .arg("--network-alias")
            .arg(STARROCKS_BE_HOSTNAME)
            .arg("-p")
            .arg(format!("{}:8040", self.config.be_http_port))
            .arg("-p")
            .arg(format!("{}:9050", self.config.be_heartbeat_port))
            .arg("-p")
            .arg("8040:8040")
            .arg(self.config.be_image.clone())
            .arg("/bin/bash")
            .arg("-c")
            .arg(format!(
                "sleep 15s; mysql --connect-timeout 2 -h starrocks-fe-server -P9030 -uroot -e \"alter system add backend \\\"{}:9050\\\";\"; /opt/starrocks/be/bin/start_be.sh",
                STARROCKS_BE_HOSTNAME
            ));
        cmd
    }

    fn wait_backend_ready(&self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        ctx.pb.set_message("waiting for backend registration...");
        ctx.wait(|| {
            let output = Command::new("docker")
                .arg("exec")
                .arg(self.fe_container_name())
                .arg("mysql")
                .arg("-h127.0.0.1")
                .arg("-P9030")
                .arg("-uroot")
                .arg("-N")
                .arg("-B")
                .arg("-e")
                .arg("show backends")
                .output()
                .context("failed to connect to StarRocks FE")?;
            if !output.status.success() {
                return Err(anyhow!(
                    "{}",
                    String::from_utf8_lossy(&output.stderr).trim().to_owned()
                ));
            }
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.lines().next().is_none() {
                return Err(anyhow!("no StarRocks backends registered yet"));
            }
            let alive = stdout.lines().any(|line| {
                line.split('\t')
                    .nth(8)
                    .map(|value| value.eq_ignore_ascii_case("true"))
                    .unwrap_or(false)
            });
            if alive {
                Ok(())
            } else {
                Err(anyhow!("StarRocks backend is not alive yet"))
            }
        })
        .with_context(|| format!("failed to wait for service `{}` backend ready", self.id()))?;
        Ok(())
    }
}

impl Task for StarrocksService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        if self.config.user_managed {
            return crate::DummyService::new(&self.id()).execute(ctx);
        }

        ctx.service(self);
        self.check_docker_installed()?;

        for image in [&self.config.fe_image, &self.config.be_image] {
            if !self.check_image_exists(image) {
                ctx.pb.set_message(format!("pulling image `{image}`..."));
                ctx.run_command(self.docker_pull(image))?;
            }
        }

        let _ = Command::new("docker")
            .arg("network")
            .arg("rm")
            .arg(self.network_name())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        ctx.run_command(self.docker_network_create())?;

        ctx.pb.set_message("starting fe...");
        ctx.run_command(self.docker_fe_run())?;
        ctx.wait_tcp(format!(
            "{}:{}",
            self.config.address, self.config.query_port
        ))?;

        ctx.pb.set_message("starting be...");
        ctx.run_command(self.docker_be_run())?;
        self.wait_backend_ready(ctx)?;
        ctx.complete_spin();

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
