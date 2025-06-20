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

use std::path::Path;
use std::process::{Command, Stdio};
use std::{env, thread};

use anyhow::{Context, Result};

use super::{ExecuteContext, Task};
use crate::DummyService;

/// Configuration for a docker-backed service.
///
/// The trait can be implemented for the configuration struct of a service.
/// After that, use `DockerService<Config>` as the service type.
pub trait DockerServiceConfig: Send + 'static {
    /// The unique identifier of the service.
    fn id(&self) -> String;

    /// Whether the service is managed by the user.
    ///
    /// If true, no docker container will be started and the service will be forwarded to
    /// the [`DummyService`].
    fn is_user_managed(&self) -> bool;

    /// The docker image to use, e.g. `mysql:5.7`.
    fn image(&self) -> String;

    /// Additional arguments to pass to the docker container.
    fn args(&self) -> Vec<String> {
        vec![]
    }

    /// The environment variables to pass to the docker container.
    fn envs(&self) -> Vec<(String, String)> {
        vec![]
    }

    /// The ports to expose on the host, e.g. `("0.0.0.0:23306", "3306")`.
    ///
    /// The first element of the tuple is the host port (or address), the second is the
    /// container port.
    fn ports(&self) -> Vec<(String, String)> {
        vec![]
    }

    /// The path in the container to persist data to, e.g. `/var/lib/mysql`.
    ///
    /// `Some` if the service is specified to persist data, `None` otherwise.
    fn data_path(&self) -> Option<String> {
        None
    }

    /// Network latency in milliseconds to add to the container.
    ///
    /// `Some` if latency should be added, `None` otherwise.
    fn latency_ms(&self) -> Option<u32> {
        None
    }
}

/// A service that runs a docker container with the given configuration.
pub struct DockerService<B> {
    config: B,
}

impl<B> DockerService<B>
where
    B: DockerServiceConfig,
{
    pub fn new(config: B) -> Self {
        Self { config }
    }

    /// Run `docker image inspect <image>` to check if the image exists locally.
    ///
    /// `docker run --pull=missing` does the same thing, but as we split the pull and run
    /// into two commands while `pull` does not provide such an option, we need to check
    /// the image existence manually.
    fn check_image_exists(&self) -> bool {
        Command::new("docker")
            .arg("image")
            .arg("inspect")
            .arg(self.config.image())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    fn docker_pull(&self) -> Command {
        let mut cmd = Command::new("docker");
        cmd.arg("pull").arg(self.config.image());
        cmd
    }

    fn docker_run(&self) -> Result<Command> {
        let mut cmd = Command::new("docker");
        cmd.arg("run")
            .arg("--rm")
            .arg("--name")
            .arg(format!("risedev-{}", self.id()))
            .arg("--add-host")
            .arg("host.docker.internal:host-gateway");

        // Add capabilities for traffic control if latency is configured
        if self.config.latency_ms().is_some() {
            cmd.arg("--cap-add=NET_ADMIN").arg("--cap-add=NET_RAW");
        }

        for (k, v) in self.config.envs() {
            cmd.arg("-e").arg(format!("{k}={v}"));
        }
        for (container, host) in self.config.ports() {
            cmd.arg("-p").arg(format!("{container}:{host}"));
        }

        if let Some(data_path) = self.config.data_path() {
            let path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
            fs_err::create_dir_all(&path)?;
            cmd.arg("-v")
                .arg(format!("{}:{}", path.to_string_lossy(), data_path));
        }

        cmd.arg(self.config.image());

        // Always add args for PostgreSQL
        cmd.args(self.config.args());

        Ok(cmd)
    }
}

impl<B> Task for DockerService<B>
where
    B: DockerServiceConfig,
{
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        if self.config.is_user_managed() {
            return DummyService::new(&self.id()).execute(ctx);
        }

        ctx.service(self);

        check_docker_installed()?;

        if !self.check_image_exists() {
            ctx.pb
                .set_message(format!("pulling image `{}`...", self.config.image()));
            ctx.run_command(self.docker_pull())?;
        }

        ctx.pb.set_message("starting...");
        ctx.run_command(ctx.tmux_run(self.docker_run()?)?)?;

        // If latency is configured, add it after the container starts
        if let Some(latency) = self.config.latency_ms() {
            ctx.pb.set_message("configuring network latency...");

            // Wait a moment for the container to be ready
            thread::sleep(std::time::Duration::from_secs(2));

            // Add latency using docker exec
            let mut tc_cmd = Command::new("docker");
            tc_cmd
                .arg("exec")
                .arg(format!("risedev-{}", self.id()))
                .arg("sh")
                .arg("-c")
                .arg(format!(
                    "apk add --no-cache iproute2 && tc qdisc add dev eth0 root netem delay {}ms",
                    latency
                ));

            // Run the tc command, but don't fail if it doesn't work
            let _ = tc_cmd.output();
        }

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id()
    }
}

fn check_docker_installed() -> Result<()> {
    Command::new("docker")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(anyhow::Error::from)
        .and_then(|status| status.exit_ok().map_err(anyhow::Error::from))
        .context("service requires docker to be installed")
}
