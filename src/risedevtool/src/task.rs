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

mod compactor_service;
mod compute_node_service;
mod configure_tmux_service;
mod connector_service;
mod ensure_stop_service;
mod etcd_service;
mod frontend_service;
mod grafana_service;
mod kafka_service;
mod meta_node_service;
mod minio_service;
mod prometheus_service;
mod pubsub_service;
mod redis_service;
mod task_configure_grpc_node;
mod task_configure_minio;
mod task_etcd_ready_check;
mod task_kafka_ready_check;
mod task_pubsub_emu_ready_check;
mod task_redis_ready_check;
mod tempo_service;
mod utils;
mod zookeeper_service;

use std::env;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use indicatif::ProgressBar;
use reqwest::blocking::Client;
use tempfile::TempDir;
pub use utils::*;

pub use self::compactor_service::*;
pub use self::compute_node_service::*;
pub use self::configure_tmux_service::*;
pub use self::connector_service::*;
pub use self::ensure_stop_service::*;
pub use self::etcd_service::*;
pub use self::frontend_service::*;
pub use self::grafana_service::*;
pub use self::kafka_service::*;
pub use self::meta_node_service::*;
pub use self::minio_service::*;
pub use self::prometheus_service::*;
pub use self::pubsub_service::*;
pub use self::redis_service::*;
pub use self::task_configure_grpc_node::*;
pub use self::task_configure_minio::*;
pub use self::task_etcd_ready_check::*;
pub use self::task_kafka_ready_check::*;
pub use self::task_pubsub_emu_ready_check::*;
pub use self::task_redis_ready_check::*;
pub use self::tempo_service::*;
pub use self::zookeeper_service::*;
use crate::util::{complete_spin, get_program_args, get_program_name};
use crate::wait::{wait, wait_tcp_available};

pub trait Task: 'static + Send {
    /// Execute the task
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()>;

    /// Get task id used in progress bar
    fn id(&self) -> String {
        "<task>".into()
    }
}

/// A context used in task execution
pub struct ExecuteContext<W>
where
    W: std::io::Write,
{
    /// Global log file object. (aka. risedev.log)
    pub log: W,

    /// Progress bar on screen.
    pub pb: ProgressBar,

    /// The directory for checking status.
    ///
    /// RiseDev will instruct every task to output their status to a file in temporary folder. By
    /// checking this file, we can know whether a task has early exited.
    pub status_dir: Arc<TempDir>,

    /// The current service id running in this context.
    pub id: Option<String>,

    /// The status file corresponding to the current context.
    pub status_file: Option<PathBuf>,
}

impl<W> ExecuteContext<W>
where
    W: std::io::Write,
{
    pub fn new(log: W, pb: ProgressBar, status_dir: Arc<TempDir>) -> Self {
        Self {
            log,
            pb,
            status_dir,
            status_file: None,
            id: None,
        }
    }

    pub fn service(&mut self, task: &impl Task) {
        let id = task.id();
        if !id.is_empty() {
            self.pb.set_prefix(id.clone());
            self.status_file = Some(self.status_dir.path().join(format!("{}.status", id)));
            self.id = Some(id);
        }
    }

    pub fn run_command(&mut self, mut cmd: Command) -> Result<Output> {
        let program_name = get_program_name(&cmd);

        writeln!(self.log, "> {} {}", program_name, get_program_args(&cmd))?;

        let output = cmd.output()?;

        let mut full_output = String::from_utf8_lossy(&output.stdout).to_string();
        full_output.extend(String::from_utf8_lossy(&output.stderr).chars());

        write!(self.log, "{}", full_output)?;

        writeln!(
            self.log,
            "({} exited with {:?})",
            program_name,
            output.status.code()
        )?;

        writeln!(self.log, "---")?;

        output.status.exit_ok()?;

        Ok(output)
    }

    pub fn complete_spin(&mut self) {
        complete_spin(&self.pb);
    }

    pub fn status_path(&self) -> PathBuf {
        self.status_file.clone().unwrap()
    }

    pub fn log_path(&self) -> anyhow::Result<PathBuf> {
        let prefix_log = env::var("PREFIX_LOG")?;
        Ok(Path::new(&prefix_log).join(format!("{}.log", self.id.as_ref().unwrap())))
    }

    pub fn wait_tcp(&mut self, server: impl AsRef<str>) -> anyhow::Result<()> {
        let addr = server.as_ref().parse()?;
        wait(
            || {
                TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
                Ok(())
            },
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            Some(Duration::from_secs(30)),
            true,
        )?;
        Ok(())
    }

    pub fn wait_http(&mut self, server: impl AsRef<str>) -> anyhow::Result<()> {
        let server = server.as_ref();
        wait(
            || {
                let resp = Client::new()
                    .get(server)
                    .timeout(Duration::from_secs(1))
                    .body("")
                    .send()?;
                if resp.status().is_success() {
                    Ok(())
                } else {
                    Err(anyhow!("http failed with status: {}", resp.status()))
                }
            },
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            Some(Duration::from_secs(30)),
            true,
        )
    }

    pub fn wait_http_with_cb(
        &mut self,
        server: impl AsRef<str>,
        cb: impl Fn(&str) -> bool,
    ) -> anyhow::Result<()> {
        let server = server.as_ref();
        wait(
            || {
                let resp = Client::new()
                    .get(server)
                    .timeout(Duration::from_secs(1))
                    .body("")
                    .send()?;
                if resp.status().is_success() {
                    let data = resp.text()?;
                    if cb(&data) {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            "http health check callback failed with body: {:?}",
                            data
                        ))
                    }
                } else {
                    Err(anyhow!("http failed with status: {}", resp.status()))
                }
            },
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            Some(Duration::from_secs(30)),
            true,
        )
    }

    pub fn wait(&mut self, wait_func: impl FnMut() -> Result<()>) -> anyhow::Result<()> {
        wait(
            wait_func,
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            Some(Duration::from_secs(30)),
            true,
        )
    }

    /// Wait for a TCP port to close
    pub fn wait_tcp_close(&mut self, server: impl AsRef<str>) -> anyhow::Result<()> {
        wait_tcp_available(server, Some(Duration::from_secs(30)))?;
        Ok(())
    }

    /// Wait for a user-managed service to be available
    pub fn wait_tcp_user(&mut self, server: impl AsRef<str>) -> anyhow::Result<()> {
        let addr = server.as_ref().parse()?;
        wait(
            || {
                TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
                Ok(())
            },
            &mut self.log,
            self.status_file.as_ref().unwrap(),
            self.id.as_ref().unwrap(),
            None,
            false,
        )?;
        Ok(())
    }

    pub fn tmux_run(&self, user_cmd: Command) -> anyhow::Result<Command> {
        let prefix_path = env::var("PREFIX_BIN")?;
        let mut cmd = Command::new("tmux");
        cmd.arg("new-window")
            // Set target name
            .arg("-t")
            .arg(RISEDEV_SESSION_NAME)
            // Switch to background window
            .arg("-d")
            // Set session name for this window
            .arg("-n")
            .arg(self.id.as_ref().unwrap());
        if let Some(dir) = user_cmd.get_current_dir() {
            cmd.arg("-c").arg(dir);
        }
        for (k, v) in user_cmd.get_envs() {
            cmd.arg("-e");
            if let Some(v) = v {
                cmd.arg(format!("{}={}", k.to_string_lossy(), v.to_string_lossy()));
            } else {
                cmd.arg(k);
            }
        }
        cmd.arg(Path::new(&prefix_path).join("run_command.sh"));
        cmd.arg(self.log_path()?);
        cmd.arg(self.status_path());
        cmd.arg(user_cmd.get_program());
        for arg in user_cmd.get_args() {
            cmd.arg(arg);
        }
        Ok(cmd)
    }
}
