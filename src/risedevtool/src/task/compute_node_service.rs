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
use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::{add_meta_node, ComputeNodeConfig};

pub struct ComputeNodeService {
    config: ComputeNodeConfig,
}

impl ComputeNodeService {
    pub fn new(config: ComputeNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn compute_node(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("compute-node")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("compute-node")))
        }
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &ComputeNodeConfig) -> Result<()> {
        cmd.arg("--listen-addr")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.exporter_port
            ))
            .arg("--advertise-addr")
            .arg(format!("{}:{}", config.address, config.port))
            .arg("--metrics-level")
            .arg("1")
            .arg("--async-stack-trace")
            .arg(&config.async_stack_trace)
            .arg("--connector-rpc-endpoint")
            .arg(&config.connector_rpc_endpoint)
            .arg("--parallelism")
            .arg(&config.parallelism.to_string())
            .arg("--total-memory-bytes")
            .arg(&config.total_memory_bytes.to_string());

        let provide_jaeger = config.provide_jaeger.as_ref().unwrap();
        match provide_jaeger.len() {
            0 => {}
            1 => {
                cmd.arg("--enable-jaeger-tracing");
            }
            other_size => {
                return Err(anyhow!(
                    "{} Jaeger instance found in config, but only 1 is needed",
                    other_size
                ))
            }
        }

        let provide_meta_node = config.provide_meta_node.as_ref().unwrap();
        add_meta_node(provide_meta_node, cmd)?;

        Ok(())
    }
}

impl Task for ComputeNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compute_node()?;

        cmd.env("RUST_BACKTRACE", "1").env(
            "TOKIO_CONSOLE_BIND",
            format!("127.0.0.1:{}", self.config.port + 1000),
        );
        // FIXME: Otherwise, CI will throw log size too large error
        // cmd.env("RW_QUERY_LOG_PATH", DEFAULT_QUERY_LOG_PATH);
        if crate::util::is_env_set("RISEDEV_ENABLE_PROFILE") {
            cmd.env(
                "RW_PROFILE_PATH",
                Path::new(&env::var("PREFIX_LOG")?).join(format!("profile-{}", self.id())),
            );
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_HEAP_PROFILE") {
            // See https://linux.die.net/man/3/jemalloc for the descriptions of profiling options
            cmd.env(
                "_RJEM_MALLOC_CONF",
                "prof:true,lg_prof_interval:40,lg_prof_sample:19,prof_prefix:compute-node",
            );
        }

        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));
        Self::apply_command_args(&mut cmd, &self.config)?;
        if self.config.enable_tiered_cache {
            let prefix_data = env::var("PREFIX_DATA")?;
            cmd.arg("--file-cache-dir").arg(
                PathBuf::from(prefix_data)
                    .join("filecache")
                    .join(self.config.port.to_string()),
            );
        }

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
            writeln!(
                &mut ctx.log,
                "Please use the following parameters to start the compute node:\n{}\n{} {}\n\n",
                get_program_env_cmd(&cmd),
                get_program_name(&cmd),
                get_program_args(&cmd)
            )?;
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
