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
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};
use itertools::Itertools;

use super::{ExecuteContext, Task};
use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::{add_tempo_endpoint, ComputeNodeConfig, FrontendConfig, MetaNodeConfig, StandaloneConfig};

pub struct StandaloneService {
    config: StandaloneConfig,
}

impl StandaloneService {
    pub fn new(config: StandaloneConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn standalone(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(
                Path::new(&prefix_bin)
                    .join("risingwave")
                    .join("standalone"),
            ))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("standalone")))
        }
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &StandaloneConfig) -> Result<()> {
        let frontend_args = config.frontend_config.get_arg_strs();
        let compute_args = config.meta_node_config.get_arg_strs();
        let meta_args = config.compute_node_config.get_arg_strs();
        cmd.arg(format!("--meta-opts='{}'", meta_args.join(" ")))
            .arg(format!("--compute-opts='{}'", compute_args.join(" ")))
            .arg(format!("--frontend-opts='{}'", frontend_args.join(" ")));
        Ok(())
    }
}

impl Task for StandaloneService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.standalone()?;

        cmd.env("RUST_BACKTRACE", "1");
        // FIXME: Otherwise, CI will throw log size too large error
        // cmd.env("RW_QUERY_LOG_PATH", DEFAULT_QUERY_LOG_PATH);

        let prefix_config = env::var("PREFIX_CONFIG")?;
        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));
        Self::apply_command_args(&mut cmd, &self.config)?;

        if crate::util::is_env_set("RISEDEV_ENABLE_PROFILE") {
            cmd.env(
                "RW_PROFILE_PATH",
                Path::new(&env::var("PREFIX_LOG")?).join(format!("profile-{}", self.id())),
            );
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_HEAP_PROFILE") {
            // See https://linux.die.net/man/3/jemalloc for the descriptions of profiling options
            cmd.env(
                "MALLOC_CONF",
                "prof:true,lg_prof_interval:32,lg_prof_sample:19,prof_prefix:meta-node",
            );
        }

        assert!(!self.config.compute_node_config.user_managed);
        assert!(!self.config.meta_node_config.user_managed);
        assert!(!self.config.frontend_config.user_managed);

        ctx.run_command(ctx.tmux_run(cmd)?)?;
        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
