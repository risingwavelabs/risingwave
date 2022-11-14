// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::env;
use std::io::Write;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::{
    add_meta_node, add_storage_backend, CompactorConfig, ExecuteContext, HummockInMemoryStrategy,
    Task,
};

pub struct CompactorService {
    config: CompactorConfig,
}

impl CompactorService {
    pub fn new(config: CompactorConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn compactor(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("compactor")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("compactor")))
        }
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &CompactorConfig) -> Result<()> {
        cmd.arg("--host")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.exporter_port
            ))
            .arg("--client-address")
            .arg(format!("{}:{}", config.address, config.port))
            .arg("--metrics-level")
            .arg("1")
            .arg("--max-concurrent-task-number")
            .arg(format!("{}", config.max_concurrent_task_number));
        if let Some(compaction_worker_threads_number) =
            config.compaction_worker_threads_number.as_ref()
        {
            cmd.arg("--compaction-worker-threads-number")
                .arg(format!("{}", compaction_worker_threads_number));
        }

        let provide_minio = config.provide_minio.as_ref().unwrap();
        let provide_aws_s3 = config.provide_aws_s3.as_ref().unwrap();
        add_storage_backend(
            &config.id,
            provide_minio,
            provide_aws_s3,
            HummockInMemoryStrategy::Shared,
            cmd,
        )?;

        let provide_meta_node = config.provide_meta_node.as_ref().unwrap();
        add_meta_node(provide_meta_node, cmd)?;

        Ok(())
    }
}

impl Task for CompactorService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compactor()?;

        cmd.env("RUST_BACKTRACE", "1");
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
                "prof:true,lg_prof_interval:34,lg_prof_sample:19,prof_prefix:compactor",
            );
        }

        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));
        Self::apply_command_args(&mut cmd, &self.config)?;

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
            writeln!(
                &mut ctx.log,
                "Please use the following parameters to start the compactor:\n{}\n{} {}\n\n",
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
