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

use std::env;
use std::io::Write;
use std::path::Path;
use std::process::Command;

use anyhow::Result;

use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::{CompactorConfig, ExecuteContext, Task, add_meta_node, add_tempo_endpoint};

pub struct CompactorService {
    config: CompactorConfig,
}

impl CompactorService {
    pub fn new(config: CompactorConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &CompactorConfig) -> Result<()> {
        println!("Compactor config: {:?}", config);

        cmd.arg("--listen-addr")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.exporter_port
            ))
            .arg("--advertise-addr")
            .arg(format!("{}:{}", config.address, config.port));
        if let Some(compaction_worker_threads_number) =
            config.compaction_worker_threads_number.as_ref()
        {
            cmd.arg("--compaction-worker-threads-number")
                .arg(format!("{}", compaction_worker_threads_number));
        }

        if config.enable_iceberg_compactor {
            cmd.arg("--compactor-mode").arg("dedicated_iceberg");
        }

        let provide_meta_node = config.provide_meta_node.as_ref().unwrap();
        add_meta_node(provide_meta_node, cmd)?;

        let provide_tempo = config.provide_tempo.as_ref().unwrap();
        add_tempo_endpoint(provide_tempo, cmd)?;

        Ok(())
    }
}

impl Task for CompactorService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl Write>) -> Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = ctx.risingwave_cmd("compactor")?;

        if crate::util::is_env_set("RISEDEV_ENABLE_PROFILE") {
            cmd.env(
                "RW_PROFILE_PATH",
                Path::new(&env::var("PREFIX_LOG")?).join(format!("profile-{}", self.id())),
            );
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_HEAP_PROFILE") {
            // See https://linux.die.net/man/3/jemalloc for the descriptions of profiling options
            let conf = "prof:true,lg_prof_interval:34,lg_prof_sample:19,prof_prefix:compactor";
            cmd.env("_RJEM_MALLOC_CONF", conf); // prefixed for macos
            cmd.env("MALLOC_CONF", conf); // unprefixed for linux
        }

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
