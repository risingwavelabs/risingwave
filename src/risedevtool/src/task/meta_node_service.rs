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
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::MetaNodeConfig;

pub struct MetaNodeService {
    config: MetaNodeConfig,
}

impl MetaNodeService {
    pub fn new(config: MetaNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn meta_node(&self) -> Result<Command> {
        let prefix_bin = env::var("PREFIX_BIN")?;

        if let Ok(x) = env::var("ENABLE_ALL_IN_ONE") && x == "true" {
            Ok(Command::new(Path::new(&prefix_bin).join("risingwave").join("meta-node")))
        } else {
            Ok(Command::new(Path::new(&prefix_bin).join("meta-node")))
        }
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &MetaNodeConfig) -> Result<()> {
        cmd.arg("--listen-addr")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--host")
            .arg(config.address.clone())
            .arg("--dashboard-host")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.dashboard_port
            ));

        cmd.arg("--prometheus-host").arg(format!(
            "{}:{}",
            config.listen_address, config.exporter_port
        ));

        match config.provide_etcd_backend.as_ref().map(|v| &v[..]) {
            Some([]) => {
                cmd.arg("--backend").arg("mem");
            }
            Some([etcd]) => {
                cmd.arg("--backend")
                    .arg("etcd")
                    .arg("--etcd-endpoints")
                    .arg(format!("{}:{}", etcd.address, etcd.port));
            }
            _ => {
                return Err(anyhow!(
                    "unexpected etcd config {:?}",
                    config.provide_etcd_backend
                ))
            }
        }

        if config.enable_dashboard_v2 {
            cmd.arg("--dashboard-ui-path")
                .arg(env::var("PREFIX_UI").unwrap_or_else(|_| ".risingwave/ui".to_owned()));
        }

        if config.unsafe_disable_recovery {
            cmd.arg("--disable-recovery");
        }

        if let Some(sec) = config.max_idle_secs_to_exit {
            if sec > 0 {
                cmd.arg("--dangerous-max-idle-secs").arg(format!("{}", sec));
            }
        }

        cmd.arg("--vacuum-interval-sec")
            .arg(format!("{}", config.vacuum_interval_sec))
            .arg("--collect-gc-watermark-spin-interval-sec")
            .arg(format!("{}", config.collect_gc_watermark_spin_interval_sec))
            .arg("--min-sst-retention-time-sec")
            .arg(format!("{}", config.min_sst_retention_time_sec));

        Ok(())
    }
}

impl Task for MetaNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = self.meta_node()?;

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
                "prof:true,lg_prof_interval:32,lg_prof_sample:19,prof_prefix:meta-node",
            );
        }

        Self::apply_command_args(&mut cmd, &self.config)?;

        let prefix_config = env::var("PREFIX_CONFIG")?;
        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"));

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
