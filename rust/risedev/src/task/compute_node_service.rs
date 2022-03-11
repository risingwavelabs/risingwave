use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::ComputeNodeConfig;

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
}

impl Task for ComputeNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let mut cmd = self.compute_node()?;

        cmd.arg("--config-path")
            .arg(Path::new(&prefix_config).join("risingwave.toml"))
            .arg("--host")
            .arg(format!("{}:{}", self.config.address, self.config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                self.config.exporter_address, self.config.exporter_port
            ))
            .arg("--metrics-level")
            .arg("1");

        let provide_jaeger = self.config.provide_jaeger.as_ref().unwrap();
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

        let provide_minio = self.config.provide_minio.as_ref().unwrap();
        match provide_minio.len() {
            0 => {}
            1 => {
                let minio = &provide_minio[0];
                cmd.arg("--state-store").arg(format!(
                    "hummock+minio://{hummock_user}:{hummock_password}@{minio_addr}:{minio_port}/{hummock_bucket}",
                    hummock_user = minio.hummock_user,
                    hummock_password = minio.hummock_password,
                    hummock_bucket = minio.hummock_bucket,
                    minio_addr = minio.address,
                    minio_port = minio.port,
                ));
            }
            other_size => {
                return Err(anyhow!(
                    "{} minio instance found in config, but only 1 is needed",
                    other_size
                ))
            }
        }

        let provide_meta_node = self.config.provide_meta_node.as_ref().unwrap();
        match provide_meta_node.len() {
            0 => {
                return Err(anyhow!(
                    "Cannot start node: no meta node found in this configuration."
                ));
            }
            1 => {
                let meta_node = &provide_meta_node[0];
                cmd.arg("--meta-address")
                    .arg(format!("http://{}:{}", meta_node.address, meta_node.port));
            }
            other_size => {
                return Err(anyhow!(
                    "Cannot start node: {} meta nodes found in this configuration, but only 1 is needed.",
                    other_size
                ));
            }
        };

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
