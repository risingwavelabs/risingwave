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
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, anyhow};

use super::{ExecuteContext, Task};
use crate::MoatConfig;
use crate::util::stylized_risedev_subcmd;

#[derive(Debug)]
pub struct MoatService {
    config: MoatConfig,
}

impl MoatService {
    pub fn new(config: MoatConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn moat_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("moat"))
    }

    fn moat(&self) -> Result<Command> {
        Ok(Command::new(self.moat_path()?))
    }

    fn cache_dir(&self) -> Result<PathBuf> {
        let prefix_data = env::var("PREFIX_DATA")?;
        Ok(Path::new(&prefix_data)
            .join("moat")
            .join(format!("cache-{}", self.config.port)))
    }

    pub fn apply_command_args(cmd: &mut Command, config: &MoatConfig) -> Result<()> {
        let minios = config
            .provide_minio
            .as_ref()
            .ok_or_else(|| anyhow!("minio not provided"))?;

        if minios.is_empty() || minios.len() > 1 {
            return Err(anyhow!(
                "expected exactly one minio instance, got {}",
                minios.len()
            ));
        }

        let minio = &minios[0];

        cmd.arg("--listen").arg(format!("0.0.0.0:{}", config.port));
        cmd.arg("--peer")
            .arg(format!("{}:{}", config.address, config.port));
        cmd.arg("--bootstrap-peers")
            .arg(format!("{}:{}", config.address, config.port));
        cmd.arg("--s3-endpoint")
            .arg(format!("http://{}:{}", minio.listen_address, minio.port));
        cmd.arg("--s3-access-key-id").arg(&minio.root_user);
        cmd.arg("--s3-secret-access-key").arg(&minio.root_password);
        cmd.arg("--s3-bucket").arg(&minio.hummock_bucket);
        cmd.arg("--s3-region").arg("us-east-1");
        cmd.arg("--weight").arg("1");
        cmd.arg("--mem").arg("64MiB");
        cmd.arg("--disk").arg("1GiB");

        let prefix_log = env::var("PREFIX_LOG")?;
        let log_dir = Path::new(&prefix_log)
            .join("moat")
            .join(format!("moat:{}", config.port));
        cmd.arg("--telemetry-logging-dir").arg(log_dir);

        cmd.env("RUST_LOG", "info");

        Ok(())
    }

    // fn minio_path(&self) -> Result<PathBuf> {
    //     let prefix_bin = env::var("PREFIX_BIN")?;
    //     Ok(Path::new(&prefix_bin).join("minio"))
    // }

    // fn minio(&self) -> Result<Command> {
    //     Ok(Command::new(self.minio_path()?))
    // }

    // /// Apply command args according to config
    // pub fn apply_command_args(cmd: &mut Command, config: &MinioConfig) -> Result<()> {
    //     cmd.arg("server")
    //         .arg("--address")
    //         .arg(format!("{}:{}", config.listen_address, config.port))
    //         .arg("--console-address")
    //         .arg(format!("{}:{}", config.listen_address, config.console_port))
    //         .env("MINIO_ROOT_USER", &config.root_user)
    //         .env("MINIO_ROOT_PASSWORD", &config.root_password)
    //         .env("MINIO_PROMETHEUS_AUTH_TYPE", "public")
    //         // Allow MinIO to be used on root disk, bypass restriction.
    //         // https://github.com/risingwavelabs/risingwave/pull/3012
    //         // https://docs.min.io/minio/baremetal/installation/deploy-minio-single-node-single-drive.html#id3
    //         .env("MINIO_CI_CD", "1");
    //     if config.api_requests_max > 0 {
    //         // Rate limit minio
    //         cmd.env(
    //             "MINIO_API_REQUESTS_MAX",
    //             config.api_requests_max.to_string(),
    //         );
    //     }
    //     if !config.api_requests_deadline.is_empty() {
    //         // Rate limit minio
    //         cmd.env("MINIO_API_REQUESTS_DEADLINE", &config.api_requests_deadline);
    //     }

    //     let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
    //     match provide_prometheus.len() {
    //         0 => {}
    //         1 => {
    //             let prometheus = &provide_prometheus[0];
    //             cmd.env(
    //                 "MINIO_PROMETHEUS_URL",
    //                 format!("http://{}:{}", prometheus.address, prometheus.port),
    //             );
    //         }
    //         other_length => {
    //             return Err(anyhow!("expected 0 or 1 prometheus, get {}", other_length));
    //         }
    //     }

    //     Ok(())
    // }
}

impl Task for MoatService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.moat_path()?;
        if !path.exists() {
            return Err(anyhow!(
                "moat binary not found in {:?}\nDid you enable moat feature in `{}`?",
                path,
                stylized_risedev_subcmd("configure")
            ));
        }

        let mut cmd = self.moat()?;
        cmd.arg("--dir").arg(self.cache_dir()?);

        Self::apply_command_args(&mut cmd, &self.config)?;
        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
