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
use std::fs;
use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::MinioConfig;

pub struct MinioService {
    config: MinioConfig,
}

impl MinioService {
    pub fn new(config: MinioConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn minio_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("minio"))
    }

    fn minio(&self) -> Result<Command> {
        Ok(Command::new(self.minio_path()?))
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &MinioConfig) -> Result<()> {
        println!("这里{:?} {:?} {:?}", config.listen_address, config.port, config.console_port);
        // let cert_file_content = fs::read_to_string("/Users/wangcongyi/singularity/risingwave/public.crt")?;
        // let cert_file_content2= fs::read_to_string("/Users/wangcongyi/singularity/risingwave/public.crt")?;
        // let ab = fs::read_to_string("/Users/wangcongyi/singularity/risingwave/private.key")?;
        cmd.arg("server")
            .arg("--address")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--console-address")
            .arg(format!("{}:{}", config.listen_address, config.console_port))
            // .arg("--protocol")
            // .arg("=https")
            .env("MINIO_ROOT_USER", &config.root_user)
            .env("MINIO_ROOT_PASSWORD", &config.root_password)
            .env("MINIO_PROMETHEUS_AUTH_TYPE", "public")
            .env("MINIO_BROWSER", "on")
            .env("MINIO_CERT_CER", "on")
            .env("MINIO_CERT_CERTIFICATE", "/Users/wangcongyi/singularity/risingwave/public.crt")
            .env("MINIO_CERT_PRIVATE_KEY", "/Users/wangcongyi/singularity/risingwave/private.key")
            .env("MINIO_CERT_CERTIFICATE_CHAIN", "/Users/wangcongyi/singularity/risingwave/csr.csr")
            // Allow MinIO to be used on root disk, bypass restriction.
            // https://github.com/risingwavelabs/risingwave/pull/3012
            // https://docs.min.io/minio/baremetal/installation/deploy-minio-single-node-single-drive.html#id3
            .env("MINIO_CI_CD", "1");

        let provide_prometheus = config.provide_prometheus.as_ref().unwrap();
        match provide_prometheus.len() {
            0 => {}
            1 => {
                let prometheus = &provide_prometheus[0];
                cmd.env(
                    "MINIO_PROMETHEUS_URL",
                    format!("http://{}:{}", prometheus.address, prometheus.port),
                );
            }
            other_length => {
                return Err(anyhow!("expected 0 or 1 prometheus, get {}", other_length))
            }
        }

        Ok(())
    }
}

impl Task for MinioService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.minio_path()?;
        if !path.exists() {
            return Err(anyhow!("minio binary not found in {:?}\nDid you enable minio feature in `./risedev configure`?", path));
        }

        let mut cmd = self.minio()?;

        Self::apply_command_args(&mut cmd, &self.config)?;

        println!("command = {:?}", cmd);

        let prefix_config = env::var("PREFIX_CONFIG")?;

        let data_path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        fs_err::create_dir_all(&data_path)?;

        // // 添加代码：将验证文件写入 minio/certs/CAs/ 目录
        // let ca_path = Path::new(&prefix_config).join("minio").join("certs").join("CAs");
        // let certs_path = Path::new(&prefix_config).join("minio").join("certs");
        
        // fs_err::create_dir_all(&certs_path)?;
        // let cert_file_content = fs::read_to_string("/Users/wangcongyi/singularity/risingwave/public.crt")?;
        // let cert_file_content2 = fs::read_to_string("/Users/wangcongyi/singularity/risingwave/public.crt")?;
        // let ab = fs::read_to_string("/Users/wangcongyi/singularity/risingwave/private.key")?;
        // fs_err::write(certs_path.join("public.crt"), cert_file_content)?;
        // fs_err::write(certs_path.join("private.key"), ab)?;
        // fs_err::write(ca_path.join("cert_chain_file.crt"), cert_file_content2)?;
        // println!("prefix_config = {:?} {:?}", prefix_config, data_path);
        cmd.arg("--config-dir")
            .arg(Path::new(&prefix_config).join("minio"))
            .arg(&data_path);

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
