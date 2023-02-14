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
use std::process::Command;
use std::time::Duration;

use anyhow::{anyhow, Result};

use super::{ExecuteContext, Task};
use crate::MinioConfig;

const HUMMOCK_REMOTE_NAME: &str = "hummock-minio";

pub struct ConfigureMinioTask {
    mcli_path: String,
    mcli_config_path: String,
    config: MinioConfig,
}

impl ConfigureMinioTask {
    pub fn new(config: MinioConfig) -> Result<Self> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        let prefix_config = env::var("PREFIX_CONFIG")?;
        Ok(Self {
            mcli_path: format!("{}/mcli", prefix_bin),
            mcli_config_path: format!("{}/mcli", prefix_config),
            config,
        })
    }

    fn mcli(&mut self) -> Command {
        let mut cmd = Command::new(self.mcli_path.clone());
        cmd.arg("-C").arg(&self.mcli_config_path);
        cmd
    }
}

impl Task for ConfigureMinioTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.pb.set_message("waiting for online...");
        let minio_address = format!("{}:{}", self.config.address, self.config.port);
        let minio_console_address = format!(
            "{}:{}",
            self.config.console_address, self.config.console_port
        );
        let health_check_addr = format!("http://{}/minio/health/live", minio_address);
        ctx.wait_http(&health_check_addr)?;

        ctx.pb.set_message("configure...");

        let mut last_result = Err(anyhow!("unreachable"));

        // Wait for server to be configured, otherwise there might be `Server uninitialized` error.

        for _ in 0..100 {
            let mut cmd = self.mcli();
            cmd.arg("alias")
                .arg("set")
                .arg(HUMMOCK_REMOTE_NAME)
                .arg(format!("http://{}", minio_address))
                .arg(&self.config.root_user)
                .arg(&self.config.root_password);

            last_result = ctx.run_command(cmd);
            if last_result.is_ok() {
                break;
            }

            std::thread::sleep(Duration::from_millis(50));
        }

        last_result?;

        // Previously, we create a normal user for MinIO. Now we want to simplify the whole process,
        // and only create the bucket and access using root credentials.

        // let mut cmd =
        // self.mcli(); cmd.arg("admin")
        //     .arg("user")
        //     .arg("add")
        //     .arg(format!("{}/", HUMMOCK_REMOTE_NAME))
        //     .arg(&self.config.hummock_user)
        //     .arg(&self.config.hummock_password);
        // ctx.run_command(cmd)?;

        // let mut cmd = self.mcli();
        // cmd.arg("admin")
        //     .arg("policy")
        //     .arg("set")
        //     .arg(format!("{}/", HUMMOCK_REMOTE_NAME))
        //     .arg("readwrite")
        //     .arg(format!("user={}", self.config.hummock_user));
        // ctx.run_command(cmd)?;

        let mut cmd = self.mcli();
        cmd.arg("mb").arg(format!(
            "{}/{}",
            HUMMOCK_REMOTE_NAME, self.config.hummock_bucket
        ));
        ctx.run_command(cmd).ok();

        ctx.complete_spin();

        ctx.pb.set_message(format!(
            "api http://{}/, console http://{}/",
            minio_address, minio_console_address
        ));

        Ok(())
    }
}
