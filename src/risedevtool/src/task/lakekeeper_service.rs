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
use crate::LakekeeperConfig;
use crate::util::stylized_risedev_subcmd;

pub struct LakekeeperService {
    config: LakekeeperConfig,
}

impl LakekeeperService {
    pub fn new(config: LakekeeperConfig) -> Result<Self> {
        Ok(Self { config })
    }

    fn lakekeeper_path(&self) -> Result<PathBuf> {
        let prefix_bin = env::var("PREFIX_BIN")?;
        Ok(Path::new(&prefix_bin).join("lakekeeper"))
    }

    fn lakekeeper(&self) -> Result<Command> {
        Ok(Command::new(self.lakekeeper_path()?))
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &LakekeeperConfig) -> Result<()> {
        // Set basic environment variables
        cmd.env("LAKEKEEPER__BIND_ADDRESS", &config.listen_address)
            .env("LAKEKEEPER__PORT", config.port.to_string())
            .env("LAKEKEEPER__PG_ENCRYPTION_KEY", &config.encryption_key);

        // Configure database backend
        if let Some(postgres_configs) = &config.provide_postgres_backend
            && let Some(pg_config) = postgres_configs.first()
        {
            let database_url = format!(
                "postgres://{}:{}@{}:{}/{}",
                pg_config.user, pg_config.password, pg_config.address, pg_config.port, "lakekeeper"
            );
            cmd.env("LAKEKEEPER__PG_DATABASE_URL_READ", &database_url)
                .env("LAKEKEEPER__PG_DATABASE_URL_WRITE", &database_url);
        }

        // Configure S3-compatible storage if MinIO is provided
        if let Some(minio_configs) = &config.provide_minio
            && let Some(minio_config) = minio_configs.first()
        {
            cmd.env(
                "LAKEKEEPER__STORAGE_S3_ENDPOINT",
                format!("http://{}:{}", minio_config.address, minio_config.port),
            )
            .env(
                "LAKEKEEPER__STORAGE_S3_ACCESS_KEY_ID",
                &minio_config.root_user,
            )
            .env(
                "LAKEKEEPER__STORAGE_S3_SECRET_ACCESS_KEY",
                &minio_config.root_password,
            )
            .env("LAKEKEEPER__STORAGE_S3_REGION", "us-east-1")
            .env("LAKEKEEPER__STORAGE_S3_PATH_STYLE_ACCESS", "true");
        }

        Ok(())
    }

    fn run_migrate(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> Result<()> {
        ctx.pb.set_message("running database migration...");
        let mut cmd = self.lakekeeper()?;
        Self::apply_command_args(&mut cmd, &self.config)?;
        cmd.arg("migrate");
        ctx.run_command(cmd)?;
        Ok(())
    }
}

impl Task for LakekeeperService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let path = self.lakekeeper_path()?;
        if !path.exists() {
            return Err(anyhow!(
                "lakekeeper binary not found in {:?}\nDid you enable lakekeeper feature in `{}`?",
                path,
                stylized_risedev_subcmd("configure")
            ));
        }

        // Run database migration first if using postgres backend
        if self.config.provide_postgres_backend.is_some() {
            self.run_migrate(ctx)?;
        }

        let mut cmd = self.lakekeeper()?;
        Self::apply_command_args(&mut cmd, &self.config)?;
        cmd.arg("serve");

        let prefix_config = env::var("PREFIX_CONFIG")?;
        let data_path = Path::new(&env::var("PREFIX_DATA")?).join(self.id());
        fs_err::create_dir_all(&data_path)?;

        // Create config directory
        let config_dir = Path::new(&prefix_config).join("lakekeeper");
        fs_err::create_dir_all(&config_dir)?;

        ctx.run_command(ctx.tmux_run(cmd)?)?;

        ctx.pb.set_message("started");

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
