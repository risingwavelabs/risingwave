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

use anyhow::{bail, Context};
use tokio::process::Command;
use tracing::{debug, info};

use crate::Opts;

const PG_DB_NAME: &str = "postgres";

pub(crate) struct Psql {
    opts: Opts,
}

pub(crate) struct PsqlCommandBuilder {
    database: String,
    cmd: Command,
}

impl Psql {
    pub(crate) fn new(opts: Opts) -> Self {
        Self { opts }
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn init(&self) -> anyhow::Result<()> {
        info!("Initializing instances.");

        for _db in [self.opts.database_name(), PG_DB_NAME] {
            // self.drop_database_if_exists(db).await?;
            // self.create_database(db).await?;
        }

        Ok(())
    }

    pub(crate) async fn create_database<S: AsRef<str>>(&self, db: S) -> anyhow::Result<()> {
        info!("Creating database {}", db.as_ref());

        let mut cmd = PsqlCommandBuilder::new(PG_DB_NAME, &self.opts)
            .add_cmd(format!(
                r#"CREATE DATABASE "{}" TEMPLATE=template0 LC_COLLATE='C' LC_CTYPE='C'"#,
                db.as_ref()
            ))
            .build();

        let status = cmd
            .status()
            .await
            .with_context(|| format!("Failed to execute command: {:?}", cmd))?;
        if status.success() {
            info!("Succeeded to create database {}", db.as_ref());
            Ok(())
        } else {
            bail!("Failed to create database {}", db.as_ref())
        }
    }

    pub(crate) async fn drop_database_if_exists<S: AsRef<str>>(&self, db: S) -> anyhow::Result<()> {
        info!("Dropping database {} if exists", db.as_ref());

        let mut cmd = PsqlCommandBuilder::new("postgres", &self.opts)
            .add_cmd(format!(r#"DROP DATABASE IF EXISTS "{}""#, db.as_ref()))
            .build();

        debug!("Dropping database command is: {:?}", cmd);

        let status = cmd
            .status()
            .await
            .with_context(|| format!("Failed to execute command: {:?}", cmd))?;

        if status.success() {
            info!("Succeeded to drop database {}", db.as_ref());
            Ok(())
        } else {
            bail!("Failed to drop database {}", db.as_ref())
        }
    }
}

impl PsqlCommandBuilder {
    pub(crate) fn new<S: ToString>(database: S, opts: &Opts) -> Self {
        let mut cmd = Command::new("psql");
        cmd.arg("-X")
            .args(["-h", opts.host().as_str()])
            .args(["-p", format!("{}", opts.port()).as_str()]);

        Self {
            database: database.to_string(),
            cmd,
        }
    }

    pub(crate) fn add_cmd<S: AsRef<str>>(mut self, cmd: S) -> Self {
        let cmd = cmd.as_ref();
        let mut escaped_cmd = "".to_string();

        // Escape any shell double-quote metacharacters
        for c in cmd.chars() {
            if r#"\"$`"#.contains(c) {
                escaped_cmd.push('\\');
            }
            escaped_cmd.push(c);
        }

        // Append command
        self.cmd.args(["-c", &escaped_cmd]);

        self
    }

    pub(crate) fn build(mut self) -> Command {
        self.cmd.arg(&self.database);
        self.cmd
    }
}
