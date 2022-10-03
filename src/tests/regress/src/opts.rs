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

use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::{Parser, ValueHint};
use path_absolutize::*;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum DatabaseMode {
    Postgres,
    Risingwave,
}

impl From<&OsStr> for DatabaseMode {
    fn from(mode: &OsStr) -> Self {
        let mode = mode
            .to_str()
            .expect("Expect utf-8 string for database mode");
        match mode.to_lowercase().as_str() {
            "postgres" | "pg" | "postgresql" => DatabaseMode::Postgres,
            "risingwave" | "rw" => DatabaseMode::Risingwave,
            _ => unreachable!("Unrecognized database mode. Support PostgreSQL or Risingwave only."),
        }
    }
}

#[derive(Parser, Debug, Clone)]
pub(crate) struct Opts {
    /// Database name used to connect to pg.
    #[clap(name = "DB", long = "database", default_value = "dev")]
    pg_db_name: String,
    /// Username used to connect to postgresql.
    #[clap(name = "PG_USERNAME", short = 'u', long = "user", default_value="postgres", value_hint=ValueHint::Username)]
    pg_user_name: String,
    /// Postgresql server address to test against.
    #[clap(
        name = "PG_SERVER_ADDRESS",
        short = 'h',
        long = "host",
        default_value = "localhost"
    )]
    pg_server_host: String,
    /// Postgresql server port to test against.
    #[clap(name = "PG_SERVER_PORT", short = 'p', long = "port")]
    pg_server_port: u16,
    /// Input directory containing sqls, expected outputs.
    #[clap(name = "INPUT_DIR", short = 'i', long = "input", parse(from_os_str), value_hint = ValueHint::DirPath)]
    input_dir: PathBuf,
    /// Output directory containing output files, diff reuslts.
    #[clap(name = "OUTPUT_DIR", short = 'o', long = "output", parse(from_os_str), value_hint = ValueHint::DirPath)]
    output_dir: PathBuf,
    /// Schedule file containing each parallel schedule.
    #[clap(name = "SCHEDULE", short = 's', long = "schedule", parse(from_os_str), value_hint = ValueHint::FilePath)]
    schedule: PathBuf,
    /// Location for customized log file.
    #[clap(long, parse(from_os_str), default_value = "config/log4rs.yaml", value_hint=ValueHint::FilePath)]
    log4rs_config: PathBuf,
    /// Database mode
    #[clap(name = "DATABASE_MODE", long = "mode", parse(from_os_str))]
    database_mode: DatabaseMode,
}

impl Opts {
    pub(crate) fn pg_user_name(&self) -> &str {
        &self.pg_user_name
    }

    pub(crate) fn log4rs_config_path(&self) -> &Path {
        self.log4rs_config.as_path()
    }

    pub(crate) fn absolutized_input_dir(&self) -> anyhow::Result<PathBuf> {
        self.input_dir
            .absolutize()
            .map(|c| c.into_owned())
            .with_context(|| {
                format!(
                    "Failed to canonicalize input dir: {:?}",
                    self.input_dir.as_path()
                )
            })
    }

    pub(crate) fn absolutized_output_dir(&self) -> anyhow::Result<PathBuf> {
        self.output_dir
            .absolutize()
            .map(|c| c.into_owned())
            .with_context(|| {
                format!(
                    "Failed to canonicalize output dir: {:?}",
                    self.output_dir.as_path()
                )
            })
    }

    pub(crate) fn database_name(&self) -> &str {
        self.pg_db_name.as_str()
    }

    pub(crate) fn schedule_file_path(&self) -> &Path {
        self.schedule.as_path()
    }

    pub(crate) fn host(&self) -> String {
        self.pg_server_host.to_string()
    }

    pub(crate) fn port(&self) -> u16 {
        self.pg_server_port
    }

    pub(crate) fn database_mode(&self) -> DatabaseMode {
        self.database_mode
    }
}
