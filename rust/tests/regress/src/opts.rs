use anyhow::Context;
use clap::Parser;
use clap::ValueHint;
use path_absolutize::*;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

#[derive(Parser, Debug, Clone)]
pub(crate) struct Opts {
    /// Database name used to connect to pg.
    #[clap(name = "DB", long = "database", default_value = "dev")]
    pg_db_name: String,
    /// Username used to connect to postgresql.
    #[clap(name = "PG_USERNAME", short = 'u', long = "user", default_value="postgres", value_hint=ValueHint::Username)]
    pg_user_name: String,
    /// Postgresql server address to test against.
    #[clap(name = "PG_SERVER_ADDRESS", short = 'h', long = "host", value_hint = ValueHint::Hostname)]
    pg_server_host: Ipv4Addr,
    /// Postgresql server port to test against.
    #[clap(name = "PG_SERVER_PORT", short = 'p', long = "port")]
    pg_server_port: u16,
    /// Input directory containing sqls, exepected outputs.
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
}

impl Opts {
    pub(crate) fn pg_server_addr(&self) -> SocketAddrV4 {
        SocketAddrV4::new(self.pg_server_host, self.pg_server_port)
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
}
