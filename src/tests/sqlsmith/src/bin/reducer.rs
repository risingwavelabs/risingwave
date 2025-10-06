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

use std::process::Command;
use std::time::Duration;

use clap::Parser;
use risingwave_sqlsmith::reducer::shrink_file;
use thiserror_ext::AsReport;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;

/// Reduce an sql query
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input file
    #[arg(short, long)]
    input_file: String,

    /// Output file
    #[arg(short, long)]
    output_file: String,

    /// Command to restore RW
    #[clap(long)]
    run_rw_cmd: String,

    /// Reduction mode
    #[arg(short = 'm', long, default_value = "path-based")]
    mode: ReductionModeArg,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(console::colors_enabled_stderr() && console::colors_enabled())
        .with_writer(std::io::stderr)
        .try_init();

    let args = Args::parse();

    // Execute restore command before connecting to database
    tracing::info!("Executing restore command: {}", args.run_rw_cmd);
    let status = Command::new("sh").arg("-c").arg(&args.run_rw_cmd).status();

    match status {
        Ok(s) if s.success() => tracing::info!("Restore command executed successfully"),
        Ok(s) => {
            tracing::error!("Restore command failed with status: {}", s);
            panic!("Failed to restore RW");
        }
        Err(err) => {
            tracing::error!("Failed to execute restore command: {}", err.as_report());
            panic!("Failed to execute restore command: {}", err.as_report());
        }
    }

    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .port(4566)
        .dbname("dev")
        .user("root")
        .password("")
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e.as_report()));

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e.as_report(), "Postgres connection error");
        }
    });

    shrink_file(
        &args.input_file,
        &args.output_file,
        client,
        &args.run_rw_cmd,
    )
    .await
    .unwrap();
}
