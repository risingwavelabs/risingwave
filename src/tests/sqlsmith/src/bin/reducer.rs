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

use std::time::Duration;

use clap::{Parser, ValueEnum};
use risingwave_sqlsmith::reducer::shrink_file;
use risingwave_sqlsmith::sqlreduce::{ReductionMode, Strategy};
use thiserror_ext::AsReport;
use tokio_postgres::NoTls;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, ValueEnum)]
enum ReductionStrategy {
    Single,
    Aggressive,
    Consecutive,
}

#[derive(Debug, Clone, ValueEnum)]
enum ReductionModeArg {
    PathBased,
    PassBased,
}

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

    /// Reducer strategy
    #[arg(short, long, default_value = "single")]
    strategy: ReductionStrategy,

    /// For consecutive strategy, number of elements to reduce at once (used only when strategy = consecutive)
    #[arg(short, long, default_value_t = 2)]
    consecutive_k: usize,

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

    let strategy = match args.strategy {
        ReductionStrategy::Single => Strategy::Single,
        ReductionStrategy::Aggressive => Strategy::Aggressive,
        ReductionStrategy::Consecutive => Strategy::Consecutive(args.consecutive_k),
    };

    let mode = match args.mode {
        ReductionModeArg::PathBased => ReductionMode::PathBased,
        ReductionModeArg::PassBased => ReductionMode::PassBased,
    };

    shrink_file(
        &args.input_file,
        &args.output_file,
        strategy,
        mode,
        client,
        &args.run_rw_cmd,
    )
    .await
    .unwrap();
}
