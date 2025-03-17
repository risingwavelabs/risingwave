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

#![feature(register_tool)]
#![register_tool(rw)]
#![allow(rw::format_error)] // test code

use core::panic;
use std::time::Duration;

use clap::Parser as ClapParser;
use risingwave_sqlsmith::print_function_table;
use risingwave_sqlsmith::test_runners::{generate, run, run_differential_testing};
use tokio_postgres::NoTls;

#[derive(ClapParser, Debug, Clone)]
#[clap(about, version, author)]
struct Opt {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(clap::Args, Clone, Debug)]
struct TestOptions {
    /// The database server host.
    #[clap(long, default_value = "localhost")]
    host: String,

    /// The database server port.
    #[clap(short, long, default_value = "4566")]
    port: u16,

    /// The database name to connect.
    #[clap(short, long, default_value = "dev")]
    db: String,

    /// The database username.
    #[clap(short, long, default_value = "root")]
    user: String,

    /// The database password.
    #[clap(short = 'w', long, default_value = "")]
    pass: String,

    /// Path to the testing data files.
    #[clap(short, long)]
    testdata: String,

    /// The number of test cases to generate.
    #[clap(long, default_value = "100")]
    count: usize,

    /// Output directory - only applicable if we are generating
    /// query while testing.
    #[clap(long)]
    generate: Option<String>,

    /// Whether to run differential testing mode.
    #[clap(long)]
    differential_testing: bool,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum Commands {
    /// Prints the currently supported function/operator table.
    #[clap(name = "print-function-table")]
    PrintFunctionTable,

    /// Run testing.
    Test(TestOptions),
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    tracing_subscriber::fmt::init();

    let opt = Opt::parse();
    let command = opt.command;
    let opt = match command {
        Commands::PrintFunctionTable => {
            println!("{}", print_function_table());
            return;
        }
        Commands::Test(test_opts) => test_opts,
    };
    let (client, connection) = tokio_postgres::Config::new()
        .host(&opt.host)
        .port(opt.port)
        .dbname(&opt.db)
        .user(&opt.user)
        .password(&opt.pass)
        .connect_timeout(Duration::from_secs(5))
        .connect(NoTls)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e));
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!("Postgres connection error: {:?}", e);
        }
    });
    if opt.differential_testing {
        return run_differential_testing(&client, &opt.testdata, opt.count, None)
            .await
            .unwrap();
    }
    if let Some(outdir) = opt.generate {
        generate(&client, &opt.testdata, opt.count, &outdir, None).await;
    } else {
        run(&client, &opt.testdata, opt.count, None).await;
    }
}
