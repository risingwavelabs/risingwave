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

#![feature(let_chains)]

use core::panic;
use std::time::Duration;

use clap::Parser as ClapParser;
use itertools::Itertools;
use rand::Rng;
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use risingwave_sqlsmith::{mview_sql_gen, print_function_table, sql_gen, Table};
use tokio_postgres::error::{DbError, Error as PgError, SqlState};
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
    #[clap(long, default_value = "1000")]
    count: usize,
}

#[derive(clap::Subcommand, Clone, Debug)]
enum Commands {
    /// Prints the currently supported function/operator table.
    #[clap(name = "print-function-table")]
    PrintFunctionTable,

    /// Run testing.
    Test(TestOptions),
}

async fn create_tables(
    rng: &mut impl Rng,
    opt: &TestOptions,
    client: &tokio_postgres::Client,
) -> (Vec<Table>, Vec<Table>) {
    log::info!("Preparing tables...");

    let sql = std::fs::read_to_string(format!("{}/tpch.sql", opt.testdata)).unwrap();

    let statements =
        Parser::parse_sql(&sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql));
    let n_statements = statements.len();

    for stmt in statements.iter() {
        let create_sql = format!("{}", stmt);
        client.execute(&create_sql, &[]).await.unwrap();
    }
    let mut tables = statements
        .into_iter()
        .map(|s| match s {
            Statement::CreateTable { name, columns, .. } => Table {
                name: name.0[0].value.clone(),
                columns: columns.iter().map(|c| c.clone().into()).collect(),
            },
            _ => panic!("Unexpected statement: {}", s),
        })
        .collect_vec();

    let mut mviews = vec![];
    // Generate Materialized Views 1:1 with tables, so they have equal weight
    // of being queried.
    for i in 0..n_statements {
        let (create_sql, table) = mview_sql_gen(rng, tables.clone(), &format!("m{}", i));
        client.execute(&create_sql, &[]).await.unwrap();
        tables.push(table.clone());
        mviews.push(table);
    }
    (tables, mviews)
}

async fn drop_tables(mviews: &[Table], opt: &TestOptions, client: &tokio_postgres::Client) {
    log::info!("Cleaning tables...");
    let sql = std::fs::read_to_string(format!("{}/drop_tpch.sql", opt.testdata)).unwrap();
    for Table { name, .. } in mviews.iter().rev() {
        client
            .execute(&format!("DROP MATERIALIZED VIEW {}", name), &[])
            .await
            .unwrap();
    }
    for stmt in sql.lines() {
        client.execute(stmt, &[]).await.unwrap();
    }
}

/// We diverge from PostgreSQL, instead of having undefined behaviour for overflows,
/// See: <https://github.com/singularity-data/risingwave/blob/b4eb1107bc16f8d583563f776f748632ddcaa0cb/src/expr/src/vector_op/bitwise_op.rs#L24>
/// FIXME: This approach is brittle and should change in the future,
/// when we have a better way of handling overflows.
/// Tracked here: <https://github.com/singularity-data/risingwave/issues/3900>
fn is_numeric_out_of_range_err(db_error: &DbError) -> bool {
    let is_internal_error = *db_error.code() == SqlState::INTERNAL_ERROR;
    let is_numeric_error = db_error.message().contains("Expr error: NumericOutOfRange");
    is_internal_error && is_numeric_error
}

/// Validate client responses
fn validate_response<_Row>(response: Result<_Row, PgError>) {
    match response {
        Ok(_) => {}
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error() && is_numeric_out_of_range_err(e) {
                return;
            }
            panic!("{}", e);
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    env_logger::init();

    let opt = Opt::parse();
    let opt = match opt.command {
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
            log::error!("Postgres connection error: {:?}", e);
        }
    });

    let mut rng = rand::thread_rng();

    let (tables, mviews) = create_tables(&mut rng, &opt, &client).await;

    for _ in 0..opt.count {
        let sql = sql_gen(&mut rng, tables.clone());
        log::info!("Executing: {}", sql);
        let response = client.query(sql.as_str(), &[]).await;
        validate_response(response);
    }

    drop_tables(&mviews, &opt, &client).await;
}
