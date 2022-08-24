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

use core::panic;
use std::time::Duration;

use clap::Parser as ClapParser;
use itertools::Itertools;
use rand::Rng;
use risingwave_sqlsmith::{
    create_table_statement_to_table, mview_sql_gen, parse_sql, print_function_table, sql_gen, Table,
};
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
    #[clap(long, default_value = "100")]
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

fn get_seed_table_sql(opt: &TestOptions) -> String {
    let seed_files = vec!["tpch.sql", "nexmark.sql"];
    seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(format!("{}/{}", opt.testdata, filename)).unwrap())
        .collect::<String>()
}

async fn create_tables(
    rng: &mut impl Rng,
    opt: &TestOptions,
    client: &tokio_postgres::Client,
) -> (Vec<Table>, Vec<Table>, String) {
    tracing::info!("Preparing tables...");

    let mut setup_sql = String::with_capacity(1000);
    let sql = get_seed_table_sql(opt);
    let statements = parse_sql(&sql);
    let mut tables = statements
        .iter()
        .map(create_table_statement_to_table)
        .collect_vec();

    for stmt in statements.iter() {
        let create_sql = stmt.to_string();
        setup_sql.push_str(&format!("{};", &create_sql));
        client.execute(&create_sql, &[]).await.unwrap();
    }

    let mut mviews = vec![];
    // Generate some mviews
    for i in 0..10 {
        let (create_sql, table) = mview_sql_gen(rng, tables.clone(), &format!("m{}", i));
        setup_sql.push_str(&format!("{};", &create_sql));
        client.execute(&create_sql, &[]).await.unwrap();
        tables.push(table.clone());
        mviews.push(table);
    }
    (tables, mviews, setup_sql)
}

async fn drop_mview_table(mview: &Table, client: &tokio_postgres::Client) {
    client
        .execute(&format!("DROP MATERIALIZED VIEW {}", mview.name), &[])
        .await
        .unwrap();
}

async fn drop_tables(mviews: &[Table], opt: &TestOptions, client: &tokio_postgres::Client) {
    tracing::info!("Cleaning tables...");

    for mview in mviews.iter().rev() {
        drop_mview_table(mview, client).await;
    }

    let seed_files = vec!["drop_tpch.sql", "drop_nexmark.sql"];
    let sql = seed_files
        .iter()
        .map(|filename| std::fs::read_to_string(format!("{}/{}", opt.testdata, filename)).unwrap())
        .collect::<String>();

    for stmt in sql.lines() {
        client.execute(stmt, &[]).await.unwrap();
    }
}

/// We diverge from PostgreSQL, instead of having undefined behaviour for overflows,
/// See: <https://github.com/singularity-data/risingwave/blob/b4eb1107bc16f8d583563f776f748632ddcaa0cb/src/expr/src/vector_op/bitwise_op.rs#L24>
/// FIXME: This approach is brittle and should change in the future,
/// when we have a better way of handling overflows.
/// Tracked by: <https://github.com/singularity-data/risingwave/issues/3900>
fn is_numeric_out_of_range_err(db_error: &DbError) -> bool {
    db_error.message().contains("Expr error: NumericOutOfRange")
}

/// Workaround to permit runtime errors not being propagated through channels.
/// FIXME: This also means some internal system errors won't be caught.
/// Tracked by: <https://github.com/singularity-data/risingwave/issues/3908#issuecomment-1186782810>
fn is_broken_chan_err(db_error: &DbError) -> bool {
    db_error
        .message()
        .contains("internal error: broken fifo_channel")
}

fn is_permissible_error(db_error: &DbError) -> bool {
    let is_internal_error = *db_error.code() == SqlState::INTERNAL_ERROR;
    is_internal_error && (is_numeric_out_of_range_err(db_error) || is_broken_chan_err(db_error))
}

/// Validate client responses
fn validate_response<_Row>(setup_sql: &str, query: &str, response: Result<_Row, PgError>) {
    match response {
        Ok(_) => {}
        Err(e) => {
            // Permit runtime errors conservatively.
            if let Some(e) = e.as_db_error()
                && is_permissible_error(e)
            {
                return;
            }
            panic!(
                "
Query failed:
---- START
-- Setup
{}
-- Query
{}
---- END

Reason:
{}
",
                setup_sql, query, e
            );
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    tracing_subscriber::fmt::init();

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
            tracing::error!("Postgres connection error: {:?}", e);
        }
    });

    let mut rng = rand::thread_rng();

    let (tables, mviews, setup_sql) = create_tables(&mut rng, &opt, &client).await;

    // Test batch
    // Queries we generate are complex, can cause overflow in
    // local execution mode.
    client
        .query("SET query_mode TO distributed;", &[])
        .await
        .unwrap();
    for _ in 0..opt.count {
        let sql = sql_gen(&mut rng, tables.clone());
        tracing::info!("Executing: {}", sql);
        let response = client.query(sql.as_str(), &[]).await;
        validate_response(&setup_sql, &format!("{};", sql), response);
    }

    // Test stream
    for _ in 0..opt.count {
        let (sql, table) = mview_sql_gen(&mut rng, tables.clone(), "stream_query");
        tracing::info!("Executing: {}", sql);
        let response = client.execute(&sql, &[]).await;
        validate_response(&setup_sql, &format!("{};", sql), response);
        drop_mview_table(&table, &client).await;
    }

    drop_tables(&mviews, &opt, &client).await;
}
