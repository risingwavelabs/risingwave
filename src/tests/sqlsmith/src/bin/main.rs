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

use std::time::Duration;

use clap::Parser as ClapParser;
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use risingwave_sqlsmith::{sql_gen, Table};
use tokio_postgres::NoTls;

#[derive(ClapParser, Debug, Clone)]
#[clap(about, version, author)]
struct Opt {
    /// The database server host.
    #[clap(short, long, default_value = "localhost")]
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
}

async fn create_tables(opt: &Opt, client: &tokio_postgres::Client) -> Vec<Table> {
    log::info!("Preparing tables...");

    let sql = std::fs::read_to_string(format!("{}/tpch.sql", opt.testdata)).unwrap();

    let statements =
        Parser::parse_sql(&sql).unwrap_or_else(|_| panic!("Failed to parse SQL: {}", sql));
    for stmt in statements.iter() {
        let create_sql = format!("{}", stmt);
        client.execute(&create_sql, &[]).await.unwrap();
    }
    statements
        .into_iter()
        .map(|s| match s {
            Statement::CreateTable { name, columns, .. } => Table {
                name: name.0[0].value.clone(),
                columns: columns.iter().map(|c| c.clone().into()).collect(),
            },
            _ => panic!("Unexpected statement: {}", s),
        })
        .collect()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 5)]
async fn main() {
    env_logger::init();

    let opt = Opt::parse();
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

    let tables = create_tables(&opt, &client).await;

    let mut rng = rand::thread_rng();

    for _ in 0..100 {
        let sql = sql_gen(&mut rng, tables.clone());
        log::info!("Executing: {}", sql);
        let _ = client.query(sql.as_str(), &[]).await.unwrap();
    }
}
