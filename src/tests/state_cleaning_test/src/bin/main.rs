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

use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use regex::Regex;
use serde::Deserialize;
use serde_with::{OneOrMany, serde_as};
use tokio::fs;
use tokio_postgres::{NoTls, SimpleQueryMessage};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{debug, error, info};

#[derive(clap::Parser, Clone, Debug)]
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
}

#[derive(Debug, Clone, Deserialize)]
struct BoundTable {
    pattern: String,
    limit: usize,
}

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
struct TestCase {
    name: String,
    init_sqls: Vec<String>,
    #[serde_as(deserialize_as = "OneOrMany<_>")]
    bound_tables: Vec<BoundTable>,
}

#[derive(Debug, Clone, Deserialize)]
struct TestFile {
    test: Vec<TestCase>,
}

async fn validate_case(
    client: &tokio_postgres::Client,
    TestCase {
        name,
        init_sqls,
        bound_tables,
    }: TestCase,
) -> anyhow::Result<()> {
    info!(%name, "validating");

    for sql in init_sqls {
        client.simple_query(&sql).await?;
    }

    let msgs = client.simple_query("SHOW INTERNAL TABLES").await?;
    let internal_tables: HashSet<String> = msgs
        .into_iter()
        .filter_map(|msg| {
            let SimpleQueryMessage::Row(row) = msg else {
                return None;
            };
            Some(row.get("Name").unwrap().to_owned())
        })
        .collect();
    info!(?internal_tables, "found tables");

    #[derive(Debug)]
    struct ProcessedBoundTable {
        interested_tables: Vec<String>,
        limit: usize,
    }

    let tables: Vec<_> = bound_tables
        .into_iter()
        .map(|t| {
            let pattern = Regex::new(&t.pattern).unwrap();
            let interested_tables = internal_tables
                .iter()
                .filter(|t| pattern.is_match(t))
                .cloned()
                .collect::<Vec<_>>();
            ProcessedBoundTable {
                interested_tables,
                limit: t.limit,
            }
        })
        .collect();

    info!(?tables, "start checking");

    const CHECK_COUNT: usize = 100;
    const CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

    for i in 0..CHECK_COUNT {
        for ProcessedBoundTable {
            interested_tables,
            limit,
        } in &tables
        {
            for table in interested_tables {
                let sql = format!("SELECT COUNT(*) FROM {}", table);
                let res = client.query_one(&sql, &[]).await?;
                let cnt: i64 = res.get(0);
                debug!(iter=i, %table, %cnt, "checking");
                if cnt > *limit as i64 {
                    anyhow::bail!(
                        "Table {} has {} rows, which is more than limit {}",
                        table,
                        cnt,
                        limit
                    );
                }
            }
        }

        tokio::time::sleep(CHECK_INTERVAL).await;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::default());

    let opt = TestOptions::parse();

    let conn_builder = tokio_postgres::Config::new()
        .host(&opt.host)
        .port(opt.port)
        .user(&opt.user)
        .password(&opt.pass)
        .connect_timeout(Duration::from_secs(5))
        .clone();

    let (main_client, connection) = conn_builder
        .clone()
        .dbname(&opt.db)
        .connect(NoTls)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to database: {}", e));

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!(?e, "connection error");
        }
    });

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    let manifest = env!("CARGO_MANIFEST_DIR");

    let data_dir = PathBuf::from_str(manifest).unwrap().join("data");

    ReadDirStream::new(fs::read_dir(data_dir).await?)
        .map(|path| async {
            let path = path?.path();
            let content = tokio::fs::read_to_string(&path).await?;
            let test_file: TestFile = toml::from_str(&content)?;
            let cases = test_file.test;

            let test_name = path.file_stem().unwrap().to_string_lossy();

            let cur_db_name = format!("state_cleaning_test_{}_{}", test_name, now);

            main_client
                .simple_query(&format!("CREATE DATABASE {}", cur_db_name))
                .await?;

            let (client, connection) = conn_builder
                .clone()
                .dbname(&cur_db_name)
                .connect(NoTls)
                .await?;

            info!(%test_name, %cur_db_name, "run test in new database");

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!(?e, "connection error");
                }
            });

            for case in cases {
                validate_case(&client, case).await?;
            }

            Ok::<_, anyhow::Error>(())
        })
        .buffer_unordered(16)
        .try_collect::<()>()
        .await?;

    Ok(())
}
