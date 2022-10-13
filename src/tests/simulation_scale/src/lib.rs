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

#![cfg(madsim)]
#![feature(trait_alias)]
#![feature(lint_reasons)]

use std::time::Duration;

use anyhow::Result;

pub mod cluster;
pub mod ctl_ext;
pub mod nexmark_ext;
pub mod utils;

struct RisingWave {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
}

impl RisingWave {
    async fn connect(host: String, dbname: String) -> Self {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&host)
            .port(4566)
            .dbname(&dbname)
            .user("root")
            .connect_timeout(Duration::from_secs(5))
            .connect(tokio_postgres::NoTls)
            .await
            .expect("Failed to connect to database");
        let task = tokio::spawn(async move {
            connection.await.expect("Postgres connection error");
        });
        RisingWave { client, task }
    }

    async fn run(&mut self, sql: &str) -> Result<String> {
        use std::fmt::Write;

        let mut output = String::new();
        let rows = self.client.simple_query(sql).await?;
        for row in rows {
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        if i != 0 {
                            write!(output, " ").unwrap();
                        }
                        match row.get(i) {
                            Some(v) if v.is_empty() => write!(output, "(empty)").unwrap(),
                            Some(v) => write!(output, "{}", v).unwrap(),
                            None => write!(output, "NULL").unwrap(),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
                _ => unreachable!(),
            }
            writeln!(output).unwrap();
        }
        Ok(output)
    }

    async fn close(self) {
        drop(self.client);
        self.task.await.unwrap();
    }
}
