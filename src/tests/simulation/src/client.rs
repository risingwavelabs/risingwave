// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use lru::{Iter, LruCache};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;

/// A RisingWave client.
pub struct RisingWave {
    client: tokio_postgres::Client,
    task: tokio::task::JoinHandle<()>,
    host: String,
    dbname: String,
    /// The `SET` statements that have been executed on this client.
    /// We need to replay them when reconnecting.
    set_stmts: SetStmts,
}

/// `SetStmts` stores and compacts all `SET` statements that have been executed in the client
/// history.
pub struct SetStmts {
    stmts_cache: LruCache<String, String>,
}

impl Default for SetStmts {
    fn default() -> Self {
        Self {
            stmts_cache: LruCache::unbounded(),
        }
    }
}

struct SetStmtsIterator<'a, 'b>
where
    'a: 'b,
{
    _stmts: &'a SetStmts,
    stmts_iter: core::iter::Rev<Iter<'b, String, String>>,
}

impl<'a, 'b> SetStmtsIterator<'a, 'b> {
    fn new(stmts: &'a SetStmts) -> Self {
        Self {
            _stmts: stmts,
            stmts_iter: stmts.stmts_cache.iter().rev(),
        }
    }
}

impl SetStmts {
    fn push(&mut self, sql: &str) {
        let ast = Parser::parse_sql(&sql).expect("a set statement should be parsed successfully");
        match ast
            .into_iter()
            .exactly_one()
            .expect("should contain only one statement")
        {
            // record `local` for variable and `SetTransaction` if supported in the future.
            Statement::SetVariable {
                local: _,
                variable,
                value: _,
            } => {
                let key = variable.real_value().to_lowercase();
                // store complete sql as value.
                self.stmts_cache.put(key, sql.to_string());
            }
            _ => unreachable!(),
        }
    }
}

impl Iterator for SetStmtsIterator<'_, '_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        let (_, stmt) = self.stmts_iter.next()?;
        Some(stmt.clone())
    }
}

impl RisingWave {
    pub async fn connect(
        host: String,
        dbname: String,
    ) -> Result<Self, tokio_postgres::error::Error> {
        let set_stmts = SetStmts::default();
        let (client, task) = Self::connect_inner(&host, &dbname, &set_stmts).await?;
        Ok(Self {
            client,
            task,
            host,
            dbname,
            set_stmts,
        })
    }

    pub async fn connect_inner(
        host: &str,
        dbname: &str,
        set_stmts: &SetStmts,
    ) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), tokio_postgres::error::Error>
    {
        let (client, connection) = tokio_postgres::Config::new()
            .host(host)
            .port(4566)
            .dbname(dbname)
            .user("root")
            .connect_timeout(Duration::from_secs(5))
            .connect(tokio_postgres::NoTls)
            .await?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("postgres connection error: {e}");
            }
        });
        // for recovery
        client
            .simple_query("SET RW_IMPLICIT_FLUSH TO true;")
            .await?;
        // replay all SET statements
        for stmt in SetStmtsIterator::new(&set_stmts) {
            client.simple_query(&stmt).await?;
        }
        Ok((client, task))
    }

    pub async fn reconnect(&mut self) -> Result<(), tokio_postgres::error::Error> {
        let (client, task) = Self::connect_inner(&self.host, &self.dbname, &self.set_stmts).await?;
        self.client = client;
        self.task = task;
        Ok(())
    }

    /// Returns a reference of the inner Postgres client.
    pub fn pg_client(&self) -> &tokio_postgres::Client {
        &self.client
    }
}

impl Drop for RisingWave {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for RisingWave {
    type Error = tokio_postgres::error::Error;

    async fn run(&mut self, sql: &str) -> Result<sqllogictest::DBOutput, Self::Error> {
        use sqllogictest::{ColumnType, DBOutput};

        if self.client.is_closed() {
            // connection error, reset the client
            self.reconnect().await?;
        }

        if sql.trim_start().to_lowercase().starts_with("set") {
            self.set_stmts.push(sql);
        }

        let mut output = vec![];

        let rows = self.client.simple_query(sql).await?;
        let mut cnt = 0;
        for row in rows {
            let mut row_vec = vec![];
            match row {
                tokio_postgres::SimpleQueryMessage::Row(row) => {
                    for i in 0..row.len() {
                        match row.get(i) {
                            Some(v) => {
                                if v.is_empty() {
                                    row_vec.push("(empty)".to_string());
                                } else {
                                    row_vec.push(v.to_string());
                                }
                            }
                            None => row_vec.push("NULL".to_string()),
                        }
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(cnt_) => {
                    cnt = cnt_;
                    break;
                }
                _ => unreachable!(),
            }
            output.push(row_vec);
        }

        if output.is_empty() {
            Ok(DBOutput::StatementComplete(cnt))
        } else {
            Ok(DBOutput::Rows {
                types: vec![ColumnType::Any; output[0].len()],
                rows: output,
            })
        }
    }

    fn engine_name(&self) -> &str {
        "risingwave"
    }

    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await
    }
}
