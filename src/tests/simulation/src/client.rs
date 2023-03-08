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

use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::time::Duration;

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
#[derive(Debug, Clone, Default)]
pub struct SetStmts {
    stmts: HashMap<String, String>,
}

struct SetStmtsIterator<'a, 'b>
where
    'a: 'b,
{
    _stmts: &'a SetStmts,
    stmts_iter: Iter<'b, String, String>,
    remaining_cnt: usize,
}

impl<'a, 'b> SetStmtsIterator<'a, 'b> {
    fn new(stmts: &'a SetStmts) -> Self {
        Self {
            _stmts: stmts,
            stmts_iter: stmts.stmts.iter(),
            remaining_cnt: stmts.stmts.len(),
        }
    }
}

impl SetStmts {
    fn push(&mut self, sql: &str) {
        let sql = sql.trim_start().to_lowercase();
        let mut tokens = sql.split_whitespace();
        assert_eq!(tokens.next().unwrap_or_default(), "set");

        let key = tokens.next().unwrap_or_default().to_string();
        if key.is_empty() {
            return;
        }
        let value = tokens.collect::<Vec<&str>>().join(" ");
        self.stmts.insert(key, value);
    }
}

impl Iterator for SetStmtsIterator<'_, '_> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_cnt == 0 {
            return None;
        }
        self.remaining_cnt -= 1;
        let (k, v) = self.stmts_iter.next().unwrap();
        format!("set {} {}", k, v).into()
    }
}

impl RisingWave {
    pub async fn connect(
        host: String,
        dbname: String,
    ) -> Result<Self, tokio_postgres::error::Error> {
        Self::reconnect(host, dbname, SetStmts::default()).await
    }

    pub async fn reconnect(
        host: String,
        dbname: String,
        set_stmts: SetStmts,
    ) -> Result<Self, tokio_postgres::error::Error> {
        let (client, connection) = tokio_postgres::Config::new()
            .host(&host)
            .port(4566)
            .dbname(&dbname)
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
        client
            .simple_query("SET CREATE_COMPACTION_GROUP_FOR_MV TO true;")
            .await?;
        // FIXME #7188: Temporarily enforce VISIBILITY_MODE=checkpoint to work around the known
        // issue in failure propagation for local mode #7367, which would fail VISIBILITY_MODE=all.
        client
            .simple_query("SET VISIBILITY_MODE TO checkpoint;")
            .await?;
        // replay all SET statements
        for stmt in SetStmtsIterator::new(&set_stmts) {
            client.simple_query(&stmt).await?;
        }
        Ok(RisingWave {
            client,
            task,
            host,
            dbname,
            set_stmts,
        })
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
            *self = Self::reconnect(
                self.host.clone(),
                self.dbname.clone(),
                self.set_stmts.clone(),
            )
            .await?;
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
