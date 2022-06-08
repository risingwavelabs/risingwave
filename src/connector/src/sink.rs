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

// use risingwave_common::error::{Result, RwError};
// use mysql::*;
// use mysql::prelude::*;

use async_trait::async_trait;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::Op::*;
use risingwave_common::array::{Row, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: Schema) -> Result<()>;

    fn endpoint(&self) -> String;
    fn table(&self) -> String;
    fn database(&self) -> Option<String>;
    fn user(&self) -> Option<String>;
    fn password(&self) -> Option<String>; // TODO(nanderstabel): auth?
}

// Primitive design of MySQLSink
#[allow(dead_code)]
pub struct MySQLSink {
    endpoint: String,
    table: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl MySQLSink {
    fn new(
        endpoint: String,
        table: String,
        database: Option<String>,
        user: Option<String>,
        password: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            table,
            database,
            user,
            password,
        }
    }
}

#[async_trait]
impl Sink for MySQLSink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: Schema) -> Result<()> {
        // TODO(nanderstabel): fix, currently defaults to port 3306
        let builder = OptsBuilder::default()
            .user(self.user())
            .pass(self.password())
            .ip_or_hostname(self.endpoint())
            .db_name(self.database());

        let mut conn = Conn::new(builder).await?;

        let mut transaction = conn.start_transaction(TxOpts::default()).await?;

        for (idx, op) in chunk.ops().iter().enumerate() {
            let row = Row(chunk
                .columns()
                .iter()
                .map(|x| x.array_ref().datum_at(idx))
                .collect_vec());

            match *op {
                Insert | UpdateInsert => {
                    transaction
                        .exec_drop(
                            format!(
                                "INSERT INTO {} VALUES ({})",
                                self.table(),
                                row.0[0].clone().unwrap().as_int32()
                            ),
                            Params::Empty,
                        )
                        .await?
                }
                Delete | UpdateDelete => (),
            }
        }

        transaction.commit().await?;

        // // Save ints
        // r"INSERT INTO t
        // VALUES (:i)"
        //     .with(chunk.ops().iter().map(|o| params! {
        //         "Insert" => i.i,
        //     }))
        //     .batch(&mut conn)
        //     .await?;

        // Dropped connection will go to the pool
        drop(conn);
        Ok(())
    }

    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    fn table(&self) -> String {
        self.table.clone()
    }

    fn database(&self) -> Option<String> {
        self.database.clone()
    }

    fn user(&self) -> Option<String> {
        self.user.clone()
    }

    fn password(&self) -> Option<String> {
        self.password.clone()
    }
}

pub struct RedisSink;

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, _chunk: StreamChunk, _schema: Schema) -> Result<()> {
        todo!();
    }

    fn endpoint(&self) -> String {
        todo!();
    }

    fn table(&self) -> String {
        todo!();
    }

    fn database(&self) -> Option<String> {
        todo!();
    }

    fn user(&self) -> Option<String> {
        todo!();
    }

    fn password(&self) -> Option<String> {
        todo!();
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Int {
    i: i32,
}

#[cfg(test)]
mod test {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;

    use super::*;

    #[tokio::test]
    async fn test_basic_async() -> Result<()> {
        let mut sink = MySQLSink::new(
            "127.0.0.1".into(),
            "t".into(),
            Some("db1".into()),
            Some("nander".into()),
            Some("123".into()),
        );

        let chunk = StreamChunk::from_pretty(
            " i
            + 55
            + 44
            + 33",
        );

        let schema = Schema::new(vec![Field {
            data_type: DataType::Int32,
            name: "v1".to_string(),
            sub_fields: vec![],
            type_name: "test".to_string(),
        }]);

        sink.write_batch(chunk, schema).await?;

        let builder = OptsBuilder::default()
            .user(sink.user())
            .pass(sink.password())
            .ip_or_hostname(sink.endpoint())
            .db_name(sink.database());

        let mut conn = Conn::new(builder).await?;
        let select: Vec<i32> = conn
            .query(format!("SELECT * FROM {};", sink.table()))
            .await?;

        println!("{:?}", select);

        conn.query_drop("DELETE FROM t;").await?;
        drop(conn);

        Ok(())
    }
}
