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
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::Row;
use itertools::Itertools;


#[async_trait]
pub trait Sink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()>;
}

// Primitive design of MySQLSink
#[allow(dead_code)]
pub struct MySQLSink {
    pub endpoint: String,
    pub table: String,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl MySQLSink {
    fn new(endpoint: String, table: String, database: Option<String>, user: Option<String>, password: Option<String>) -> Self {
        Self {
            endpoint,
            table,
            database,
            user,
            password
        }
    }
}

#[async_trait]
impl Sink for MySQLSink {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        
        // TODO(nanderstabel): fix, currently defaults to port 3306
        let builder = OptsBuilder::default()
            .user(self.user.clone())
            .pass(self.password.clone())
            .ip_or_hostname(self.endpoint.clone())
            .db_name(self.database.clone());
        
        let mut conn = Conn::new(builder).await?;

        for (idx, op) in chunk.ops().iter().enumerate() {
            let row = Row(chunk
                .columns()
                .iter()
                .map(|x| x.array_ref().datum_at(idx))
                .collect_vec());

            let result: Vec<i32> = conn.query(format!("INSERT INTO {} VALUES ({})", self.table, row.0[0].clone().unwrap().as_int32())).await?;
        }

        
        
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

}

pub struct RedisSink;

#[async_trait]
impl Sink for RedisSink {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        todo!();
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Int {
    i: i32,
}

#[cfg(test)]
mod test {
    use super::*;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;

    // use anyhow::Error;
    
    #[tokio::test]
    async fn test_basic_async() -> Result<()> {
    
        // // Create a temporary table
        // r"CREATE TEMPORARY TABLE payment (
        //     customer_id int not null,
        //     amount int not null,
        //     account_name text
        // )".ignore(&mut conn).await?;


        let mut sink = MySQLSink::new(
            "127.0.0.1".into(),
            "t".into(),
            Some("db1".into()),
            Some("nander".into()),
            Some("123".into())
        );

        let chunk = StreamChunk::from_pretty(
            " i
            + 1
            + 2
            + 3",
        );

        sink.write_batch(chunk).await?;

    //     // Load ints from the database. Type inference will work here.
    //     let loaded_ints = "SELECT v1 FROM t"
    //         .with(())
    //         .map(&mut conn, |i| Int { i })
    //         .await?;

    //     // Delete ints
    //    let result: Vec<i32> = conn.query("DELETE FROM t").await?;
    
        // the async fn returns Result, so
        Ok(())

    }
}