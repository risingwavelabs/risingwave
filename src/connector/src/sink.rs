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

use std::fmt;

use async_trait::async_trait;
use itertools::{join, Itertools};
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::Op::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::decimal::Decimal;
use risingwave_common::types::{
    DataType, NaiveDateWrapper, NaiveTimeWrapper, OrderedF32,
    OrderedF64, ScalarImpl, IntervalUnit
};

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

// TODO(nanderstabel): Add DATETIME and TIMESTAMP
pub enum MySQLType {
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(OrderedF32),
    Double(OrderedF64),
    Bool(bool),
    Decimal(Decimal),
    Varchar(String),
    Date(NaiveDateWrapper),
    Time(NaiveTimeWrapper),
    Interval(IntervalUnit)
}

impl TryFrom<ScalarImpl> for MySQLType {
    type Error = Error;

    fn try_from(s: ScalarImpl) -> Result<Self> {
        match s {
            ScalarImpl::Int16(v) => Ok(MySQLType::SmallInt(v)),
            ScalarImpl::Int32(v) => Ok(MySQLType::Int(v)),
            ScalarImpl::Int64(v) => Ok(MySQLType::BigInt(v)),
            ScalarImpl::Float32(v) => Ok(MySQLType::Float(v)),
            ScalarImpl::Float64(v) => Ok(MySQLType::Double(v)),
            ScalarImpl::Bool(v) => Ok(MySQLType::Bool(v)),
            ScalarImpl::Decimal(v) => Ok(MySQLType::Decimal(v)),
            ScalarImpl::Utf8(v) => Ok(MySQLType::Varchar(v)),
            ScalarImpl::NaiveDate(v) => Ok(MySQLType::Date(v)),
            ScalarImpl::NaiveTime(v) => Ok(MySQLType::Time(v)),
            ScalarImpl::Interval(v) => Ok(MySQLType::Interval(v)),
            _ => unimplemented!(),
        }
    }
}

impl fmt::Display for MySQLType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MySQLType::SmallInt(v) => write!(f, "{}", v)?,
            MySQLType::Int(v) => write!(f, "{}", v)?,
            MySQLType::BigInt(v) => write!(f, "{}", v)?,
            MySQLType::Float(v) => write!(f, "{}", v)?,
            MySQLType::Double(v) => write!(f, "{}", v)?,
            MySQLType::Bool(v) => write!(f, "{}", v)?,
            MySQLType::Decimal(v) => write!(f, "{}", v)?,
            MySQLType::Varchar(v) => write!(f, "{}", v)?,
            MySQLType::Date(_v) => todo!(),
            MySQLType::Time(_v) => todo!(),
            MySQLType::Interval(_v) => todo!(),
        }
        Ok(())
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
            // TODO(nanderstabel): Refactor
            let values = chunk
                .columns()
                .iter()
                .map(|x| MySQLType::try_from(x.array_ref().datum_at(idx).unwrap()).unwrap())
                .collect_vec();

            // Get SQL statement
            let stmt = match *op {
                Insert | UpdateInsert => format!(
                    "INSERT INTO {} VALUES ({})",
                    self.table(),
                    join(values, ",")
                ),
                Delete | UpdateDelete => format!(
                    "DELETE FROM {} WHERE ({})",
                    self.table(),
                    join(
                        schema
                            .names()
                            .iter()
                            .zip(values.iter())
                            .map(|(c, v)| format!("{}={}", c, v))
                            .collect::<Vec<String>>(),
                        " AND "
                    )
                ),
            };
            transaction.exec_drop(stmt, Params::Empty).await?;
        }

        // Commit and drop the connection.
        transaction.commit().await?;
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

#[cfg(test)]
mod test {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;

    // use futures::future::BoxFuture;
    use super::*;

    struct ConnectionParams<'a> {
        pub endpoint: &'a str,
        pub table: &'a str,
        pub database: &'a str,
        pub user: &'a str,
        pub password: &'a str,
    }

    async fn start_connection(params: &ConnectionParams<'_>) -> Result<Conn> {
        let builder = OptsBuilder::default()
            .user(Some(params.user))
            .pass(Some(params.password))
            .ip_or_hostname(params.endpoint)
            .db_name(Some(params.database));
        let conn = Conn::new(builder).await?;
        Ok(conn)
    }

    #[tokio::test]
    async fn test_basic_async() -> Result<()> {
        // Connection parameters for testing purposes.
        let params = ConnectionParams {
            endpoint: "127.0.0.1",
            table: "t",
            database: "db1",
            user: "nander",
            password: "123",
        };

        // Initialize a sink using connection parameters.
        let mut sink = MySQLSink::new(
            params.endpoint.into(),
            params.table.into(),
            Some(params.database.into()),
            Some(params.user.into()),
            Some(params.password.into()),
        );

        // Initialize streamchunk.
        let chunk = StreamChunk::from_pretty(
            "  i  i
            + 55 11
            + 44 22
            + 33 00
            - 55 11",
        );

        // Initialize schema of two Int32 columns.
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int32, "v1"),
            Field::with_name(DataType::Int32, "v2"),
        ]);

        // TODO(nanderstabel): currently writing chunk, not batch..
        sink.write_batch(chunk, schema).await?;

        // Start a new connection using the same connection parameters and get the SELECT result.
        let mut conn = start_connection(&params).await?;
        let select: Vec<(i32, i32)> = conn
            .query(format!("SELECT * FROM {};", params.table))
            .await?;

        assert_eq!(select, [(44, 22), (33, 00),]);

        // Clean up the table and drop the connection.
        conn.query_drop("DELETE FROM t;").await?;
        drop(conn);

        Ok(())
    }
}
