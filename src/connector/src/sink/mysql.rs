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

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use itertools::{join, Itertools};
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::Op::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use strum_macros;

use crate::sink::{Result, Sink, SinkError};

pub const MYSQL_SINK: &str = "mysql";

#[derive(Clone, Debug)]
pub struct MySQLConfig {
    pub endpoint: String,
    pub table: String,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl MySQLConfig {
    pub fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        let endpoint = values.get("endpoint").expect("endpoint must be set");
        let table = values.get("table").expect("table must be set");
        let database = values.get("database");
        let user = values.get("user");
        let password = values.get("password");

        Ok(MySQLConfig {
            endpoint: endpoint.to_string(),
            table: table.to_string(),
            database: database.cloned(),
            user: user.cloned(),
            password: password.cloned(),
        })
    }
}

// Primitive design of MySQLSink
#[allow(dead_code)]
#[derive(Debug)]
pub struct MySQLSink {
    cfg: MySQLConfig,

    conn: Conn,
    chunk_cache: Vec<(StreamChunk, Schema)>,
}

impl MySQLSink {
    pub async fn new(cfg: MySQLConfig) -> Result<Self> {
        // Build a connection and start transaction
        let conn = Conn::new(get_builder(&cfg)).await?;
        Ok(Self {
            cfg,
            conn,
            chunk_cache: vec![],
        })
    }

    pub async fn prepare(&mut self, schema: &Schema) -> Result<()> {
        // Create a table
        let create_table = format!(
            r"CREATE TABLE IF NOT EXISTS `{}`.`{}` ( {} );",
            self.cfg.database.clone().unwrap(),
            self.cfg.table,
            join(
                schema
                    .names()
                    .iter()
                    .zip_eq(schema.data_types().iter())
                    .map(|(n, dt)| format!("`{}` {}", n, MySQLDataType::from(dt)))
                    .collect_vec(),
                ", ",
            )
        );
        self.conn.query_drop(create_table).await?;
        Ok(())
    }

    fn endpoint(&self) -> String {
        self.cfg.endpoint.clone()
    }

    fn table(&self) -> String {
        self.cfg.table.clone()
    }

    fn database(&self) -> Option<String> {
        self.cfg.database.clone()
    }

    fn user(&self) -> Option<String> {
        self.cfg.user.clone()
    }

    fn password(&self) -> Option<String> {
        self.cfg.password.clone()
    }
}

fn get_builder(cfg: &MySQLConfig) -> OptsBuilder {
    let endpoint = cfg.endpoint.clone();
    let mut endpoint = endpoint.split(':');
    let mut builder = OptsBuilder::default()
        .user(cfg.user.clone())
        .pass(cfg.password.clone())
        .ip_or_hostname(endpoint.next().unwrap())
        .db_name(cfg.database.clone());
    // TODO(nanderstabel): Fix ParseIntError
    if let Some(port) = endpoint.next() {
        builder = builder.tcp_port(port.parse().unwrap());
    }
    builder
}

#[derive(Debug)]
struct MySQLValue(Value);

impl fmt::Display for MySQLValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0.as_sql(true))
    }
}

impl TryFrom<Datum> for MySQLValue {
    type Error = SinkError;

    fn try_from(datum: Datum) -> Result<MySQLValue> {
        if let Some(scalar) = datum {
            match scalar {
                ScalarImpl::Int16(v) => Ok(MySQLValue(v.into())),
                ScalarImpl::Int32(v) => Ok(MySQLValue(v.into())),
                ScalarImpl::Int64(v) => Ok(MySQLValue(v.into())),
                ScalarImpl::Float32(v) => Ok(MySQLValue(f32::from(v).into())),
                ScalarImpl::Float64(v) => Ok(MySQLValue(f64::from(v).into())),
                ScalarImpl::Bool(v) => Ok(MySQLValue(v.into())),
                ScalarImpl::Decimal(Decimal::Normalized(v)) => Ok(MySQLValue(v.into())),
                ScalarImpl::Decimal(_) => panic!("NaN, -inf, +inf are not supported by MySQL"),
                ScalarImpl::Utf8(v) => Ok(MySQLValue(v.into())),
                ScalarImpl::NaiveDate(v) => Ok(MySQLValue(format!("{}", v).into())),
                ScalarImpl::NaiveTime(v) => Ok(MySQLValue(format!("{}", v).into())),
                ScalarImpl::NaiveDateTime(v) => Ok(MySQLValue(format!("{}", v).into())),
                // ScalarImpl::Interval(v) => Ok(MySQLValue(Value::NULL)),
                _ => unimplemented!(),
            }
        } else {
            Ok(MySQLValue(Value::NULL))
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(strum_macros::Display)]
enum MySQLDataType {
    BOOL,
    SMALLINT,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    // TODO(nanderstabel): find solution for varchar length.
    #[strum(serialize = "VARCHAR(255)")]
    VARCHAR,
}

impl From<&DataType> for MySQLDataType {
    fn from(data_type: &DataType) -> MySQLDataType {
        match data_type {
            DataType::Boolean => MySQLDataType::BOOL,
            DataType::Int16 => MySQLDataType::SMALLINT,
            DataType::Int32 => MySQLDataType::INT,
            DataType::Int64 => MySQLDataType::BIGINT,
            DataType::Float32 => MySQLDataType::FLOAT,
            DataType::Float64 => MySQLDataType::DOUBLE,
            DataType::Varchar => MySQLDataType::VARCHAR,
            _ => unimplemented!(),
        }
    }
}

#[async_trait]
impl Sink for MySQLSink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
        self.chunk_cache.push((chunk, schema.clone()));
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let mut txn = self.conn.start_transaction(TxOpts::default()).await?;
        for (chunk, schema) in &self.chunk_cache {
            write_to_mysql(&mut txn, chunk, schema, &self.cfg).await?;
        }
        txn.commit().await?;

        self.chunk_cache.clear();
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

async fn write_to_mysql<'a>(
    txn: &mut Transaction<'a>,
    chunk: &StreamChunk,
    schema: &Schema,
    config: &MySQLConfig,
) -> Result<()> {
    // Closure that takes an idx to create a vector of MySQLValues from a StreamChunk 'row'.
    let values = |idx| -> Result<Vec<MySQLValue>> {
        chunk
            .columns()
            .iter()
            .map(|x| MySQLValue::try_from(x.array_ref().datum_at(idx)))
            .collect_vec()
            .into_iter()
            .collect()
    };

    // Closure that builds a String containing WHERE conditions, i.e. 'v1=1 AND v2=2'.
    // Perhaps better to replace this functionality with a new Sink trait method.
    let conditions = |values: Vec<MySQLValue>| {
        schema
            .names()
            .iter()
            .zip_eq(values.iter())
            .map(|(c, v)| format!("{}={}", c, v))
            .collect::<Vec<String>>()
    };

    let mut iter = chunk.ops().iter().enumerate();
    while let Some((idx, op)) = iter.next() {
        // Get SQL statement
        let stmt = match *op {
            Insert => format!(
                "INSERT INTO {} VALUES ({});",
                &config.table,
                join(values(idx)?, ",")
            ),
            Delete => format!(
                "DELETE FROM {} WHERE ({});",
                &config.table,
                join(conditions(values(idx)?), " AND ")
            ),
            UpdateDelete => {
                if let Some((idx2, UpdateInsert)) = iter.next() {
                    format!(
                        "UPDATE {} SET {} WHERE {};",
                        &config.table,
                        join(conditions(values(idx2)?), ","),
                        join(conditions(values(idx)?), " AND ")
                    )
                } else {
                    return Err(SinkError::MySQL(
                        "UpdateDelete should always be followed by an UpdateInsert!".into(),
                    ));
                }
            }
            _ => return Err(SinkError::MySQL("Unsupported operation".into())),
        };
        // TODO by doc, exec_drop will simply exec query and drop the result, we may check and retry
        // for jitter or other reasons
        txn.exec_drop(stmt, Params::Empty).await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use risingwave_common::array;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, Op, Utf8Array};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::chrono_wrapper::*;
    use risingwave_common::types::DataType;
    use rust_decimal::Decimal as RustDecimal;

    use super::*;

    struct ConnectionParams<'a> {
        pub endpoint: &'a str,
        pub table: &'a str,
        pub database: &'a str,
        pub user: &'a str,
        pub password: &'a str,
    }

    #[test]
    fn test_date() {
        assert_eq!(
            MySQLValue::try_from(Some(ScalarImpl::NaiveDate(NaiveDateWrapper::default())))
                .unwrap()
                .to_string(),
            "'1970-01-01'"
        );
    }

    #[test]
    fn test_time() {
        assert_eq!(
            MySQLValue::try_from(Some(ScalarImpl::NaiveTime(NaiveTimeWrapper::default())))
                .unwrap()
                .to_string(),
            "'00:00:00'"
        );
    }

    #[test]
    fn test_datetime() {
        assert_eq!(
            MySQLValue::try_from(Some(ScalarImpl::NaiveDateTime(
                NaiveDateTimeWrapper::default()
            )))
            .unwrap()
            .to_string(),
            "'1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn test_decimal() {
        assert_eq!(
            MySQLValue::try_from(Some(ScalarImpl::Decimal(Decimal::Normalized(
                RustDecimal::new(0, 0)
            ))))
            .unwrap()
            .to_string(),
            "'0'"
        );
        assert_eq!(
            MySQLValue::try_from(Some(ScalarImpl::Decimal(Decimal::Normalized(
                RustDecimal::new(124, 5)
            ))))
            .unwrap()
            .to_string(),
            "'0.00124'"
        );
    }

    impl Default for MySQLConfig {
        fn default() -> Self {
            MySQLConfig {
                endpoint: "127.0.0.1:3306".to_string(),
                table: "t".to_string(),
                database: Some("test".into()),
                user: Some("root".into()),
                password: None,
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn test_create_table() -> Result<()> {
        let cfg = MySQLConfig::default();
        let mut sink = MySQLSink::new(cfg.clone()).await?;

        let schema = Schema::new(vec![Field {
            data_type: DataType::Int32,
            name: "v1".into(),
            sub_fields: vec![],
            type_name: "".into(),
        }]);

        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![Column::new(Arc::new(ArrayImpl::from(array!(
                I32Array,
                [Some(1), Some(2)]
            ))))],
            None,
        );

        sink.prepare(&schema).await?;
        sink.begin_epoch(1000).await?;
        sink.write_batch(chunk, &schema).await?;
        sink.commit().await?;

        #[derive(Debug, PartialEq, Eq, Clone)]
        struct Res(i32);

        let builder = get_builder(&cfg);

        let mut conn = Conn::new(builder).await?;
        let res = "SELECT v1 FROM `test`.`t`"
            .with(())
            .map(&mut conn, Res)
            .await?;

        assert_eq!(res, vec![Res(1), Res(2)]);

        "DROP TABLE `test`.`t`".ignore(conn).await?;

        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn test_drop() -> Result<()> {
        let cfg = MySQLConfig::default();
        let mut sink = MySQLSink::new(cfg.clone()).await?;

        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "v1".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
            Field {
                data_type: DataType::Varchar,
                name: "v2".into(),
                sub_fields: vec![],
                type_name: "".into(),
            },
        ]);

        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                Column::new(Arc::new(ArrayImpl::from(array!(
                    I32Array,
                    [Some(1), Some(2), Some(3)]
                )))),
                Column::new(Arc::new(ArrayImpl::from(array!(
                    Utf8Array,
                    [Some("1"), Some("2"), Some("; drop database")]
                )))),
            ],
            None,
        );

        sink.prepare(&schema).await?;
        sink.begin_epoch(1000).await?;
        sink.write_batch(chunk, &schema).await?;
        sink.commit().await?;

        let builder = get_builder(&cfg);

        let conn = Conn::new(builder).await?;
        "DROP TABLE `test`.`t`".ignore(conn).await?;

        Ok(())
    }
}
