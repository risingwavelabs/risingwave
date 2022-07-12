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

use std::fmt;

use async_trait::async_trait;
use itertools::{join, Itertools};
use mysql_async::prelude::*;
use mysql_async::*;
use risingwave_common::array::Op::*;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{Datum, Decimal, ScalarImpl};

use crate::sink::{Result, Sink, SinkError};

#[derive(Clone, Debug)]
pub struct MySQLConfig {
    pub endpoint: String,
    pub table: String,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
}

// Primitive design of MySQLSink
#[allow(dead_code)]
pub struct MySQLSink {
    cfg: MySQLConfig,
}

impl MySQLSink {
    pub fn new(cfg: MySQLConfig) -> Result<Self> {
        Ok(Self { cfg })
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

#[async_trait]
impl Sink for MySQLSink {
    async fn write_batch(&mut self, chunk: StreamChunk, schema: &Schema) -> Result<()> {
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

        // Build a connection and start transaction
        let endpoint = self.endpoint();
        let mut endpoint = endpoint.split(':');
        let mut builder = OptsBuilder::default()
            .user(self.user())
            .pass(self.password())
            .ip_or_hostname(endpoint.next().unwrap())
            .db_name(self.database());
        // TODO(nanderstabel): Fix ParseIntError
        if let Some(port) = endpoint.next() {
            builder = builder.tcp_port(port.parse().unwrap());
        }

        let mut conn = Conn::new(builder).await?;
        let mut transaction = conn.start_transaction(TxOpts::default()).await?;

        let mut iter = chunk.ops().iter().enumerate();
        while let Some((idx, op)) = iter.next() {
            // Get SQL statement
            let stmt = match *op {
                Insert => format!(
                    "INSERT INTO {} VALUES ({});",
                    self.table(),
                    join(values(idx)?, ",")
                ),
                Delete => format!(
                    "DELETE FROM {} WHERE ({});",
                    self.table(),
                    join(conditions(values(idx)?), " AND ")
                ),
                UpdateDelete => {
                    if let Some((idx2, UpdateInsert)) = iter.next() {
                        format!(
                            "UPDATE {} SET {} WHERE {};",
                            self.table(),
                            join(conditions(values(idx2)?), ","),
                            join(conditions(values(idx)?), " AND ")
                        )
                    } else {
                        panic!("UpdateDelete should always be followed by an UpdateInsert!")
                    }
                }
                _ => panic!("UpdateInsert should always follow an UpdateDelete!"),
            };
            transaction.exec_drop(stmt, Params::Empty).await?;
        }

        // Commit and drop the connection.
        transaction.commit().await?;
        drop(conn);
        Ok(())
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        todo!()
    }

    async fn commit(&mut self) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::types::chrono_wrapper::*;
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
}
