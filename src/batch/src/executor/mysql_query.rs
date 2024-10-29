// Copyright 2024 RisingWave Labs
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

use anyhow::Context;
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use mysql_async::prelude::*;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use rust_decimal::Decimal as RustDecimal;
use {chrono, mysql_async};

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// `MySqlQuery` executor. Runs a query against a `MySql` database.
pub struct MySqlQueryExecutor {
    schema: Schema,
    host: String,
    port: String,
    username: String,
    password: String,
    database: String,
    query: String,
    identity: String,
}

impl Executor for MySqlQueryExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}
pub fn mysql_row_to_owned_row(
    mut row: mysql_async::Row,
    schema: &Schema,
) -> Result<OwnedRow, BatchError> {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = mysql_cell_to_scalar_impl(&mut row, &rw_field.data_type, i, name)?;
        datums.push(datum);
    }
    Ok(OwnedRow::new(datums))
}

macro_rules! mysql_value_to_scalar {
    ($row:ident, $i:ident, $name:ident, $ty:ty, $variant:ident) => {{
        let val = $row.take_opt::<Option<$ty>, _>($i);
        match val {
            None => bail!("missing value for column {}, at index {}", $name, $i),
            Some(Ok(Some(val))) => Some(ScalarImpl::$variant(val.into())),
            Some(Ok(None)) => None,
            Some(Err(e)) => return Err(e.into()),
        }
    }};
}

fn mysql_cell_to_scalar_impl(
    row: &mut mysql_async::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> Result<Datum, BatchError> {
    let datum = match data_type {
        DataType::Boolean => mysql_value_to_scalar!(row, i, name, bool, Bool),
        DataType::Int16 => mysql_value_to_scalar!(row, i, name, i16, Int16),
        DataType::Int32 => mysql_value_to_scalar!(row, i, name, i32, Int32),
        DataType::Int64 => mysql_value_to_scalar!(row, i, name, i64, Int64),
        DataType::Float32 => mysql_value_to_scalar!(row, i, name, f32, Float32),
        DataType::Float64 => mysql_value_to_scalar!(row, i, name, f64, Float64),
        DataType::Decimal => mysql_value_to_scalar!(row, i, name, RustDecimal, Decimal),
        DataType::Date => mysql_value_to_scalar!(row, i, name, chrono::NaiveDate, Date),
        DataType::Time => mysql_value_to_scalar!(row, i, name, chrono::NaiveTime, Time),
        DataType::Timestamp => {
            mysql_value_to_scalar!(row, i, name, chrono::NaiveDateTime, Timestamp)
        }
        DataType::Varchar => mysql_value_to_scalar!(row, i, name, String, Utf8),
        _ => {
            bail!("unsupported data type: {}", data_type)
        }
    };
    Ok(datum)
}

impl MySqlQueryExecutor {
    pub fn new(
        schema: Schema,
        host: String,
        port: String,
        username: String,
        password: String,
        database: String,
        query: String,
        identity: String,
    ) -> Self {
        Self {
            schema,
            host,
            port,
            username,
            password,
            database,
            query,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        tracing::debug!("mysql_query_executor: started");
        let database_opts: mysql_async::Opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(self.host)
            .tcp_port(self.port.parse::<u16>().unwrap()) // FIXME
            .user(Some(self.username))
            .pass(Some(self.password))
            .db_name(Some(self.database))
            .into();

        let pool = mysql_async::Pool::new(database_opts);
        let mut conn = pool
            .get_conn()
            .await
            .context("failed to connect to mysql in batch executor")?;

        let query = self.query;
        let mut query_iter = conn
            .query_iter(query)
            .await
            .context("failed to execute my_sql_query in batch executor")?;
        let Some(row_stream) = query_iter.stream::<mysql_async::Row>().await? else {
            bail!("failed to get row stream from mysql query")
        };

        let mut builder = DataChunkBuilder::new(self.schema.data_types(), 1024);
        tracing::debug!("mysql_query_executor: query executed, start deserializing rows");
        // deserialize the rows
        #[for_await]
        for row in row_stream {
            let row = row?;
            let owned_row = mysql_row_to_owned_row(row, &self.schema)?;
            if let Some(chunk) = builder.append_one_row(owned_row) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
        return Ok(());
    }
}

pub struct MySqlQueryExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for MySqlQueryExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let mysql_query_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::MysqlQuery
        )?;

        Ok(Box::new(MySqlQueryExecutor::new(
            Schema::from_iter(mysql_query_node.columns.iter().map(Field::from)),
            mysql_query_node.hostname.clone(),
            mysql_query_node.port.clone(),
            mysql_query_node.username.clone(),
            mysql_query_node.password.clone(),
            mysql_query_node.database.clone(),
            mysql_query_node.query.clone(),
            source.plan_node().get_identity().clone(),
        )))
    }
}
