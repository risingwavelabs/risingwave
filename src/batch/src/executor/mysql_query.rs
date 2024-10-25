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
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use thiserror_ext::AsReport;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// `MySqlQuery` executor. Runs a query against a MySql database.
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
// pub fn mysql_row_to_owned_row(
//     row: tokio_mysql::Row,
//     schema: &Schema,
// ) -> Result<OwnedRow, BatchError> {
//     let mut datums = vec![];
//     for i in 0..schema.fields.len() {
//         let rw_field = &schema.fields[i];
//         let name = rw_field.name.as_str();
//         let datum = mysql_cell_to_scalar_impl(&row, &rw_field.data_type, i, name)?;
//         datums.push(datum);
//     }
//     Ok(OwnedRow::new(datums))
// }

// TODO(kwannoel): Support more types, see mysql connector's ScalarAdapter.
// fn mysql_cell_to_scalar_impl(
//     row: &tokio_mysql::Row,
//     data_type: &DataType,
//     i: usize,
//     name: &str,
// ) -> Result<Datum, BatchError> {
//     let datum = match data_type {
//         DataType::Boolean
//         | DataType::Int16
//         | DataType::Int32
//         | DataType::Int64
//         | DataType::Float32
//         | DataType::Float64
//         | DataType::Date
//         | DataType::Time
//         | DataType::Timestamp
//         | DataType::Timestamptz
//         | DataType::Jsonb
//         | DataType::Interval
//         | DataType::Varchar
//         | DataType::Bytea => {
//             // ScalarAdapter is also fine. But ScalarImpl is more efficient
//             row.try_get::<_, Option<ScalarImpl>>(i)?
//         }
//         DataType::Decimal => {
//             // Decimal is more efficient than PgNumeric in ScalarAdapter
//             let val = row.try_get::<_, Option<Decimal>>(i)?;
//             val.map(ScalarImpl::from)
//         }
//         _ => {
//             tracing::warn!(name, ?data_type, "unsupported data type, set to null");
//             None
//         }
//     };
//     Ok(datum)
// }

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
        // let conn_str = format!(
        //     "host={} port={} user={} password={} dbname={}",
        //     self.host, self.port, self.username, self.password, self.database
        // );
        // let (client, conn) = tokio_mysql::connect(&conn_str, tokio_mysql::NoTls).await?;
        //
        // tokio::spawn(async move {
        //     if let Err(e) = conn.await {
        //         tracing::error!(
        //             "mysql_query_executor: connection error: {:?}",
        //             e.as_report()
        //         );
        //     }
        // });
        //
        // let params: &[&str] = &[];
        // let row_stream = client
        //     .query_raw(&self.query, params)
        //     .await
        //     .context("mysql_query received error from remote server")?;
        // let mut builder = DataChunkBuilder::new(self.schema.data_types(), 1024);
        // tracing::debug!("mysql_query_executor: query executed, start deserializing rows");
        // // deserialize the rows
        // #[for_await]
        // for row in row_stream {
        //     let row = row?;
        //     let owned_row = mysql_row_to_owned_row(row, &self.schema)?;
        //     if let Some(chunk) = builder.append_one_row(owned_row) {
        //         yield chunk;
        //     }
        // }
        // if let Some(chunk) = builder.consume_all() {
        //     yield chunk;
        // }
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
