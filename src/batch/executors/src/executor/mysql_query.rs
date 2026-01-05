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
use mysql_async;
use mysql_async::prelude::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::parser::mysql_datum_to_rw_datum;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, BatchExternalSystemError};
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

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
    chunk_size: usize,
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
        let datum = match mysql_datum_to_rw_datum(&mut row, i, name, &rw_field.data_type) {
            Ok(val) => val,
            Err(e) => {
                let e = BatchExternalSystemError(e);
                return Err(e.into());
            }
        };
        datums.push(datum);
    }
    Ok(OwnedRow::new(datums))
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
        chunk_size: usize,
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
            chunk_size,
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

        let mut builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
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

impl BoxedExecutorBuilder for MySqlQueryExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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
            source.context().get_config().developer.chunk_size,
        )))
    }
}
