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

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::catalog::{Field, Schema};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use tokio_postgres;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// S3 file scan executor. Currently only support parquet file format.
pub struct PostgresQueryExecutor {
    schema: Schema,
    host: String,
    port: String,
    username: String,
    password: String,
    database: String,
    query: String,
    identity: String,
}

impl Executor for PostgresQueryExecutor {
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

impl PostgresQueryExecutor {
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
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.username, self.password, self.database
        );
        let (client, _conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        // TODO(kwannoel): Use pagination using CURSOR.
        let rows = client.query(&self.query, &[]).await?;
    }
}

pub struct PostgresQueryExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for PostgresQueryExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let postgres_query_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::PostgresQuery
        )?;

        Ok(Box::new(PostgresQueryExecutor::new(
            Schema::from_iter(postgres_query_node.columns.iter().map(Field::from)),
            postgres_query_node.hostname.clone(),
            postgres_query_node.port.clone(),
            postgres_query_node.username.clone(),
            postgres_query_node.password.clone(),
            postgres_query_node.database.clone(),
            postgres_query_node.query.clone(),
            source.plan_node().get_identity().clone(),
        )))
    }
}
