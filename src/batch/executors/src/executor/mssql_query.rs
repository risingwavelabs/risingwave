// Copyright 2026 RisingWave Labs
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
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::connector_common::sql_server::{
    MssqlConnectionConfig, create_mssql_client,
};
use risingwave_connector::parser::sql_server_row_to_owned_row;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// `MssqlQuery` executor. Runs a query against a SQL Server database via `tiberius`.
pub struct MssqlQueryExecutor {
    schema: Schema,
    config: MssqlConnectionConfig,
    query: String,
    identity: String,
    chunk_size: usize,
}

impl Executor for MssqlQueryExecutor {
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

impl MssqlQueryExecutor {
    pub fn new(
        schema: Schema,
        config: MssqlConnectionConfig,
        query: String,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            schema,
            config,
            query,
            identity,
            chunk_size,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        tracing::debug!("mssql_query_executor: started");

        let mut client = create_mssql_client(&self.config)
            .await
            .context("mssql_query: failed to connect to sql server")?;

        tracing::debug!(
            query = %self.query,
            "mssql_query_executor: running query"
        );

        // Run the user-provided query verbatim (no parameter binding).
        // `Client::query` returns a `QueryStream`; `into_row_stream` yields one
        // `tiberius::Row` per item. The row type is inferred — we never name
        // `tiberius::Row` directly so `tiberius` doesn't need to be a direct
        // dep of this crate.
        let row_stream = client
            .query(self.query.as_str(), &[])
            .await
            .context("mssql_query received error from remote server")?
            .into_row_stream();

        let mut builder = DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);
        tracing::debug!("mssql_query_executor: query executed, start deserializing rows");

        futures::pin_mut!(row_stream);

        // Deserialize rows. All per-cell decoding is inlined into the loop
        // body so we don't need any helper that names `tiberius::Row`.
        //
        // Decoding strategy: route through [`ScalarImplTiberiusWrapper`], the
        // canonical RisingWave-side `tiberius::FromSql` impl in the connector
        // crate. It handles: `Decimal` ← `Numeric`, `Timestamptz` ←
        // `DateTimeOffset`, `Varchar` ← `UniqueIdentifier` (uppercased) /
        // `Xml`, etc.
        //
        // Cells whose target [`DataType`] has no direct tiberius mapping (e.g.
        // `Jsonb`, `Interval`) are returned as NULL with a warning. Cells
        // whose underlying type mismatches the requested RisingWave type
        // (NULL on non-nullable, wire type that can't be coerced) also fall
        // through to NULL — failing the whole query for one bad cell is too
        // aggressive for an ad-hoc table-valued function.
        #[for_await]
        for row_result in row_stream {
            let row = row_result.context("mssql_query: row decode error")?;
            // Delegate per-cell decoding to the canonical helper in the connector crate.
            // It already handles types without a direct tiberius mapping (Jsonb, Interval, …)
            // by emitting NULL with a logged warning, and it owns the
            // `ScalarImplTiberiusWrapper` glue which is private to the connector crate.
            let mut row_mut = row;
            let owned_row = sql_server_row_to_owned_row(&mut row_mut, &self.schema);
            if let Some(chunk) = builder.append_one_row(owned_row) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
    }
}

pub struct MssqlQueryExecutorBuilder {}

impl BoxedExecutorBuilder for MssqlQueryExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let mssql_query_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::MssqlQuery
        )?;

        let port = mssql_query_node
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid sql server port `{}`", mssql_query_node.port))?;

        // Default `encrypt=false`, `trust_cert=true` matches local development
        // defaults (sqlcmd `-C` flag). `trust_cert=true` forces TLS via
        // `EncryptionLevel::Required` inside `create_mssql_client`.
        let encrypt = mssql_query_node.encrypt.parse::<bool>().unwrap_or(false);
        let trust_cert = mssql_query_node.trust_cert.parse::<bool>().unwrap_or(true);

        Ok(Box::new(MssqlQueryExecutor::new(
            Schema::from_iter(mssql_query_node.columns.iter().map(Field::from)),
            MssqlConnectionConfig {
                host: mssql_query_node.hostname.clone(),
                port,
                user: mssql_query_node.username.clone(),
                password: mssql_query_node.password.clone(),
                database: mssql_query_node.database.clone(),
                encrypt,
                trust_cert,
            },
            mssql_query_node.query.clone(),
            source.plan_node().get_identity().clone(),
            source.context().get_config().developer.chunk_size,
        )))
    }
}
