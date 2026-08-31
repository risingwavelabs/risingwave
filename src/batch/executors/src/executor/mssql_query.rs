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
use anyhow::anyhow;
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

/// Parse a strict boolean TLS flag from a plan-node string. Mirror of the
/// binder-side parser: the binder validates user input, but the executor
/// re-validates because plan nodes can be deserialized from disk or
/// produced by older planner versions. Invalid values are an error, never a
/// silent fallback to a permissive default.
fn parse_strict_tls_flag(value: &str, default: bool) -> Result<bool, BatchError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        // The empty string is reserved as the "absent argument" sentinel
        // and falls back to the default; the binder only emits it for the
        // 2-arg source-reference form, never for an inline 8-arg call.
        "" => Ok(default),
        other => Err(BatchError::from(anyhow::anyhow!(
            "expected 'true' or 'false', got {:?}",
            other
        ))),
    }
}

/// `MssqlQuery` executor. Runs a query against a SQL Server database via `tiberius`.
pub struct MssqlQueryExecutor {
    schema: Schema,
    config: MssqlConnectionConfig,
    query: String,
    identity: String,
    chunk_size: usize,
}

impl Executor for MssqlQueryExecutor {
    /// Return the result schema, which was discovered at bind time via
    /// `describe_mssql_query` and stored at construction. The schema has one
    /// field per visible column of the user query (synthetically named
    /// `column_N` for unnamed columns such as `SELECT COUNT(*)`).
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    /// Return the identity string copied from the plan node, used in
    /// tracing and error messages.
    fn identity(&self) -> &str {
        &self.identity
    }

    /// Consume the executor and return a stream of `DataChunk`s. Delegates
    /// to the `#[try_stream]`-decorated `do_execute` async fn.
    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl MssqlQueryExecutor {
    /// Build a new [`MssqlQueryExecutor`] from the pre-discovered schema,
    /// connection config, the user query, the plan node identity, and the
    /// batch chunk size. The schema is computed at bind time by
    /// `describe_mssql_query`; this constructor only stores it.
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

    /// Stream query results. Connects to SQL Server via `tiberius`, runs
    /// `self.query` verbatim, and yields each [`DataChunk`] decoded through
    /// [`sql_server_row_to_owned_row`]. Errors from connection setup, query
    /// execution, or row decoding are propagated as [`BatchError`].
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

        // Deserialize rows. Decoding is delegated to the canonical
        // [`sql_server_row_to_owned_row`] helper in the connector crate, which
        // routes through [`ScalarImplTiberiusWrapper`] — the RisingWave-side
        // `tiberius::FromSql` impl. It handles: `Decimal` ← `Numeric`,
        // `Timestamptz` ← `DateTimeOffset`, `Varchar` ← `UniqueIdentifier`
        // (uppercased) / `Xml`, etc.
        //
        // Cells whose target [`DataType`] has no direct tiberius mapping (e.g.
        // `Jsonb`, `Interval`) are returned as NULL with a warning. Cells
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

/// Builder for [`MssqlQueryExecutor`]. Unpacks a [`NodeBody::MssqlQuery`]
/// batch plan node into a ready-to-execute [`MssqlQueryExecutor`].
pub struct MssqlQueryExecutorBuilder {}

impl BoxedExecutorBuilder for MssqlQueryExecutorBuilder {
    /// Decode a `NodeBody::MssqlQuery` batch plan node, parse the SQL Server
    /// port as `u16`, validate the `encrypt` / `trust_cert` TLS flags
    /// strictly (any non-`true`/`false` value is a build error rather than
    /// a silent fallback), and assemble the [`MssqlConnectionConfig`].
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
        //
        // These values are pre-validated at bind time. A parse error here
        // means the plan was produced by a non-conforming client (e.g. a
        // different planner version or hand-crafted proto) — fail loudly
        // rather than silently downgrading to insecure defaults.
        let encrypt = parse_strict_tls_flag(&mssql_query_node.encrypt, false)
            .context("invalid `encrypt` value in MssqlQuery plan node")?;
        let trust_cert = parse_strict_tls_flag(&mssql_query_node.trust_cert, true)
            .context("invalid `trust_cert` value in MssqlQuery plan node")?;

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
