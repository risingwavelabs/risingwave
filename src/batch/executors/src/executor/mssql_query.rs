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

/// Replace string literals, line/block comments, and `"..."` quoted
/// identifiers with spaces (preserving newlines) so the caller can scan
/// for top-level keywords without being fooled by SQL embedded inside.
///
/// **Must be a single pass**: a two-pass approach (`strip_sql_comments`
/// then `mask_sql_string_literals`) is vulnerable to a payload such as
/// `SELECT '--'\n; DELETE FROM t` — the comment stripper eats the `--`
/// inside the `'--'` literal, swallows the closing `'`, and the trailing
/// `; DELETE` ends up un-masked.
fn mask_sql_for_validation(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            // Single-quoted string literal: '...'. T-SQL escaped quote is ''.
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                        out.push(' ');
                        out.push(' ');
                        i += 2;
                        continue;
                    }
                    out.push(' ');
                    i += 1;
                    break;
                }
                out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                i += 1;
            }
            continue;
        }
        if bytes[i] == b'"' {
            // Quoted identifier "..." (T-SQL). Only valid at a word
            // boundary; in any other position `"` would not appear.
            let prev = if i > 0 { bytes[i - 1] } else { b' ' };
            if b"\n \t(,.".contains(&prev) {
                out.push(' ');
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                    i += 1;
                }
                if i < bytes.len() {
                    out.push(' ');
                    i += 1;
                }
                continue;
            }
        }
        if bytes[i] == b'[' {
            // Bracketed identifier [name] (T-SQL standard escape for
            // reserved words, e.g. `SELECT 1 AS [INTO] FROM t`). Masked
            // so the contents don't match keyword checks below.
            out.push(' ');
            i += 1;
            while i < bytes.len() && bytes[i] != b']' {
                out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                i += 1;
            }
            if i < bytes.len() {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            // Line comment -- ...  (only reached when not inside a string
            // — strings are handled by the first branch above).
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            // Block comment /* ... */
            out.push(' ');
            out.push(' ');
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                out.push(if bytes[i] == b'\n' { '\n' } else { ' ' });
                i += 1;
            }
            if i + 1 < bytes.len() {
                out.push(' ');
                out.push(' ');
                i += 2;
            }
            continue;
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn has_top_level_keyword(masked: &str, keyword: &str) -> bool {
    let bytes = masked.as_bytes();
    let kw = keyword.as_bytes();
    let kw_len = kw.len();
    let mut depth: i32 = 0;
    let mut i = 0;
    while i + kw_len <= bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {
                if depth == 0
                    && bytes[i..i + kw_len].eq_ignore_ascii_case(kw)
                    && (i == 0 || !is_ident_char(bytes[i - 1]))
                    && (i + kw_len >= bytes.len() || !is_ident_char(bytes[i + kw_len]))
                {
                    return true;
                }
            }
        }
        i += 1;
    }
    false
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Enforce that the user-provided query is a single read-only `SELECT` or
/// `WITH ... SELECT` CTE. Rejects:
///  - non-`SELECT`/`WITH` first keyword (INSERT/UPDATE/DELETE/DDL/EXEC/...),
///  - `WITH cte AS (...) DELETE FROM t` — CTE followed by a write,
///  - `SELECT ... INTO new_table FROM src` — `INTO` is a write in T-SQL,
///  - semicolon-delimited multi-statement batches (`tiberius::Client::query`
///    would happily run every statement against the source credentials),
///  - **semicolon-free** multi-statement batches such as
///    `SELECT 1 AS x UPDATE dbo.t SET v = 2` — T-SQL does not require `;`
///    between statements, and `Client::query` would execute the trailing
///    `UPDATE` using the source credentials,
///  - **multiple top-level SELECTs** like `SELECT 1 SELECT 2`, whose result
///    sets are flattened by `into_row_stream`,
///  - CTEs with an explicit column list such as
///    `WITH cte(x) AS (SELECT 1) SELECT x FROM cte` — the optional
///    `(<columns>)` after the CTE name must be skipped when locating the
///    end of the CTE definition.
///
/// This is a lexical scan, not a full parser; the production source-
/// reference form should be paired with a read-only SQL Server principal.
fn validate_read_only_query(query: &str) -> Result<(), BatchError> {
    let masked = mask_sql_for_validation(query);

    let first_token: String = masked
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .find(|tok| !tok.is_empty())
        .map(|tok| {
            tok.trim_end_matches(|c: char| ",.)".contains(c))
                .to_ascii_lowercase()
        })
        .unwrap_or_default();
    if first_token != "select" && first_token != "with" {
        return Err(BatchError::from(anyhow::anyhow!(
            "mssql_query only accepts read-only statements \
             (SELECT / WITH ... SELECT); got `{} ...`",
            first_token
        )));
    }

    if first_token == "with" {
        // Find the end of the CTE clause. A CTE clause is one or more
        // comma-separated `name [(column_list)] AS (subquery)` definitions
        // followed by a single main statement (`SELECT`/`INSERT`/`UPDATE`/
        // `DELETE`/`MERGE`). We walk the query tracking paren depth,
        // skipping over the optional `(<column_list>)` that may follow
        // a CTE name (otherwise the close-paren of the column list is
        // mistaken for the close-paren of the CTE body). We then pick
        // the FIRST top-level close-paren whose next non-whitespace
        // token at depth 0 is not `,` — that's where the CTE list
        // ends and the main statement begins. Using the last close
        // paren instead breaks for
        // `WITH c AS (...) SELECT * FROM t WHERE x IN (SELECT 1)`,
        // where the trailing subquery's close paren would be
        // misidentified as the CTE boundary.
        let bytes = masked.as_bytes();
        let mut depth: i32 = 0;
        let mut cte_end: Option<usize> = None;
        let mut i: usize = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if b == b'(' {
                // Distinguish a column list `name(...)` from a CTE body
                // `AS (...)`: a column list is glued directly to the
                // preceding identifier (no whitespace). CTE bodies are
                // always preceded by whitespace + `AS`. Skip past the
                // matching `)` so it isn't recorded as a top-level
                // close-paren below.
                let prev_is_ident =
                    i > 0 && is_ident_char(bytes[i - 1]) && depth == 0;
                if prev_is_ident {
                    let mut inner: i32 = 1;
                    i += 1;
                    while i < bytes.len() && inner > 0 {
                        match bytes[i] {
                            b'(' => inner += 1,
                            b')' => inner -= 1,
                            _ => {}
                        }
                        i += 1;
                    }
                    continue;
                }
                depth += 1;
            } else if b == b')' {
                depth -= 1;
                if depth == 0 {
                    let rest = masked[i + 1..].trim_start();
                    if !rest.starts_with(',') {
                        cte_end = Some(i + 1);
                        break;
                    }
                }
            }
            i += 1;
        }
        let after_cte = masked[cte_end.unwrap_or(masked.len())..].trim_start();
        let next_token = after_cte
            .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
            .find(|tok| !tok.is_empty())
            .map(|tok| {
                tok.trim_end_matches(|c: char| ",.)".contains(c))
                    .to_ascii_lowercase()
            })
            .unwrap_or_default();
        if next_token != "select" {
            return Err(BatchError::from(anyhow::anyhow!(
                "mssql_query WITH clause must end with a SELECT; \
                 got `WITH ... {} ...`",
                next_token
            )));
        }
    }

    if has_top_level_keyword(&masked, "INTO") {
        return Err(BatchError::from(anyhow::anyhow!(
            "mssql_query does not allow SELECT ... INTO \
             (writes are not permitted)"
        )));
    }

    // Top-level DML / DDL / control-of-flow keywords. T-SQL treats any
    // of these at depth 0 as the start of a write (or otherwise
    // non-read-only) statement, even when no `;` precedes them. The
    // existing `has_top_level_keyword` helper enforces paren depth 0
    // and word boundaries, so identifiers like `update_log` are not
    // matched.
    for forbidden in [
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "CREATE",
        "ALTER",
        "DROP",
        "TRUNCATE",
        "EXEC",
        "EXECUTE",
        "GRANT",
        "REVOKE",
        "DENY",
    ] {
        if has_top_level_keyword(&masked, forbidden) {
            return Err(BatchError::from(anyhow::anyhow!(
                "mssql_query does not allow `{forbidden}` at the top level \
                 (writes / DDL are not permitted)"
            )));
        }
    }

    for forbidden in ["openquery", "openrowset", "opendatasource"] {
        if masked.to_ascii_lowercase().contains(forbidden) {
            return Err(BatchError::from(anyhow::anyhow!(
                "mssql_query does not allow `{forbidden}`; \
                 pass-through statements cannot be validated as read-only"
            )));
        }
    }

    // Multiple top-level SELECT statements must be connected by a set
    // operator (`UNION [ALL|DISTINCT]`, `EXCEPT [ALL]`, `INTERSECT [ALL]`)
    // — otherwise they are concatenated batches that `Client::query`
    // would execute one after another, flattening the result sets
    // through `into_row_stream`. `SELECT 1 SELECT 2` would otherwise
    // pass the first-token check above.
    let select_positions = collect_top_level_keyword_positions(&masked, "SELECT");
    if select_positions.len() > 1 {
        for w in select_positions.windows(2) {
            let gap = masked[w[0] + 6..w[1]].trim();
            let gap_upper = gap.to_ascii_uppercase();
            let connected = gap_upper.ends_with("UNION")
                || gap_upper.ends_with("UNION ALL")
                || gap_upper.ends_with("UNION DISTINCT")
                || gap_upper.ends_with("EXCEPT")
                || gap_upper.ends_with("EXCEPT ALL")
                || gap_upper.ends_with("INTERSECT")
                || gap_upper.ends_with("INTERSECT ALL");
            if !connected {
                return Err(BatchError::from(anyhow::anyhow!(
                    "mssql_query does not allow multiple top-level SELECT \
                     statements without a set operator; pass a single \
                     SELECT (or use UNION / EXCEPT / INTERSECT to combine)"
                )));
            }
        }
    }

    let mut s = masked.trim_end().to_owned();
    while let Some(c) = s.chars().last() {
        if c == ';' || c.is_whitespace() {
            s.pop();
        } else {
            break;
        }
    }
    if s.contains(';') {
        return Err(BatchError::from(anyhow::anyhow!(
            "mssql_query does not allow semicolon-delimited batches; \
             submit a single SELECT statement"
        )));
    }

    Ok(())
}

/// Return the byte offsets of every occurrence of `keyword` at depth 0
/// with proper word boundaries. Used to enumerate all top-level
/// statement-start positions for a given keyword (e.g. `SELECT`).
fn collect_top_level_keyword_positions(masked: &str, keyword: &str) -> Vec<usize> {
    let bytes = masked.as_bytes();
    let kw = keyword.as_bytes();
    let kw_len = kw.len();
    let mut depth: i32 = 0;
    let mut out = Vec::new();
    let mut i = 0;
    while i + kw_len <= bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {
                if depth == 0
                    && bytes[i..i + kw_len].eq_ignore_ascii_case(kw)
                    && (i == 0 || !is_ident_char(bytes[i - 1]))
                    && (i + kw_len >= bytes.len() || !is_ident_char(bytes[i + kw_len]))
                {
                    out.push(i);
                }
            }
        }
        i += 1;
    }
    out
}

/// `MssqlQuery` executor. Runs a query against a SQL Server database via `tiberius`.
pub struct MssqlQueryExecutor {
    schema: Schema,
    config: MssqlConnectionConfig,
    query: String,
    identity: String,
    chunk_size: usize,
    /// 0-based ordinals of `MONEY` / `SMALLMONEY` columns in
    /// `schema`, as discovered at bind time by `describe_mssql_query`
    /// (and carried through the plan node). Forwarded to the row
    /// decoder so the `i64 / 10000` → `Decimal` conversion runs even
    /// when Tiberius reports the wire column as `ColumnType::Intn`
    /// (which it does for `CAST(... AS MONEY)` expressions).
    money_column_indices: Vec<usize>,
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
    /// connection config, the user query, the plan node identity, the batch
    /// chunk size, and the bind-time `MONEY` / `SMALLMONEY` ordinals.
    /// The schema is computed at bind time by `describe_mssql_query`;
    /// this constructor only stores it.
    pub fn new(
        schema: Schema,
        config: MssqlConnectionConfig,
        query: String,
        identity: String,
        chunk_size: usize,
        money_column_indices: Vec<usize>,
    ) -> Self {
        Self {
            schema,
            config,
            query,
            identity,
            chunk_size,
            money_column_indices,
        }
    }

    /// Stream query results. Connects to SQL Server via `tiberius`, runs
    /// `self.query` verbatim, and yields each [`DataChunk`] decoded through
    /// [`sql_server_row_to_owned_row`]. Errors from connection setup, query
    /// execution, or row decoding are propagated as [`BatchError`].
    ///
    /// **Security**: the query is first run through
    /// [`validate_read_only_query`] to refuse DML/DDL or
    /// semicolon-delimited multi-statement batches. The 2-arg
    /// source-reference form uses credentials from the named
    /// `sqlserver-cdc` source, so a DML batch could otherwise modify
    /// external data; this gate prevents that.
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        tracing::debug!("mssql_query_executor: started");

        // Read-only enforcement: reject DML/DDL or semicolon-delimited
        // batches before we even open a connection. See
        // [`validate_read_only_query`] for the exact rules.
        validate_read_only_query(&self.query)?;

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
            let owned_row = sql_server_row_to_owned_row_with_money_indices(
                &mut row_mut,
                &self.schema,
                &self.money_column_indices,
            );
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
            mssql_query_node.money_column_indices.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SELECT * FROM t` is a single read-only statement — must pass.
    #[test]
    fn validate_read_only_query_accepts_simple_select() {
        validate_read_only_query("SELECT * FROM test").unwrap();
        validate_read_only_query("select * from test").unwrap();
        validate_read_only_query("  SELECT 1  ").unwrap();
    }

    /// CTEs (`WITH ... SELECT`) are read-only — must pass.
    #[test]
    fn validate_read_only_query_accepts_cte() {
        validate_read_only_query("WITH cte AS (SELECT id FROM test) SELECT * FROM cte").unwrap();
    }

    /// A trailing `;` is common in client tools — must be tolerated.
    #[test]
    fn validate_read_only_query_accepts_trailing_semicolon() {
        validate_read_only_query("SELECT 1;").unwrap();
        validate_read_only_query("SELECT 1 ; ").unwrap();
    }

    /// Semicolons *inside* the query (multi-statement batches) must be
    /// rejected. This is the critical guard against
    /// `SELECT 1; DELETE FROM test` running DML with the source
    /// credentials. (The new top-level DML/DDL keyword check fires
    /// *before* the trailing-semicolon check, so the error message
    /// for these probes mentions the offending keyword rather than
    /// `semicolon-delimited` — both are valid rejections.)
    #[test]
    fn validate_read_only_query_rejects_multi_statement_batch() {
        // Either error message is acceptable: the new top-level DML/DDL
        // check is stricter than the semicolon-delimited check, so it
        // fires first.
        for q in [
            "SELECT 1; DELETE FROM test",
            "-- a comment\nSELECT 1; /* another comment */ DROP TABLE test",
        ] {
            let err = validate_read_only_query(q).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("semicolon-delimited")
                    || msg.contains("not allowed")
                    || msg.contains("at the top level"),
                "expected rejection for {q:?}, got: {msg}"
            );
        }
    }

    /// DML / DDL statements are rejected regardless of case.
    #[test]
    fn validate_read_only_query_rejects_dml_and_ddl() {
        for q in [
            "INSERT INTO test VALUES (1)",
            "update test set x = 1",
            "DELETE FROM test",
            "drop table test",
            "CREATE TABLE x (id int)",
            "ALTER TABLE test ADD COLUMN x int",
            "TRUNCATE test",
            "exec sp_helpdb",
            "MERGE INTO test USING src ON test.id = src.id",
        ] {
            let err = validate_read_only_query(q).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("read-only"),
                "expected `read-only` error for `{q}`, got: {msg}"
            );
        }
    }

    /// Keywords that *look* like DML but live inside string literals or
    /// comments must not trigger a false positive.
    #[test]
    fn validate_read_only_query_ignores_dml_in_strings_and_comments() {
        validate_read_only_query("SELECT 'DROP TABLE test' AS msg FROM t").unwrap();
        validate_read_only_query("SELECT \"DELETE FROM test\" AS msg FROM t").unwrap();
        validate_read_only_query("SELECT 1 -- this comment mentions DELETE but is fine").unwrap();
        validate_read_only_query("SELECT 1 /* block comment mentioning INSERT is fine */ FROM t")
            .unwrap();
    }

    /// Empty / whitespace-only queries are rejected.
    #[test]
    fn validate_read_only_query_rejects_empty() {
        validate_read_only_query("").unwrap_err();
        validate_read_only_query("   \n\t  ").unwrap_err();
    }

    /// Bypass #1: `SELECT '--'\n; DELETE FROM t`. The earlier two-pass
    /// masker ate the `--` inside the `'--'` literal, hiding the
    /// trailing `; DELETE` from the validation. The single-pass masker
    /// now processes strings before comments, so the closing `'` is
    /// recognized correctly. The query is still rejected — by the
    /// new top-level DML/DDL keyword check first, then by the
    /// semicolon-delimited check.
    #[test]
    fn validate_read_only_query_rejects_delete_after_string_with_dashes() {
        let q = "SELECT '--'\n; DELETE FROM test";
        let err = validate_read_only_query(q).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("semicolon-delimited")
                || msg.contains("not allowed")
                || msg.contains("at the top level"),
            "expected rejection for {q:?}, got: {msg}"
        );
    }

    /// Bypass #2: `WITH cte AS (...) DELETE FROM t`. The first-token
    /// check accepted `WITH`; we now also verify that the post-CTE
    /// statement is `SELECT`.
    #[test]
    fn validate_read_only_query_rejects_with_followed_by_delete() {
        let q = "WITH cte AS (SELECT 1 AS x) DELETE FROM test";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string()
                .contains("WITH clause must end with a SELECT"),
            "expected `WITH ... SELECT` error for {q:?}, got: {err}"
        );
    }

    /// `SELECT ... INTO new_table FROM src` is a write in T-SQL and must
    /// be rejected.
    #[test]
    fn validate_read_only_query_rejects_select_into() {
        let q = "SELECT * INTO new_table FROM src";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string().contains("SELECT ... INTO"),
            "expected `SELECT ... INTO` error for {q:?}, got: {err}"
        );
    }

    /// `WITH cte AS (...) INSERT INTO t SELECT *` is also a write, but
    /// reaches us via the WITH-check (post-CTE token is INSERT, not
    /// SELECT) rather than the first-token check.
    #[test]
    fn validate_read_only_query_rejects_with_followed_by_insert() {
        let q = "WITH cte AS (SELECT 1 AS x) INSERT INTO t SELECT x FROM cte";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string()
                .contains("WITH clause must end with a SELECT"),
            "expected `WITH ... SELECT` error for {q:?}, got: {err}"
        );
    }

    /// `INTO` mentioned in a column list (e.g. inside parentheses) is
    /// fine — only top-level `INTO` is rejected.
    #[test]
    fn validate_read_only_query_allows_into_in_subquery() {
        // `IN` must not match the `INTO` keyword check.
        validate_read_only_query(
            "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte WHERE x IN (SELECT 1)",
        )
        .unwrap();
        // `INTO` inside parentheses is not top level.
        validate_read_only_query("SELECT 1 AS [INTO] FROM t").unwrap();
    }

    /// Bypass #3: a semicolon-free batch `SELECT 1 AS x UPDATE ... SET ...`.
    /// T-SQL does not require `;` between statements; `Client::query` would
    /// execute the trailing `UPDATE`. The validator must reject any
    /// top-level DML/DDL keyword regardless of the presence of `;`.
    #[test]
    fn validate_read_only_query_rejects_semicolon_free_update() {
        let q = "SELECT 1 AS x UPDATE dbo.review_probe SET value = 2";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string().contains("UPDATE"),
            "expected UPDATE rejection for {q:?}, got: {err}"
        );
    }

    /// Bypass #4: semicolon-free DDL after a SELECT. The new top-level
    /// DML/DDL check rejects each example with a specific error
    /// message naming the offending keyword.
    #[test]
    fn validate_read_only_query_rejects_semicolon_free_ddl() {
        for (q, expected_kw) in [
            ("SELECT 1 DROP TABLE test", "DROP"),
            ("SELECT 1; DROP TABLE test", "DROP"),   // also caught by `;` check
            ("SELECT 1 AS x TRUNCATE TABLE test", "TRUNCATE"),
            ("SELECT 1 EXEC sp_helpdb", "EXEC"),
        ] {
            let err = validate_read_only_query(q).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("not allowed")
                    || msg.contains(expected_kw)
                    || msg.contains("semicolon-delimited"),
                "expected rejection for {q:?}, got: {msg}"
            );
        }
    }

    /// Bypass #5: multiple top-level SELECTs concatenated without a
    /// semicolon. `Client::query` would run them in sequence and
    /// `into_row_stream` would flatten the result sets, hiding the
    /// second statement from the caller.
    #[test]
    fn validate_read_only_query_rejects_multiple_top_level_selects() {
        let q = "SELECT 1 AS x SELECT 2 AS x";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string().contains("multiple top-level SELECT"),
            "expected multiple-SELECTs rejection for {q:?}, got: {err}"
        );
    }

    /// Set operators (`UNION [ALL|DISTINCT]`, `EXCEPT`, `INTERSECT`) are
    /// valid between SELECTs and must NOT be rejected.
    #[test]
    fn validate_read_only_query_allows_set_operators_between_selects() {
        validate_read_only_query("SELECT 1 UNION SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 UNION ALL SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 UNION DISTINCT SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 EXCEPT SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 EXCEPT ALL SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 INTERSECT SELECT 2").unwrap();
        validate_read_only_query("SELECT 1 INTERSECT ALL SELECT 2").unwrap();
        // Three-way, mixed.
        validate_read_only_query(
            "SELECT a FROM t1 UNION SELECT b FROM t2 EXCEPT SELECT c FROM t3",
        )
        .unwrap();
    }

    /// A DML keyword that is part of an identifier (e.g. table named
    /// `update_log`) must not trigger a false positive.
    #[test]
    fn validate_read_only_query_allows_dml_substring_as_identifier() {
        validate_read_only_query("SELECT * FROM update_log").unwrap();
        validate_read_only_query("SELECT * FROM inserted_rows WHERE id = 1").unwrap();
        validate_read_only_query("WITH cte AS (SELECT * FROM deleted_log) SELECT * FROM cte").unwrap();
    }

    /// CTEs with an explicit column list — `WITH cte(x) AS (...)` — must
    /// be accepted. The optional `(<columns>)` after the CTE name is
    /// skipped when locating the CTE definition's close paren.
    #[test]
    fn validate_read_only_query_accepts_cte_with_column_list() {
        validate_read_only_query("WITH cte(x) AS (SELECT 1) SELECT x FROM cte").unwrap();
        validate_read_only_query(
            "WITH cte(a, b) AS (SELECT 1, 2) SELECT a + b AS s FROM cte",
        )
        .unwrap();
        // Multiple CTEs, each with an explicit column list.
        validate_read_only_query(
            "WITH cte1(a) AS (SELECT 1), cte2(b) AS (SELECT 2) SELECT a + b FROM cte1, cte2",
        )
        .unwrap();
    }

    /// DML after a CTE with an explicit column list is still rejected.
    /// (Coverage for the interaction between the column-list skip and
    /// the post-CTE token check.)
    #[test]
    fn validate_read_only_query_rejects_cte_with_column_list_followed_by_write() {
        let q = "WITH cte(x) AS (SELECT 1) DELETE FROM test";
        let err = validate_read_only_query(q).unwrap_err();
        assert!(
            err.to_string()
                .contains("WITH clause must end with a SELECT"),
            "expected WITH ... SELECT error for {q:?}, got: {err}"
        );
    }
}
