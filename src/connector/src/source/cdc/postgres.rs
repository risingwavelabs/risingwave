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

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::LazyLock;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio_postgres::types::{PgLsn, Type as PgType};

use crate::connector_common::postgres::postgres_point_type;
use crate::connector_common::{
    PgConnectionConfig, PostgresExternalTable, SslMode, create_pg_client,
    create_pg_client_from_properties, discover_pgvector_dimensions,
};
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::schema_change::{TableChangeColumn, TableChangeContext};
use crate::parser::{
    AccessError, AccessResult, DebeziumTransactionStatus, TransactionControl, jsonb_access_field,
    postgres_cell_to_scalar_impl_strict, postgres_row_to_owned_row_with_strict_pk,
};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption,
    DebeziumOffset, ExternalTable, ExternalTableConfig, ExternalTableReader, SCHEMA_NAME_KEY,
    SchemaTableName, TABLE_NAME_KEY,
};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumOffsetInterpreter, CdcDebeziumPayloadInterpreter, CdcExternalTableHandler,
    CdcExternalTableNameParser, CdcSerializedNames, CdcSourceFrontendValidator,
    CdcSourceRuntimeFlags, Postgres,
    db_name_end_and_table_name_start_from_schema_qualified_topic_suffix,
    derived_cdc_offset_variant, parse_debezium_offset,
};

// ------- Persisted names -------

impl CdcSerializedNames for Postgres {
    const CDC_OFFSET_VARIANT: &'static str = derived_cdc_offset_variant!(Postgres);
    const DEBEZIUM_CDC_SPLIT_SPLIT_FIELD: &'static str = "pg_split";
}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Postgres {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[
        "cdc.source.wait.streaming.start.timeout",
        "debezium.max.queue.size",
        "debezium.queue.memory.ratio",
        "debezium.heartbeat.interval.ms",
        "hostname",
        "port",
        "password",
    ];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] = &[
        (PbFormatType::Debezium, &[PbEncodeType::Json]),
        // support source stream job
        (PbFormatType::Plain, &[PbEncodeType::Json]),
    ];

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        fill_and_validate_postgres_props(props)
    }
}

/// Shared by Postgres and Citus.
pub(crate) fn fill_and_validate_postgres_props(
    props: &mut BTreeMap<String, String>,
) -> ConnectorResult<()> {
    match props.get("slot.name") {
        None => {
            // Build a random slot name with UUID
            // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4();
            props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));
        }
        Some(slot_name) => {
            // please refer to
            // - https://github.com/debezium/debezium/blob/97956ce25b7612e3413d363658661896b7d2e0a2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java#L1179
            // - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
            if !slot_name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                || slot_name.len() > 63
            {
                bail!(
                    "Invalid replication slot name: {:?}. Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63",
                    slot_name
                );
            }
        }
    }

    if !props.contains_key("schema.name") {
        // Default schema name is "public"
        props.insert("schema.name".into(), "public".into());
    }
    if !props.contains_key("publication.name") {
        // Build a random publication name with UUID to avoid conflicts between sources
        // e.g. "rw_publication_f9a3567e6dd54bf5900444c8b1c03815"
        let uuid = uuid::Uuid::new_v4();
        props.insert(
            "publication.name".into(),
            format!("rw_publication_{}", uuid.simple()),
        );
    }
    if !props.contains_key("publication.create.enable") {
        // Default auto create publication if doesn't exist
        props.insert("publication.create.enable".into(), "true".into());
    }
    Ok(())
}

// ------- Runtime flags -------

impl CdcSourceRuntimeFlags for Postgres {
    const CAN_BACKFILL: bool = true;
    const COMMIT_OFFSET_ON_CHECKPOINT: bool = true;
    const ENABLE_TRANSACTION_METADATA: bool = true;
    const SUPPORTS_PARALLEL_BACKFILL: bool = true;
}

// ------- Table name -------

impl CdcExternalTableNameParser for Postgres {
    /// Accepts `schema.table` with optional double-quoted identifiers, e.g. `public."Orders"`.
    /// The returned name has the quotes stripped, `public.Orders`, which is how Debezium reports
    /// the table in change-event topics and schema-change events.
    fn fill_table_name_options(
        options: &mut BTreeMap<String, String>,
        external_table_name: &str,
    ) -> ConnectorResult<String> {
        let (schema_name, table_name) =
            parse_postgres_cdc_external_table_name(external_table_name)?;
        let normalized_external_table_name = format!("{}.{}", schema_name, table_name);
        options.insert(SCHEMA_NAME_KEY.into(), schema_name);
        options.insert(TABLE_NAME_KEY.into(), table_name);
        Ok(normalized_external_table_name)
    }

    fn schema_table_name_from_options(options: &BTreeMap<String, String>) -> SchemaTableName {
        SchemaTableName {
            schema_name: options.get(SCHEMA_NAME_KEY).cloned().unwrap_or_default(),
            table_name: options.get(TABLE_NAME_KEY).cloned().unwrap_or_default(),
        }
    }

    fn db_name_end_and_table_name_start_from_debezium_topic_suffix(
        topic_suffix: &str,
    ) -> (usize, usize) {
        db_name_end_and_table_name_start_from_schema_qualified_topic_suffix(topic_suffix)
    }
}

/// Parse the schema/table name from the CDC `TABLE` clause.
///
/// Column names do not need the same parsing here: wildcard schema derivation reads
/// them from PostgreSQL catalogs after the exact table has been identified.
fn parse_postgres_cdc_external_table_name(
    external_table_name: &str,
) -> ConnectorResult<(String, String)> {
    let mut parts = vec![];
    let mut current = String::new();
    let mut chars = external_table_name.chars().peekable();
    let mut in_quote = false;
    let mut just_closed_quote = false;

    while let Some(ch) = chars.next() {
        if in_quote {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quote = false;
                    just_closed_quote = true;
                }
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                '.' => {
                    if current.is_empty() {
                        bail!(
                            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                            external_table_name
                        );
                    }
                    parts.push(std::mem::take(&mut current));
                    just_closed_quote = false;
                }
                '"' if current.is_empty() => {
                    in_quote = true;
                }
                '"' => {
                    bail!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    );
                }
                _ if just_closed_quote => {
                    bail!(
                        "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
                        external_table_name
                    );
                }
                _ => current.push(ch),
            }
        }
    }

    if in_quote || current.is_empty() {
        bail!(
            "Invalid Postgres CDC table name '{}'. Expected 'schema.table'.",
            external_table_name
        );
    }
    parts.push(current);

    if let [schema_name, table_name] = parts.as_slice() {
        Ok((schema_name.clone(), table_name.clone()))
    } else {
        bail!("The upstream table name must contain schema name prefix, e.g. 'public.table'")
    }
}

// ------- Debezium payload -------

/// JDBC type constants found in Debezium schema change events.
mod debezium_sql_types {
    pub const STRUCT: i32 = 2002;
    pub const ARRAY: i32 = 2003;
}

impl CdcDebeziumPayloadInterpreter for Postgres {
    /// The id is `txId:LSN`; only the transaction id is kept.
    fn transaction_control_from_transaction_meta(
        status: DebeziumTransactionStatus,
        id: &str,
    ) -> Option<TransactionControl> {
        let (tx_id, _) = id.split_once(':').unwrap();
        Some(status.into_control(tx_id))
    }

    /// Builtin types come from `type_name_to_pg_type`. User-defined enum and composite types are
    /// not in that list because their names are user-chosen; Debezium marks enums with a
    /// non-null `enumValues` and composites with `jdbcType == STRUCT`, and both become `varchar`
    /// (composite values are converted to text by our `CustomConverter`). `vector` columns and
    /// arrays of user-defined types need the upstream catalog, which is queried with
    /// `source_properties`. Default expressions arrive as `'value'::type` and are stripped to
    /// `value`.
    async fn columns_from_table_change(
        ctx: &TableChangeContext<'_>,
    ) -> AccessResult<Vec<TableChangeColumn>> {
        let mut pgvector_dims_cache: HashMap<(String, String), HashMap<String, usize>> =
            HashMap::new();
        let mut columns = vec![];
        for col in ctx.columns.array_elements().unwrap() {
            let name: String = jsonb_access_field!(col, "name", string);
            let type_name: String = jsonb_access_field!(col, "typeName", string);
            let is_enum =
                matches!(col.access_object_field("enumValues"), Some(val) if !val.is_jsonb_null());
            let jdbc_type = col
                .access_object_field("jdbcType")
                .and_then(|v| v.as_number().ok())
                .map(|n| n.0 as i32);
            let is_composite = jdbc_type == Some(debezium_sql_types::STRUCT);

            let data_type = if is_composite || is_enum {
                tracing::debug!(target: "auto_schema_change",
                    "Convert PostgreSQL user-defined type '{}' ({}) to VARCHAR",
                    type_name,
                    if is_composite { "composite" } else { "enum" });
                DataType::Varchar
            } else if type_name.eq_ignore_ascii_case("vector") {
                let Some(cache_key) = parse_schema_table_from_debezium_id(ctx.debezium_table_id)
                else {
                    return Err(ctx.unsupported_type(type_name));
                };
                if !pgvector_dims_cache.contains_key(&cache_key) {
                    let fetched = fetch_pgvector_dimensions_for_table(
                        ctx.source_properties,
                        &cache_key.0,
                        &cache_key.1,
                    )
                    .await?;
                    pgvector_dims_cache.insert(cache_key.clone(), fetched);
                }
                match pgvector_dims_cache
                    .get(&cache_key)
                    .and_then(|m| m.get(name.as_str()).copied())
                {
                    Some(dim) if (1..=DataType::VEC_MAX_SIZE).contains(&dim) => {
                        DataType::Vector(dim)
                    }
                    _ => return Err(ctx.unsupported_type(type_name)),
                }
            } else {
                match type_name_to_pg_type(type_name.as_str()) {
                    Some(ty) => match pg_type_to_rw_type(&ty) {
                        Ok(data_type) => data_type,
                        Err(err) => {
                            tracing::warn!(error=%err.as_report(), "unsupported postgres type in schema change message");
                            return Err(ctx.unsupported_type(type_name));
                        }
                    },
                    None if jdbc_type == Some(debezium_sql_types::ARRAY)
                        && can_fallback_array_to_varchar(
                            ctx.source_properties,
                            type_name.as_str(),
                        )
                        .await? =>
                    {
                        tracing::debug!(target: "auto_schema_change",
                            "Fall back PostgreSQL array type '{}' to VARCHAR[]",
                            type_name);
                        DataType::Varchar.list()
                    }
                    None => return Err(ctx.unsupported_type(type_name)),
                }
            };

            let default_value_expression = col
                .access_object_field("defaultValueExpression")
                .filter(|v| !v.is_jsonb_null())
                .map(|v| v.as_str().unwrap().to_owned());
            let default_value_text = default_value_expression.as_deref().and_then(|expr| {
                expr.split("::")
                    .map(|s| s.trim_matches('\''))
                    .next()
                    .map(ToOwned::to_owned)
            });
            columns.push(TableChangeColumn {
                name,
                data_type,
                default_value_expression,
                default_value_text,
            });
        }
        Ok(columns)
    }
}

/// Parse Debezium `tableChanges[].id` into `(schema_name, table_name)`.
///
/// Input examples observed in Debezium schema-change events:
/// - `"public"."orders"` (quoted format)
/// - `public.orders` (plain dotted format)
/// - `db.public.orders` (database-prefixed dotted format)
///
/// Why two parsing branches:
/// - Different Debezium versions/connectors may emit quoted or plain identifiers.
/// - We normalize both forms so downstream lookup can consistently query upstream catalogs.
///
/// Output:
/// - `Some((schema, table))` when both schema and table are successfully extracted.
/// - `None` when the id is malformed or does not contain enough segments.
fn parse_schema_table_from_debezium_id(id: &str) -> Option<(String, String)> {
    let trimmed = id.trim();
    if trimmed.contains("\".\"") {
        let parts: Vec<_> = trimmed
            .split("\".\"")
            .map(|s| s.trim_matches('"').trim())
            .collect();
        if parts.len() >= 2 {
            let schema = parts[parts.len() - 2].to_owned();
            let table = parts[parts.len() - 1].to_owned();
            if !schema.is_empty() && !table.is_empty() {
                return Some((schema, table));
            }
        }
    }

    let cleaned = trimmed.trim_matches('"');
    let parts: Vec<_> = cleaned.split('.').collect();
    if parts.len() >= 2 {
        let schema = parts[parts.len() - 2].trim().to_owned();
        let table = parts[parts.len() - 1].trim().to_owned();
        if !schema.is_empty() && !table.is_empty() {
            return Some((schema, table));
        }
    }
    None
}

async fn fetch_pgvector_dimensions_for_table(
    props: &BTreeMap<String, String>,
    schema: &str,
    table: &str,
) -> AccessResult<HashMap<String, usize>> {
    let client = create_pg_client_from_properties(props, None)
        .await
        .map_err(|err| AccessError::Uncategorized {
            message: format!(
                "failed to connect upstream postgres for schema change lookup: {}",
                err.as_report()
            ),
        })?;

    discover_pgvector_dimensions(&client, schema, table)
        .await
        .map_err(|err| AccessError::Uncategorized {
            message: format!(
                "failed to query upstream postgres schema for {schema}.{table}: {}",
                err.as_report()
            ),
        })
}

/// Decide whether an unknown array column type (one not resolved by
/// `type_name_to_pg_type`) can be represented as `varchar[]` in RW. This is the
/// single question this function answers; the criteria behind it may grow over
/// time.
///
/// Right now the answer is "yes iff the array's element type is a user-defined
/// enum or composite". Debezium does not expose element enum/composite metadata
/// on the array column (the `enumValues` field is only set for scalar enum
/// columns, and `jdbcType == STRUCT` is only set for scalar composite columns),
/// so we ask the upstream catalog directly: follow the array type's `typelem`
/// to its element type and check `typtype IN ('e', 'c')`. Enum values are plain
/// text, and composite values are converted to text by our `CustomConverter`,
/// hence `varchar[]`.
async fn can_fallback_array_to_varchar(
    props: &BTreeMap<String, String>,
    array_type_name: &str,
) -> AccessResult<bool> {
    let client = create_pg_client_from_properties(props, None)
        .await
        .map_err(|err| AccessError::Uncategorized {
            message: format!(
                "failed to connect upstream postgres for schema change lookup: {}",
                err.as_report()
            ),
        })?;

    let row = client
        .query_opt(
            "SELECT t_elem.typtype IN ('e', 'c') \
             FROM pg_type t \
             JOIN pg_type t_elem ON t.typelem = t_elem.oid \
             WHERE t.typname = $1 \
             LIMIT 1",
            &[&array_type_name],
        )
        .await
        .map_err(|err| AccessError::Uncategorized {
            message: format!(
                "failed to query upstream postgres for array element type of `{array_type_name}`: {}",
                err.as_report()
            ),
        })?;

    Ok(row.map(|r| r.get::<_, bool>(0)).unwrap_or(false))
}

// ------- Split -------

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PostgresCdcSplit {
    pub inner: CdcSplitBase,
}

impl CdcSplitTrait for PostgresCdcSplit {
    fn inner(&self) -> &CdcSplitBase {
        &self.inner
    }

    fn inner_mut(&mut self) -> &mut CdcSplitBase {
        &mut self.inner
    }

    fn update_offset(
        &mut self,
        last_seen_offset: String,
        snapshot_done: bool,
    ) -> ConnectorResult<()> {
        // Monotonicity guard for streaming-phase Postgres CDC offsets.
        //
        // Debezium's task-level auto-restart (BaseSourceTask) reloads
        // effectiveOffset from ConfigurableOffsetBackingStore, which lags
        // dbz's in-memory position by one barrier + async commit cycle.
        // After a broken pipe, dbz re-emits older WAL events whose LSNs
        // are smaller than the freshest entry we already persisted.
        // Without this guard those stale LSNs would overwrite the split
        // state, the store would then be pushed backwards via the next
        // barrier, and a subsequent restart would load the stale value.
        //
        // We guard both `lsn_commit` and `lsn_proc` because dbz consumes
        // them for two different things on resume:
        //   - `lsn_commit` is what `validateLogPosition` checks against
        //     PG slot.restart_lsn; regression here causes a fatal
        //     `DebeziumException` ("change stream ... no longer available").
        //   - `lsn_proc` is the start position for `startStreaming` and
        //     the key `WalPositionLocator` uses to deduplicate replayed
        //     events on restart; regression here can lead to silent
        //     data loss / a hung resume if PG has already advanced past
        //     the rewound value.
        //
        // The per-message `lsn` field is intentionally NOT used as a
        // monotonicity key: under concurrent overlapping transactions
        // PG streams events in commit order, so the per-event physical
        // lsn can legitimately move backwards when a long-running
        // earlier transaction commits later than a shorter newer one.
        // Only `lsn_commit` and `lsn_proc` are monotonic under correct
        // dbz behavior.
        //
        // If both offsets are parseable as streaming Postgres offsets, use
        // the same `(lsn_commit, lsn_proc)` ordering as snapshot backfill
        // deduplication. Snapshot / heartbeat / incomplete offsets are not
        // comparable and pass through.
        if let (Some(old_offset), Some(new_offset)) = (
            self.inner
                .start_offset
                .as_deref()
                .and_then(parse_postgres_offset_from_offset_str),
            parse_postgres_offset_from_offset_str(&last_seen_offset),
        ) && new_offset < old_offset
        {
            tracing::warn!(
                split_id = self.inner.split_id,
                ?old_offset,
                ?new_offset,
                "Rejecting backward Postgres CDC offset update; \
                 keeping current state to prevent state-table regression."
            );
            return Ok(());
        }

        self.inner.snapshot_done = snapshot_done;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl PostgresCdcSplit {
    /// Extract PostgreSQL LSN value from the offset JSON string.
    ///
    /// This function parses the offset JSON and extracts the LSN value from the sourceOffset.lsn field.
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
        let source_offset = offset.get("sourceOffset")?;
        let lsn = source_offset.get("lsn")?;
        lsn.as_u64()
    }

    /// Extract PostgreSQL `lsn_commit` from the current offset (last commit position).
    pub fn pg_lsn_commit(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_commit_from_offset_str(offset_str)
    }

    /// Extract PostgreSQL `lsn_proc` from the current offset (last completely processed position).
    pub fn pg_lsn_proc(&self) -> Option<u64> {
        let offset_str = self.inner.start_offset.as_ref()?;
        extract_postgres_lsn_proc_from_offset_str(offset_str)
    }
}

impl DebeziumCdcSplit<Postgres> {
    /// Extract PostgreSQL LSN value from the current split offset.
    ///
    /// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
    pub fn pg_lsn(&self) -> Option<u64> {
        self.postgres_split.as_ref()?.pg_lsn()
    }
}

/// Extract PostgreSQL LSN value from a CDC offset JSON string.
///
/// This is a standalone helper function that can be used when you only have the offset string
/// (e.g., in callbacks) and don't have access to the Split object.
///
/// Returns Some(lsn) if the LSN is found and can be parsed as u64, None otherwise.
pub fn extract_postgres_lsn_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn")?;
    lsn.as_u64()
}

fn parse_postgres_offset_from_offset_str(offset_str: &str) -> Option<PostgresOffset> {
    parse_debezium_offset::<Postgres>(offset_str).ok()
}

/// Extract PostgreSQL `lsn_commit` from a CDC offset JSON string.
///
/// `lsn_commit` is the WAL position of the last COMMIT message dbz has
/// processed. It is the value dbz flushes to PG (`flushLsn`) to advance
/// `confirmed_flush_lsn`, and the value `validateLogPosition` compares
/// against `slot.restart_lsn` on restart. Must be monotonic across
/// restarts -- a regression here can cause a fatal `DebeziumException`
/// ("change stream ... no longer available").
pub fn extract_postgres_lsn_commit_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_commit")?;
    lsn.as_u64()
}

/// Extract PostgreSQL `lsn_proc` from a CDC offset JSON string.
///
/// `lsn_proc` is the WAL position of the last message dbz has fully
/// processed. On restart dbz uses it as the start position passed to
/// `startStreaming`, and as the dedup key for `WalPositionLocator` to
/// filter replayed events. A regression can cause silent data loss or
/// a hung resume if PG has already advanced past the rewound value.
pub fn extract_postgres_lsn_proc_from_offset_str(offset_str: &str) -> Option<u64> {
    let offset = serde_json::from_str::<serde_json::Value>(offset_str).ok()?;
    let source_offset = offset.get("sourceOffset")?;
    let lsn = source_offset.get("lsn_proc")?;
    lsn.as_u64()
}

// ------- External table -------

impl ExternalTable for PostgresExternalTable {
    fn column_descs(&self) -> &[ColumnDesc] {
        PostgresExternalTable::column_descs(self)
    }

    fn pk_names(&self) -> &[String] {
        PostgresExternalTable::pk_names(self)
    }
}

/// Postgres-specific options of [`ExternalTableConfig`].
#[derive(Debug, Default, Clone, Deserialize)]
pub struct PostgresTableConfig {
    /// SSL/TLS level of the upstream connection: `disabled`, `preferred`, `required`,
    /// `verify-ca` or `verify-full`.
    #[serde(rename = "ssl.mode", default = "ssl_mode_default")]
    #[serde(alias = "debezium.database.sslmode")]
    pub ssl_mode: SslMode,

    #[serde(rename = "ssl.root.cert")]
    #[serde(alias = "debezium.database.sslrootcert")]
    pub ssl_root_cert: Option<String>,
}

/// Default to `disabled` for backward compatibility.
fn ssl_mode_default() -> SslMode {
    SslMode::Disabled
}

impl ExternalTableConfig<PostgresTableConfig> {
    /// The connection parameters in the shape the shared Postgres client helpers take.
    pub fn pg_connection_config(&self) -> ConnectorResult<PgConnectionConfig> {
        let port = self
            .port
            .parse::<u16>()
            .with_context(|| format!("invalid postgres port `{}`", self.port))?;
        Ok(PgConnectionConfig {
            host: self.host.clone(),
            port,
            user: self.username.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            ssl_mode: self.specific.ssl_mode.clone(),
            ssl_root_cert: self.specific.ssl_root_cert.clone(),
        })
    }
}

impl CdcExternalTableHandler for Postgres {
    type TableReader = PostgresExternalTableReader;

    async fn create_table_reader(
        config: ExternalTableConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
        table_id: u32,
    ) -> ConnectorResult<PostgresExternalTableReader> {
        PostgresExternalTableReader::new(config, schema, pk_indices, schema_table_name, table_id)
            .await
    }

    async fn connect_external_table(
        config: ExternalTableConfig,
    ) -> ConnectorResult<Box<dyn ExternalTable>> {
        let config = config.with_specific::<PostgresTableConfig>()?;
        let table = PostgresExternalTable::connect(
            &config.pg_connection_config()?,
            &config.schema,
            &config.table,
            false,
            Some("SELECT"),
        )
        .await?;
        Ok(Box::new(table))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PostgresOffset {
    pub txid: i64,
    // In postgres, an LSN is a 64-bit integer, representing a byte position in the write-ahead log stream.
    // It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash; for example, 16/B374D848
    pub lsn: u64,
    // Additional LSN fields for improved tracking
    #[serde(default)]
    pub lsn_commit: Option<u64>,
    #[serde(default)]
    pub lsn_proc: Option<u64>,
}

impl PartialOrd for PostgresOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for PostgresOffset {}
impl PartialEq for PostgresOffset {
    fn eq(&self, other: &Self) -> bool {
        match (
            self.lsn_commit,
            self.lsn_proc,
            other.lsn_commit,
            other.lsn_proc,
        ) {
            (_, Some(_), _, Some(_)) => {
                self.lsn_commit == other.lsn_commit && self.lsn_proc == other.lsn_proc
            }
            _ => self.lsn == other.lsn,
        }
    }
}

// only compare the lsn field, prefer lsn_commit and lsn_proc if both available
impl Ord for PostgresOffset {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.lsn_commit,
            self.lsn_proc,
            other.lsn_commit,
            other.lsn_proc,
        ) {
            (_, Some(self_proc), _, Some(other_proc)) => {
                // if both have `lsn_commit` and `lsn_proc`, compare `lsn_commit` first, then `lsn_proc`
                // if `lsn_commit` is None, fall back to `lsn_proc`
                match self.lsn_commit.cmp(&other.lsn_commit) {
                    Ordering::Equal => self_proc.cmp(&other_proc),
                    other_result => other_result,
                }
            }
            _ => {
                // Fall back to lsn comparison when either lsn_commit or lsn_proc is missing
                static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                    LazyLock::new(LogSuppressor::default);
                if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                    tracing::warn!(
                        suppressed_count,
                        self_lsn = self.lsn,
                        other_lsn = other.lsn,
                        "lsn_commit and lsn_proc are missing, fall back to lsn comparison"
                    );
                }
                self.lsn.cmp(&other.lsn)
            }
        }
    }
}

/// `sourceOffset` of a Postgres Debezium offset.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PostgresSourceOffset {
    pub last_snapshot_record: Option<bool>,
    pub lsn: Option<u64>,
    #[serde(rename = "txId")]
    pub txid: Option<i64>,
    pub lsn_commit: Option<u64>,
    pub lsn_proc: Option<u64>,
}

impl CdcDebeziumOffsetInterpreter for Postgres {
    type Offset = PostgresOffset;
    type SourceOffset = PostgresSourceOffset;

    /// Debezium marks the last snapshot record with `last_snapshot_record: true`.
    fn snapshot_done_from_debezium_offset(offset: &DebeziumOffset<PostgresSourceOffset>) -> bool {
        offset.source_offset.last_snapshot_record.unwrap_or(false)
    }

    fn offset_from_debezium_offset(
        offset: DebeziumOffset<PostgresSourceOffset>,
    ) -> ConnectorResult<PostgresOffset> {
        let lsn = offset.source_offset.lsn.context("invalid postgres lsn")?;

        // `lsn_commit` may not be present in the offset for the first tx.
        let lsn_commit = offset.source_offset.lsn_commit;

        let lsn_proc = offset
            .source_offset
            .lsn_proc
            .context("invalid postgres lsn_proc")?;

        Ok(PostgresOffset {
            txid: offset.source_offset.txid.context("invalid postgres txid")?,
            lsn,
            lsn_commit,
            lsn_proc: Some(lsn_proc),
        })
    }
}

pub struct PostgresExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    pk_indices: Vec<usize>,
    client: tokio::sync::Mutex<tokio_postgres::Client>,
    schema_table_name: SchemaTableName,
}

impl ExternalTableReader for PostgresExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut client = self.client.lock().await;
        // start a transaction to read current lsn and txid
        let trxn = client.transaction().await?;
        let row = trxn.query_one("SELECT pg_current_wal_lsn()", &[]).await?;
        let mut pg_offset = PostgresOffset::default();
        let pg_lsn = row.get::<_, PgLsn>(0);
        tracing::debug!("current lsn: {}", pg_lsn);
        pg_offset.lsn = pg_lsn.into();

        let txid_row = trxn.query_one("SELECT txid_current()", &[]).await?;
        let txid: i64 = txid_row.get::<_, i64>(0);
        pg_offset.txid = txid;

        // commit the transaction
        trxn.commit().await?;

        Ok(CdcOffset::Postgres(pg_offset))
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        assert_eq!(table_name, self.schema_table_name);
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits(&self, options: CdcTableSnapshotSplitOption) {
        let backfill_num_rows_per_split = options.backfill_num_rows_per_split;
        if backfill_num_rows_per_split == 0 {
            return Err(anyhow::anyhow!(
                "invalid backfill_num_rows_per_split, must be greater than 0"
            )
            .into());
        }
        if options.backfill_split_pk_column_index as usize >= self.pk_indices.len() {
            return Err(anyhow::anyhow!(format!(
                "invalid backfill_split_pk_column_index {}, out of bound",
                options.backfill_split_pk_column_index
            ))
            .into());
        }
        let split_column = self.split_column(&options);
        let row_stream = if options.backfill_as_even_splits
            && is_supported_even_split_data_type(&split_column.data_type)
        {
            // For certain types, use evenly-sized partition to optimize performance.
            tracing::info!(?self.schema_table_name, ?self.rw_schema, ?self.pk_indices, ?split_column, "Get parallel cdc table snapshot even splits.");
            self.as_even_splits(options)
        } else {
            tracing::info!(?self.schema_table_name, ?self.rw_schema, ?self.pk_indices, ?split_column, "Get parallel cdc table snapshot uneven splits.");
            self.as_uneven_splits(options)
        };
        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        assert_eq!(table_name, self.schema_table_name);
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl PostgresExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
        table_id: u32,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create postgres external table reader"
        );
        let config = config.with_specific::<PostgresTableConfig>()?;
        // No TCP keepalive for CDC source
        let application_name = format!(
            "risingwave-postgres-source-reader-{}-{}.{}",
            table_id, schema_table_name.schema_name, schema_table_name.table_name
        );
        let client = create_pg_client(
            &config.pg_connection_config()?,
            None,
            Some(&application_name),
        )
        .await?;

        // Discover user-defined composite columns and arrays of composites.
        // tokio-postgres cannot decode composite values natively, so for these
        // columns we cast to text in the snapshot SELECT. Scalar composites
        // become `(a,b,c)`; arrays become `text[]` so tokio-postgres can
        // decode them directly as `Vec<Option<String>>` matching the RW
        // `List(Varchar)` column. Other varchar columns stay as-is to
        // preserve RW's own text rendering (e.g. numeric -> "NaN").
        let composite_columns = client
            .query(
                "SELECT a.attname, (t.typcategory = 'A') AS is_array \
                 FROM pg_attribute a \
                 JOIN pg_class c ON a.attrelid = c.oid \
                 JOIN pg_namespace n ON c.relnamespace = n.oid \
                 JOIN pg_type t ON a.atttypid = t.oid \
                 LEFT JOIN pg_type t_elem ON t.typelem = t_elem.oid \
                 WHERE n.nspname = $1 \
                   AND c.relname = $2 \
                   AND a.attnum > 0 \
                   AND NOT a.attisdropped \
                   AND (t.typtype = 'c' \
                        OR (t.typcategory = 'A' AND t_elem.typtype = 'c'))",
                &[
                    &schema_table_name.schema_name,
                    &schema_table_name.table_name,
                ],
            )
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1)))
                    .collect::<std::collections::HashMap<_, _>>()
            })
            .unwrap_or_else(|err| {
                tracing::warn!(
                    error = %err.as_report(),
                    schema = %schema_table_name.schema_name,
                    table = %schema_table_name.table_name,
                    "failed to discover postgres composite columns; falling back to no text cast"
                );
                std::collections::HashMap::new()
            });

        let field_names = rw_schema
            .fields
            .iter()
            .map(|f| {
                let quoted = Self::quote_column(&f.name);
                match composite_columns.get(&f.name) {
                    Some(false) if matches!(f.data_type, DataType::Varchar) => {
                        format!("{quoted}::text AS {quoted}")
                    }
                    Some(true) if matches!(f.data_type, DataType::List(_)) => {
                        format!(
                            "CASE WHEN {quoted} IS NULL THEN NULL \
                             ELSE ARRAY(SELECT t::text FROM unnest({quoted}) t)::text[] \
                             END AS {quoted}"
                        )
                    }
                    _ => quoted,
                }
            })
            .join(",");

        Ok(Self {
            rw_schema,
            field_names,
            pk_indices,
            client: tokio::sync::Mutex::new(client),
            schema_table_name,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::Postgres(parse_debezium_offset::<Postgres>(
                offset,
            )?))
        })
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        scan_limit: u32,
    ) {
        let order_key = Self::get_order_key(&primary_keys);
        let client = self.client.lock().await;
        client.execute("set time zone '+00:00'", &[]).await?;

        let stream = match start_pk_row {
            Some(ref pk_row) => {
                // prepare the scan statement, since we may need to convert the RW data type to postgres data type
                // e.g. varchar to uuid
                let prepared_scan_stmt = {
                    let primary_keys = self
                        .pk_indices
                        .iter()
                        .map(|i| self.rw_schema.fields[*i].name.clone())
                        .collect_vec();

                    let order_key = Self::get_order_key(&primary_keys);
                    let scan_sql = format!(
                        "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {scan_limit}",
                        self.field_names,
                        Self::get_normalized_table_name(&table_name),
                        Self::filter_expression(&primary_keys),
                        order_key,
                    );
                    client.prepare(&scan_sql).await?
                };

                let params: Vec<Option<ScalarAdapter>> = pk_row
                    .iter()
                    .zip_eq_fast(prepared_scan_stmt.params())
                    .map(|(datum, ty)| {
                        datum
                            .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                            .transpose()
                    })
                    .try_collect()?;

                client.query_raw(&prepared_scan_stmt, &params).await?
            }
            None => {
                let sql = format!(
                    "SELECT {} FROM {} ORDER BY {} LIMIT {scan_limit}",
                    self.field_names,
                    Self::get_normalized_table_name(&table_name),
                    order_key,
                );
                let params: Vec<Option<ScalarAdapter>> = vec![];
                client.query_raw(&sql, &params).await?
            }
        };

        let row_stream = stream.map(|row| {
            let row = row?;
            postgres_row_to_owned_row_with_strict_pk(row, &self.rw_schema, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    // row filter expression: (v1, v2, v3) > ($1, $2, $3)
    fn filter_expression(columns: &[String]) -> String {
        let mut col_expr = String::new();
        let mut arg_expr = String::new();
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                col_expr.push_str(", ");
                arg_expr.push_str(", ");
            }
            col_expr.push_str(&Self::quote_column(column));
            arg_expr.push_str(format!("${}", i + 1).as_str());
        }
        format!("({}) > ({})", col_expr, arg_expr)
    }

    // row filter expression: (v1, v2, v3) >= ($1, $2, $3) AND (v1, v2, v3) < ($1, $2, $3)
    fn split_filter_expression(
        columns: &[String],
        is_first_split: bool,
        is_last_split: bool,
    ) -> String {
        let mut left_col_expr = String::new();
        let mut left_arg_expr = String::new();
        let mut right_col_expr = String::new();
        let mut right_arg_expr = String::new();
        let mut c = 1;
        if !is_first_split {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    left_col_expr.push_str(", ");
                    left_arg_expr.push_str(", ");
                }
                left_col_expr.push_str(&Self::quote_column(column));
                left_arg_expr.push_str(format!("${}", c).as_str());
                c += 1;
            }
        }
        if !is_last_split {
            for (i, column) in columns.iter().enumerate() {
                if i > 0 {
                    right_col_expr.push_str(", ");
                    right_arg_expr.push_str(", ");
                }
                right_col_expr.push_str(&Self::quote_column(column));
                right_arg_expr.push_str(format!("${}", c).as_str());
                c += 1;
            }
        }
        if is_first_split && is_last_split {
            "1 = 1".to_owned()
        } else if is_first_split {
            format!("({}) < ({})", right_col_expr, right_arg_expr,)
        } else if is_last_split {
            format!("({}) >= ({})", left_col_expr, left_arg_expr,)
        } else {
            format!(
                "({}) >= ({}) AND ({}) < ({})",
                left_col_expr, left_arg_expr, right_col_expr, right_arg_expr,
            )
        }
    }

    fn get_order_key(primary_keys: &Vec<String>) -> String {
        primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",")
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column)
    }

    async fn min_and_max(
        &self,
        split_column: &Field,
    ) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let sql = format!(
            "SELECT MIN({}), MAX({}) FROM {}",
            split_column.name,
            split_column.name,
            Self::get_normalized_table_name(&self.schema_table_name),
        );
        let client = self.client.lock().await;
        let rows = client.query(&sql, &[]).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let row = &rows[0];
            let min = postgres_cell_to_scalar_impl_strict(
                row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?;
            let max = postgres_cell_to_scalar_impl_strict(
                row,
                &split_column.data_type,
                1,
                &split_column.name,
            )?;
            match (min, max) {
                (Some(min), Some(max)) => Ok(Some((min, max))),
                _ => Ok(None),
            }
        }
    }

    async fn next_split_right_bound_exclusive(
        &self,
        left_value: &ScalarImpl,
        max_value: &ScalarImpl,
        max_split_size: u64,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "WITH t as (SELECT {} FROM {} WHERE {} >= $1 ORDER BY {} ASC LIMIT {}) SELECT CASE WHEN MAX({}) < $2 THEN MAX({}) ELSE NULL END FROM t",
            Self::quote_column(&split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
            max_split_size,
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
        );
        let client = self.client.lock().await;
        let prepared_stmt = client.prepare(&sql).await?;
        let params: Vec<Option<ScalarAdapter>> = vec![
            Some(ScalarAdapter::from_scalar(
                left_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[0],
            )?),
            Some(ScalarAdapter::from_scalar(
                max_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[1],
            )?),
        ];
        let stream = client.query_raw(&prepared_stmt, &params).await?;
        let datum_stream = stream.map(|row| {
            let row = row?;
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl_strict(
                &row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?)
        });
        pin_mut!(datum_stream);
        #[for_await]
        for datum in datum_stream {
            let right = datum?;
            return Ok(Some(right.to_owned_datum()));
        }
        Ok(None)
    }

    async fn next_greater_bound(
        &self,
        start_offset: &ScalarImpl,
        max_value: &ScalarImpl,
        split_column: &Field,
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "SELECT MIN({}) FROM {} WHERE {} > $1 AND {} <$2",
            Self::quote_column(&split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&split_column.name),
            Self::quote_column(&split_column.name),
        );
        let client = self.client.lock().await;
        let prepared_stmt = client.prepare(&sql).await?;
        let params: Vec<Option<ScalarAdapter>> = vec![
            Some(ScalarAdapter::from_scalar(
                start_offset.as_scalar_ref_impl(),
                &prepared_stmt.params()[0],
            )?),
            Some(ScalarAdapter::from_scalar(
                max_value.as_scalar_ref_impl(),
                &prepared_stmt.params()[1],
            )?),
        ];
        let stream = client.query_raw(&prepared_stmt, &params).await?;
        let datum_stream = stream.map(|row| {
            let row = row?;
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl_strict(
                &row,
                &split_column.data_type,
                0,
                &split_column.name,
            )?)
        });
        pin_mut!(datum_stream);
        #[for_await]
        for datum in datum_stream {
            let right = datum?;
            return Ok(Some(right));
        }
        Ok(None)
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn split_snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) {
        // Conceptually, the query is:
        //
        // SELECT <selected_columns>
        // FROM <upstream_table>
        // WHERE <split_filter>
        //
        // `<split_filter>` is exactly one of:
        // - `1 = 1` when both bounds contain the unbounded `NULL` sentinel;
        // - `(<split_columns>) < (<right_bound_params>)` for the first split;
        // - `(<split_columns>) >= (<left_bound_params>)` for the last split;
        // - `(<split_columns>) >= (<left_bound_params>) AND
        //    (<split_columns>) < (<right_bound_params>)` for a middle split.
        //
        // Bound values are bound in placeholder order: left, then right.
        assert_eq!(
            split_columns.len(),
            1,
            "multiple split columns is not supported yet"
        );
        assert_eq!(left.len(), 1, "multiple split columns is not supported yet");
        assert_eq!(
            right.len(),
            1,
            "multiple split columns is not supported yet"
        );
        let is_first_split = left[0].is_none();
        let is_last_split = right[0].is_none();
        let split_column_names = split_columns.iter().map(|c| c.name.clone()).collect_vec();
        let client = self.client.lock().await;
        client.execute("set time zone '+00:00'", &[]).await?;
        // prepare the scan statement, since we may need to convert the RW data type to postgres data type
        // e.g. varchar to uuid
        let prepared_scan_stmt = {
            let scan_sql = format!(
                "SELECT {} FROM {} WHERE {}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                Self::split_filter_expression(&split_column_names, is_first_split, is_last_split),
            );
            client.prepare(&scan_sql).await?
        };

        let mut params: Vec<Option<ScalarAdapter>> = vec![];
        if !is_first_split {
            let left_params: Vec<Option<ScalarAdapter>> = left
                .iter()
                .zip_eq_fast(prepared_scan_stmt.params().iter().take(left.len()))
                .map(|(datum, ty)| {
                    datum
                        .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                        .transpose()
                })
                .try_collect()?;
            params.extend(left_params);
        }
        if !is_last_split {
            let right_params: Vec<Option<ScalarAdapter>> = right
                .iter()
                .zip_eq_fast(prepared_scan_stmt.params().iter().skip(params.len()))
                .map(|(datum, ty)| {
                    datum
                        .map(|scalar| ScalarAdapter::from_scalar(scalar, ty))
                        .transpose()
                })
                .try_collect()?;
            params.extend(right_params);
        }

        let stream = client.query_raw(&prepared_scan_stmt, &params).await?;
        let row_stream = stream.map(|row| {
            let row = row?;
            postgres_row_to_owned_row_with_strict_pk(row, &self.rw_schema, &self.pk_indices)
                .map_err(ConnectorError::from)
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_uneven_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);
        let mut split_id = CDC_TABLE_SPLIT_ID_START;
        let Some((min_value, max_value)) = self.min_and_max(&split_column).await? else {
            let left_bound_row = OwnedRow::new(vec![None]);
            let right_bound_row = OwnedRow::new(vec![None]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            yield split;
            return Ok(());
        };
        // left bound will never be NULL value.
        let mut next_left_bound_inclusive = min_value.clone();
        loop {
            let left_bound_inclusive: Datum = if next_left_bound_inclusive == min_value {
                None
            } else {
                Some(next_left_bound_inclusive.clone())
            };
            let right_bound_exclusive;
            let mut next_right = self
                .next_split_right_bound_exclusive(
                    &next_left_bound_inclusive,
                    &max_value,
                    options.backfill_num_rows_per_split,
                    &split_column,
                )
                .await?;
            if let Some(Some(ref inner)) = next_right
                && *inner == next_left_bound_inclusive
            {
                next_right = self
                    .next_greater_bound(&next_left_bound_inclusive, &max_value, &split_column)
                    .await?;
            }
            if let Some(next_right) = next_right {
                match next_right {
                    None => {
                        // NULL found.
                        right_bound_exclusive = None;
                    }
                    Some(next_right) => {
                        next_left_bound_inclusive = next_right.clone();
                        right_bound_exclusive = Some(next_right);
                    }
                }
            } else {
                // Not found.
                right_bound_exclusive = None;
            };
            let is_completed = right_bound_exclusive.is_none();
            if is_completed && left_bound_inclusive.is_none() {
                assert_eq!(split_id, 1);
            }
            tracing::info!(
                split_id,
                ?left_bound_inclusive,
                ?right_bound_exclusive,
                "New CDC table snapshot split."
            );
            let left_bound_row = OwnedRow::new(vec![left_bound_inclusive]);
            let right_bound_row = OwnedRow::new(vec![right_bound_exclusive]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            try_increase_split_id(&mut split_id)?;
            yield split;
            if is_completed {
                break;
            }
        }
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn as_even_splits(&self, options: CdcTableSnapshotSplitOption) {
        let split_column = self.split_column(&options);
        let mut split_id = 1;
        let Some((min_value, max_value)) = self.min_and_max(&split_column).await? else {
            let left_bound_row = OwnedRow::new(vec![None]);
            let right_bound_row = OwnedRow::new(vec![None]);
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: left_bound_row,
                right_bound_exclusive: right_bound_row,
            };
            yield split;
            return Ok(());
        };
        let min_value = min_value.as_integral();
        let max_value = max_value.as_integral();
        let saturated_split_max_size = options
            .backfill_num_rows_per_split
            .try_into()
            .unwrap_or(i64::MAX);
        let mut left = None;
        let mut right = Some(min_value.saturating_add(saturated_split_max_size));
        loop {
            let mut is_completed = false;
            if right.as_ref().map(|r| *r >= max_value).unwrap_or(true) {
                right = None;
                is_completed = true;
            }
            let split = CdcTableSnapshotSplit {
                split_id,
                left_bound_inclusive: OwnedRow::new(vec![
                    left.map(|l| to_int_scalar(l, &split_column.data_type)),
                ]),
                right_bound_exclusive: OwnedRow::new(vec![
                    right.map(|r| to_int_scalar(r, &split_column.data_type)),
                ]),
            };
            try_increase_split_id(&mut split_id)?;
            yield split;
            if is_completed {
                break;
            }
            left = right;
            right = left.map(|l| l.saturating_add(saturated_split_max_size));
        }
    }

    fn split_column(&self, options: &CdcTableSnapshotSplitOption) -> Field {
        self.rw_schema.fields[self.pk_indices[options.backfill_split_pk_column_index as usize]]
            .clone()
    }
}

fn to_int_scalar(i: i64, data_type: &DataType) -> ScalarImpl {
    match data_type {
        DataType::Int16 => ScalarImpl::Int16(i.try_into().unwrap()),
        DataType::Int32 => ScalarImpl::Int32(i.try_into().unwrap()),
        DataType::Int64 => ScalarImpl::Int64(i),
        _ => {
            panic!("Can't convert int {} to ScalarImpl::{}", i, data_type)
        }
    }
}

fn try_increase_split_id(split_id: &mut i64) -> ConnectorResult<()> {
    match split_id.checked_add(1) {
        Some(s) => {
            *split_id = s;
            Ok(())
        }
        None => Err(anyhow::anyhow!("too many CDC snapshot splits").into()),
    }
}

/// Use the first column of primary keys to split table.
fn is_supported_even_split_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub fn type_name_to_pg_type(ty_name: &str) -> Option<PgType> {
    let ty_name_lower = ty_name.to_lowercase();
    // Handle array types (prefixed with _)
    if let Some(base_type) = ty_name_lower.strip_prefix('_') {
        match base_type {
            "int2" => Some(PgType::INT2_ARRAY),
            "int4" => Some(PgType::INT4_ARRAY),
            "int8" => Some(PgType::INT8_ARRAY),
            "bit" => Some(PgType::BIT_ARRAY),
            "float4" => Some(PgType::FLOAT4_ARRAY),
            "float8" => Some(PgType::FLOAT8_ARRAY),
            "numeric" => Some(PgType::NUMERIC_ARRAY),
            "bool" => Some(PgType::BOOL_ARRAY),
            "xml" | "macaddr" | "macaddr8" | "cidr" | "inet" | "int4range" | "int8range"
            | "numrange" | "tsrange" | "tstzrange" | "daterange" | "citext" => {
                Some(PgType::VARCHAR_ARRAY)
            }
            "varchar" => Some(PgType::VARCHAR_ARRAY),
            "text" => Some(PgType::TEXT_ARRAY),
            "bytea" => Some(PgType::BYTEA_ARRAY),
            "geometry" | "geography" => Some(PgType::BYTEA_ARRAY), // PostGIS spatial arrays
            "date" => Some(PgType::DATE_ARRAY),
            "time" => Some(PgType::TIME_ARRAY),
            "timetz" => Some(PgType::TIMETZ_ARRAY),
            "timestamp" => Some(PgType::TIMESTAMP_ARRAY),
            "timestamptz" => Some(PgType::TIMESTAMPTZ_ARRAY),
            "interval" => Some(PgType::INTERVAL_ARRAY),
            "json" => Some(PgType::JSON_ARRAY),
            "jsonb" => Some(PgType::JSONB_ARRAY),
            "uuid" => Some(PgType::UUID_ARRAY),
            "point" => Some(PgType::POINT_ARRAY),
            "oid" => Some(PgType::OID_ARRAY),
            "money" => Some(PgType::MONEY_ARRAY),
            _ => None,
        }
    } else {
        // Handle non-array types
        match ty_name_lower.as_str() {
            "int2" => Some(PgType::INT2),
            "bit" => Some(PgType::BIT),
            "int" | "int4" => Some(PgType::INT4),
            "int8" => Some(PgType::INT8),
            "float4" => Some(PgType::FLOAT4),
            "float8" => Some(PgType::FLOAT8),
            "numeric" => Some(PgType::NUMERIC),
            "money" => Some(PgType::MONEY),
            "boolean" | "bool" => Some(PgType::BOOL),
            "inet" | "xml" | "varchar" | "character varying" | "int4range" | "int8range"
            | "numrange" | "tsrange" | "tstzrange" | "daterange" | "macaddr" | "macaddr8"
            | "cidr" => Some(PgType::VARCHAR),
            "char" | "character" | "bpchar" => Some(PgType::BPCHAR),
            "citext" | "text" => Some(PgType::TEXT),
            "bytea" => Some(PgType::BYTEA),
            "geometry" | "geography" => Some(PgType::BYTEA), // PostGIS spatial types
            "date" => Some(PgType::DATE),
            "time" => Some(PgType::TIME),
            "timetz" => Some(PgType::TIMETZ),
            "timestamp" => Some(PgType::TIMESTAMP),
            "timestamptz" => Some(PgType::TIMESTAMPTZ),
            "interval" => Some(PgType::INTERVAL),
            "json" => Some(PgType::JSON),
            "jsonb" => Some(PgType::JSONB),
            "uuid" => Some(PgType::UUID),
            "point" => Some(PgType::POINT),
            "oid" => Some(PgType::OID),
            _ => None,
        }
    }
}

// Keep this canonical mapping aligned with `postgres_source_column_type_compatible` in
// `src/common/src/catalog/cdc_type_compatibility.rs`, which validates user-declared RW column
// types.
pub fn pg_type_to_rw_type(pg_type: &PgType) -> ConnectorResult<DataType> {
    let data_type = match *pg_type {
        PgType::BOOL => DataType::Boolean,
        PgType::BIT => DataType::Boolean,
        PgType::INT2 => DataType::Int16,
        PgType::INT4 => DataType::Int32,
        PgType::INT8 => DataType::Int64,
        PgType::FLOAT4 => DataType::Float32,
        PgType::FLOAT8 => DataType::Float64,
        PgType::NUMERIC | PgType::MONEY => DataType::Decimal,
        PgType::DATE => DataType::Date,
        PgType::TIME => DataType::Time,
        PgType::TIMETZ => DataType::Time,
        PgType::POINT => postgres_point_type(),
        PgType::TIMESTAMP => DataType::Timestamp,
        PgType::TIMESTAMPTZ => DataType::Timestamptz,
        PgType::INTERVAL => DataType::Interval,
        PgType::VARCHAR | PgType::TEXT | PgType::BPCHAR | PgType::UUID => DataType::Varchar,
        PgType::BYTEA => DataType::Bytea,
        PgType::JSON | PgType::JSONB => DataType::Jsonb,
        // Array types
        PgType::BOOL_ARRAY => DataType::Boolean.list(),
        PgType::BIT_ARRAY => DataType::Boolean.list(),
        PgType::INT2_ARRAY => DataType::Int16.list(),
        PgType::INT4_ARRAY => DataType::Int32.list(),
        PgType::INT8_ARRAY => DataType::Int64.list(),
        PgType::FLOAT4_ARRAY => DataType::Float32.list(),
        PgType::FLOAT8_ARRAY => DataType::Float64.list(),
        PgType::NUMERIC_ARRAY => DataType::Decimal.list(),
        PgType::VARCHAR_ARRAY => DataType::Varchar.list(),
        PgType::TEXT_ARRAY => DataType::Varchar.list(),
        PgType::BYTEA_ARRAY => DataType::Bytea.list(),
        PgType::DATE_ARRAY => DataType::Date.list(),
        PgType::TIME_ARRAY => DataType::Time.list(),
        PgType::TIMESTAMP_ARRAY => DataType::Timestamp.list(),
        PgType::TIMESTAMPTZ_ARRAY => DataType::Timestamptz.list(),
        PgType::INTERVAL_ARRAY => DataType::Interval.list(),
        PgType::JSON_ARRAY => DataType::Jsonb.list(),
        PgType::JSONB_ARRAY => DataType::Jsonb.list(),
        PgType::UUID_ARRAY => DataType::Varchar.list(),
        PgType::OID => DataType::Int64,
        PgType::OID_ARRAY => DataType::Int64.list(),
        PgType::MONEY_ARRAY => DataType::Decimal.list(),
        // Debezium does not implement POINT_ARRAY schema conversion.
        // https://github.com/debezium/debezium/blob/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresValueConverter.java#L339-L348
        PgType::POINT_ARRAY => {
            return Err(anyhow::anyhow!("unsupported postgres type: {}", pg_type).into());
        }
        _ => {
            return Err(anyhow::anyhow!("unsupported postgres type: {}", pg_type).into());
        }
    };
    Ok(data_type)
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;
    use crate::connector_common::PostgresExternalTable;

    /// Build a streaming-phase PG offset JSON. Sets `lsn`, `lsn_proc`,
    /// and `lsn_commit` all to the same value -- this is the common
    /// shape for the guard's basic monotonic tests.
    fn pg_streaming_offset_json(lsn: u64) -> String {
        pg_streaming_offset_json_full(lsn, lsn, lsn)
    }

    /// Build a streaming-phase PG offset JSON with independent control
    /// over `lsn`, `lsn_proc`, and `lsn_commit`. Used to exercise
    /// transaction-interleave and asymmetric-regression scenarios.
    fn pg_streaming_offset_json_full(lsn: u64, lsn_proc: u64, lsn_commit: u64) -> String {
        format!(
            r#"{{
                "sourcePartition": {{"server": "RW_CDC_1001"}},
                "sourceOffset": {{
                    "last_snapshot_record": false,
                    "lsn": {lsn},
                    "lsn_proc": {lsn_proc},
                    "lsn_commit": {lsn_commit},
                    "txId": 12345,
                    "ts_usec": 1700000000000000
                }},
                "isHeartbeat": false
            }}"#
        )
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_rejects_backward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Push a backward LSN -- must be rejected, current offset kept.
        split
            .update_offset(pg_streaming_offset_json(150), true)
            .expect("update_offset must not error on rejection");
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_forward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        split
            .update_offset(pg_streaming_offset_json(250), true)
            .unwrap();
        assert_eq!(split.pg_lsn(), Some(250));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_equal_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Equal is allowed (no regression).
        split
            .update_offset(pg_streaming_offset_json(200), true)
            .unwrap();
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_snapshot_phase_bypasses_guard() {
        let snapshot_offset = r#"{
            "sourcePartition": {"server": "RW_CDC_1001"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 150,
                "txId": 12345,
                "ts_usec": 1700000000000000
            },
            "isHeartbeat": false
        }"#;
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Snapshot / incomplete offsets do not have a comparable streaming
        // `lsn_proc`, so they pass through even when the plain `lsn` is lower.
        split
            .update_offset(snapshot_offset.to_owned(), false)
            .unwrap();
        assert_eq!(split.pg_lsn(), Some(150));
    }

    #[test]
    fn test_postgres_offset_lsn_commit_regression_rejected() {
        // `lsn_commit` regressing alone must trigger reject -- this is
        // the field validateLogPosition checks, regression here is the
        // catastrophic case.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        // lsn_commit 200 -> 150, but lsn / lsn_proc stay at 200.
        let new = pg_streaming_offset_json_full(200, 200, 150);
        split.update_offset(new, true).unwrap();

        assert_eq!(
            split.pg_lsn_commit(),
            Some(200),
            "lsn_commit must not regress"
        );
    }

    #[test]
    fn test_postgres_offset_lsn_proc_regression_rejected() {
        // `lsn_proc` regressing alone must trigger reject -- this is the
        // field dbz uses as the startStreaming resume position, and
        // WalPositionLocator's dedup key on restart.
        let initial = pg_streaming_offset_json_full(200, 200, 200);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        // lsn_proc 200 -> 150, but lsn / lsn_commit stay at 200.
        let new = pg_streaming_offset_json_full(200, 150, 200);
        split.update_offset(new, true).unwrap();

        assert_eq!(split.pg_lsn_proc(), Some(200), "lsn_proc must not regress");
    }

    #[test]
    fn test_postgres_offset_interleaved_tx_lsn_proc_drop_allowed_by_commit_lsn() {
        // Concurrent overlapping transactions under PG logical
        // replication produce a state-table chunk where the per-event
        // `lsn` and `lsn_proc` move backwards while `lsn_commit`
        // strictly advances (because PG streams events in commit
        // order, and the earlier-begin transaction's INSERT has a
        // smaller physical LSN even though it commits later).
        //
        // Use the same Postgres offset ordering as #22503: compare
        // `lsn_commit` first, then `lsn_proc`. Since the commit LSN
        // advances, this update is not a regression.
        let initial = pg_streaming_offset_json_full(33_967_592, 33_967_592, 33_893_392);
        let mut split = PostgresCdcSplit::new(1, Some(initial));

        let new = pg_streaming_offset_json_full(33_918_336, 33_918_336, 33_969_832);
        split.update_offset(new, true).unwrap();

        assert_eq!(split.pg_lsn_commit(), Some(33_969_832));
        assert_eq!(split.pg_lsn_proc(), Some(33_918_336));
    }

    #[test]
    fn test_parse_postgres_cdc_external_table_name() {
        for (input, expected) in [
            ("public.Note", ("public", "Note")),
            ("public.\"Note\"", ("public", "Note")),
            (
                "\"Mixed.Schema\".\"Note.Table\"",
                ("Mixed.Schema", "Note.Table"),
            ),
            ("public.\"Note\"\"Archive\"", ("public", "Note\"Archive")),
        ] {
            assert_eq!(
                parse_postgres_cdc_external_table_name(input).unwrap(),
                (expected.0.to_owned(), expected.1.to_owned()),
                "input: {input}"
            );
        }

        for input in [
            "Note",
            "public.",
            ".Note",
            "public.\"Note",
            "public.\"Note\"Archive",
            "public.Note.Archive",
        ] {
            assert!(
                parse_postgres_cdc_external_table_name(input).is_err(),
                "input should be rejected: {input}"
            );
        }
    }

    #[test]
    fn test_fill_table_name_options_strips_quotes() {
        let mut options = BTreeMap::from([("database.name".to_owned(), "mydb".to_owned())]);
        let external_table_name =
            Postgres::fill_table_name_options(&mut options, r#"public."QuotedNote""#).unwrap();
        assert_eq!(external_table_name, "public.QuotedNote");
        assert_eq!(options.get(SCHEMA_NAME_KEY).unwrap(), "public");
        assert_eq!(options.get(TABLE_NAME_KEY).unwrap(), "QuotedNote");
    }

    #[ignore]
    #[tokio::test]
    async fn test_postgres_schema() {
        let config = ExternalTableConfig {
            connector: "postgres-cdc".to_owned(),
            host: "localhost".to_owned(),
            port: "8432".to_owned(),
            username: "myuser".to_owned(),
            password: "123456".to_owned(),
            database: "mydb".to_owned(),
            schema: "public".to_owned(),
            table: "mytest".to_owned(),
            specific: Default::default(),
        }
        .with_specific::<PostgresTableConfig>()
        .unwrap();

        let table = PostgresExternalTable::connect(
            &config.pg_connection_config().unwrap(),
            &config.schema,
            &config.table,
            false,
            Some("SELECT"),
        )
        .await
        .unwrap();

        println!("columns: {:?}", table.column_descs());
        println!("primary keys: {:?}", table.pk_names());
    }

    #[test]
    fn test_postgres_offset() {
        let off1 = PostgresOffset {
            txid: 4,
            lsn: 2,
            ..Default::default()
        };
        let off2 = PostgresOffset {
            txid: 1,
            lsn: 3,
            ..Default::default()
        };
        let off3 = PostgresOffset {
            txid: 5,
            lsn: 1,
            ..Default::default()
        };

        assert!(off1 < off2);
        assert!(off3 < off1);
        assert!(off2 > off3);
    }

    #[test]
    fn test_postgres_offset_partial_ord_with_lsn_commit() {
        // Test comparison with both lsn_commit and lsn_proc fields
        let off1 = PostgresOffset {
            txid: 1,
            lsn: 100,
            lsn_commit: Some(200),
            lsn_proc: Some(150),
        };
        let off2 = PostgresOffset {
            txid: 2,
            lsn: 300,
            lsn_commit: Some(250),
            lsn_proc: Some(200),
        };

        // Should compare using lsn_commit first when both have both fields
        assert!(off1 < off2);

        // Test with same lsn_commit but different lsn_proc
        let off3 = PostgresOffset {
            txid: 3,
            lsn: 500,
            lsn_commit: Some(200), // same as off1
            lsn_proc: Some(160),   // higher than off1
        };

        // Should compare lsn_proc when lsn_commit is equal
        assert!(off1 < off3);

        // Test with missing lsn_proc - should fall back to lsn comparison
        let off4 = PostgresOffset {
            txid: 4,
            lsn: 400,
            lsn_commit: Some(100), // lower than off1's lsn_commit
            lsn_proc: None,        // missing lsn_proc
        };

        // Should fall back to lsn comparison (off1.lsn=100 < off4.lsn=400)
        assert!(off1 < off4);

        // Test with missing lsn_commit - should fall back to lsn comparison
        let off5 = PostgresOffset {
            txid: 5,
            lsn: 50,             // lower than off1.lsn
            lsn_commit: None,    // missing lsn_commit
            lsn_proc: Some(300), // higher than off1's lsn_proc
        };

        // Should fall back to lsn comparison (off5.lsn=50 < off1.lsn=100)
        assert!(off5 < off1);

        // Additional test cases: equal lsn_commit values with different lsn_proc
        let off6 = PostgresOffset {
            txid: 6,
            lsn: 600,
            lsn_commit: Some(500),
            lsn_proc: Some(300),
        };
        let off7 = PostgresOffset {
            txid: 7,
            lsn: 700,
            lsn_commit: Some(500), // same as off6
            lsn_proc: Some(400),   // higher than off6
        };

        // Should compare lsn_proc since lsn_commit is equal
        assert!(off6 < off7);

        // Test reverse order
        let off8 = PostgresOffset {
            txid: 8,
            lsn: 800,
            lsn_commit: Some(500), // same as others
            lsn_proc: Some(200),   // lower than off6
        };

        assert!(off8 < off6);
        assert!(off8 < off7);

        // Test equal lsn_commit and lsn_proc
        let off9 = PostgresOffset {
            txid: 9,
            lsn: 900,
            lsn_commit: Some(500), // same as off6
            lsn_proc: Some(300),   // same as off6
        };

        // Should be equal
        assert_eq!(off6.partial_cmp(&off9), Some(Ordering::Equal));
    }

    #[test]
    fn test_debezium_offset_parsing() {
        // Test parsing with all required fields present
        let debezium_offset_with_fields = r#"{
            "sourcePartition": {"server": "RW_CDC_1004"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 29973552,
                "txId": 1046,
                "ts_usec": 1670826189008456,
                "snapshot": true,
                "lsn_commit": 29973600,
                "lsn_proc": 29973580
            },
            "isHeartbeat": false
        }"#;

        let offset = parse_debezium_offset::<Postgres>(debezium_offset_with_fields).unwrap();
        assert_eq!(offset.txid, 1046);
        assert_eq!(offset.lsn, 29973552);
        assert_eq!(offset.lsn_commit, Some(29973600));
        assert_eq!(offset.lsn_proc, Some(29973580));

        // Test parsing should fail when required fields are missing
        let debezium_offset_missing_fields = r#"{
            "sourcePartition": {"server": "RW_CDC_1004"},
            "sourceOffset": {
                "last_snapshot_record": false,
                "lsn": 29973552,
                "txId": 1046,
                "ts_usec": 1670826189008456,
                "snapshot": true
            },
            "isHeartbeat": false
        }"#;

        let result = parse_debezium_offset::<Postgres>(debezium_offset_missing_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid postgres lsn_proc"));
    }

    #[test]
    fn test_filter_expression() {
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\") > ($1)");

        let cols = vec!["v1".to_owned(), "v2".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\", \"v2\") > ($1, $2)");

        let cols = vec!["v1".to_owned(), "v2".to_owned(), "v3".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\", \"v2\", \"v3\") > ($1, $2, $3)");
    }

    #[test]
    fn test_split_filter_expression() {
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::split_filter_expression(&cols, true, true);
        assert_eq!(expr, "1 = 1");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, true, false);
        assert_eq!(expr, "(\"v1\") < ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, false, true);
        assert_eq!(expr, "(\"v1\") >= ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(&cols, false, false);
        assert_eq!(expr, "(\"v1\") >= ($1) AND (\"v1\") < ($2)");
    }

    // manual test
    #[ignore]
    #[tokio::test]
    async fn test_pg_table_reader() {
        let columns = [
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Decimal),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };

        let props: HashMap<String, String> = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8432",
                "username" => "myuser",
                "password" => "123456",
                "database.name" => "mydb",
                "schema.name" => "public",
                "table.name" => "t1"));

        let config =
            serde_json::from_value::<ExternalTableConfig>(serde_json::to_value(props).unwrap())
                .unwrap();
        let schema_table_name = SchemaTableName {
            schema_name: "public".to_owned(),
            table_name: "t1".to_owned(),
        };
        let reader = PostgresExternalTableReader::new(
            config,
            rw_schema,
            vec![0, 1],
            schema_table_name.clone(),
            233,
        )
        .await
        .unwrap();

        let offset = reader.current_cdc_offset().await.unwrap();
        println!("CdcOffset: {:?}", offset);

        let start_pk = OwnedRow::new(vec![Some(ScalarImpl::from(3)), Some(ScalarImpl::from("c"))]);
        let stream = reader.snapshot_read(
            schema_table_name,
            Some(start_pk),
            vec!["v1".to_owned(), "v2".to_owned()],
            1000,
        );

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
