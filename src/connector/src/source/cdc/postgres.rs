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

use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::connector_common::{create_pg_client_from_properties, discover_pgvector_dimensions};
use crate::error::ConnectorResult;
use crate::parser::schema_change::{TableChangeColumn, TableChangeContext};
use crate::parser::{
    AccessError, AccessResult, DebeziumTransactionStatus, TransactionControl, jsonb_access_field,
};
use crate::source::cdc::external::postgres::{
    PostgresOffset, pg_type_to_rw_type, type_name_to_pg_type,
};
use crate::source::cdc::external::{
    DebeziumOffset, SCHEMA_NAME_KEY, SchemaTableName, TABLE_NAME_KEY,
};
use crate::source::cdc::split::{CdcSplitBase, CdcSplitTrait, DebeziumCdcSplit};
use crate::source::cdc::{
    CdcDebeziumPayloadInterpreter, CdcExternalTableNameParser, CdcSerializedFieldNames,
    CdcSourceFrontendValidator, CdcSourceRuntimeFlags, Postgres,
    db_name_end_and_table_name_start_from_schema_qualified_topic_suffix,
};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Postgres {
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

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        let new_snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;

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

        self.inner.snapshot_done = new_snapshot_done;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }

    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
            format!(
                "invalid postgres offset: {}, split: {}",
                start_offset, self.inner.split_id
            )
        })?;

        // heartbeat event should not update the `snapshot_done` flag
        if !dbz_offset.is_heartbeat {
            snapshot_done = dbz_offset
                .source_offset
                .last_snapshot_record
                .unwrap_or(false);
        }
        Ok(snapshot_done)
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
    PostgresOffset::parse_debezium_offset(offset_str).ok()
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

#[cfg(test)]
mod tests {
    use super::*;

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
            .update_offset(pg_streaming_offset_json(150))
            .expect("update_offset must not error on rejection");
        assert_eq!(split.pg_lsn(), Some(200));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_forward_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        split.update_offset(pg_streaming_offset_json(250)).unwrap();
        assert_eq!(split.pg_lsn(), Some(250));
    }

    #[test]
    fn test_postgres_offset_monotonic_guard_allows_equal_lsn() {
        let mut split = PostgresCdcSplit::new(1, Some(pg_streaming_offset_json(200)));

        // Equal is allowed (no regression).
        split.update_offset(pg_streaming_offset_json(200)).unwrap();
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
        split.update_offset(snapshot_offset.to_owned()).unwrap();
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
        split.update_offset(new).unwrap();

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
        split.update_offset(new).unwrap();

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
        split.update_offset(new).unwrap();

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
}
