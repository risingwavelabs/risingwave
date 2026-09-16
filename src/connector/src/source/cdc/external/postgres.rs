// Copyright 2023 RisingWave Labs
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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio_postgres::types::{PgLsn, Type as PgType};

use crate::connector_common::create_pg_client;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::{
    postgres_cell_to_scalar_impl_strict, postgres_row_to_owned_row_with_strict_pk,
};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CDC_TABLE_SPLIT_ID_START, CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption,
    DebeziumOffset, ExternalTableConfig, ExternalTableReader, SchemaTableName,
};

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

impl PostgresOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        let lsn = dbz_offset
            .source_offset
            .lsn
            .context("invalid postgres lsn")?;

        // `lsn_commit` may not be present in the offset for the first tx.
        let lsn_commit = dbz_offset.source_offset.lsn_commit;

        let lsn_proc = dbz_offset
            .source_offset
            .lsn_proc
            .context("invalid postgres lsn_proc")?;

        Ok(Self {
            txid: dbz_offset
                .source_offset
                .txid
                .context("invalid postgres txid")?,
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
    /// Per-column bytewise ordering shared by index validation and every SQL range/order
    /// expression. Native C/POSIX collations retain their identity, including database default.
    pk_ordering: HashMap<String, PostgresTextOrdering>,
    client: tokio::sync::Mutex<tokio_postgres::Client>,
    schema_table_name: SchemaTableName,
}

#[derive(Debug, Clone)]
struct PostgresIndexKey {
    column_name: Option<String>,
    collation: Option<PostgresCollation>,
    descending: bool,
    nulls_first: bool,
    default_opclass: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostgresCollation {
    schema: String,
    name: String,
}

impl PostgresCollation {
    fn from_catalog(schema: Option<String>, name: Option<String>) -> ConnectorResult<Option<Self>> {
        match (schema, name) {
            (Some(schema), Some(name)) => Ok(Some(Self { schema, name })),
            (None, None) => Ok(None),
            _ => Err(anyhow::anyhow!("incomplete PostgreSQL collation catalog metadata").into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostgresTextOrdering {
    collation: PostgresCollation,
    use_native: bool,
}

impl PostgresTextOrdering {
    fn explicit_c() -> Self {
        Self {
            collation: PostgresCollation {
                schema: "pg_catalog".into(),
                name: "C".into(),
            },
            use_native: false,
        }
    }
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
    async fn discover_binary_collated_pk_columns(
        client: &tokio_postgres::Client,
        table_name: &SchemaTableName,
        primary_keys: &[String],
        bypass_pk_order_validation: bool,
    ) -> ConnectorResult<HashSet<String>> {
        if primary_keys.is_empty() {
            return Ok(HashSet::new());
        }

        let column_type_oids = client
            .query(
                "SELECT a.attname, a.atttypid, typ.typtype::text, typ.typcategory::text, typ.typname \
                 FROM pg_attribute a \
                 JOIN pg_type typ ON typ.oid = a.atttypid \
                 JOIN pg_class tbl ON tbl.oid = a.attrelid \
                 JOIN pg_namespace ns ON ns.oid = tbl.relnamespace \
                 WHERE ns.nspname = $1 AND tbl.relname = $2 \
                   AND a.attnum > 0 AND NOT a.attisdropped",
                &[&table_name.schema_name, &table_name.table_name],
            )
            .await?
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, String>(0),
                    (
                        row.get::<_, u32>(1),
                        row.get::<_, String>(2),
                        row.get::<_, String>(3),
                        row.get::<_, String>(4),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();

        Self::binary_collated_pk_columns(
            &column_type_oids,
            table_name,
            primary_keys,
            bypass_pk_order_validation,
        )
    }

    async fn discover_pk_ordering(
        client: &tokio_postgres::Client,
        table_name: &SchemaTableName,
        primary_keys: &[String],
        bypass_pk_order_validation: bool,
    ) -> ConnectorResult<HashMap<String, PostgresTextOrdering>> {
        let text_columns = Self::discover_binary_collated_pk_columns(
            client,
            table_name,
            primary_keys,
            bypass_pk_order_validation,
        )
        .await?;
        if text_columns.is_empty() {
            return Ok(HashMap::new());
        }

        // With PostgreSQL's default privileges, these catalogs are readable by PUBLIC and
        // pg_catalog and the functions below are accessible without extra grants. This lookup
        // needs no additional CDC privileges unless an administrator revoked those defaults.
        // datlocprovider was added in PG 15. Earlier databases always use libc.
        // Do not infer a default ICU database's ordering from its libc LC_COLLATE.
        let rows = client
            .query(
                "SELECT a.attname, coll_ns.nspname, coll.collname, \
                    CASE WHEN coll.collprovider = 'd' \
                         THEN COALESCE(to_jsonb(db)->>'datlocprovider', 'c') \
                         ELSE coll.collprovider::text END, \
                    CASE WHEN coll.collprovider = 'd' THEN db.datcollate \
                         ELSE coll.collcollate END \
             FROM pg_attribute a \
             JOIN pg_class tbl ON tbl.oid = a.attrelid \
             JOIN pg_namespace ns ON ns.oid = tbl.relnamespace \
             JOIN pg_collation coll ON coll.oid = a.attcollation \
             JOIN pg_namespace coll_ns ON coll_ns.oid = coll.collnamespace \
             JOIN pg_database db ON db.datname = current_database() \
             WHERE ns.nspname = $1 AND tbl.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped",
                &[&table_name.schema_name, &table_name.table_name],
            )
            .await?;
        let mut ordering = HashMap::new();
        for row in rows {
            let column: String = row.try_get(0)?;
            if !text_columns.contains(&column) {
                continue;
            }
            let provider: String = row.try_get(3)?;
            let locale: Option<String> = row.try_get(4)?;
            let column_ordering = if Self::is_bytewise_collation(&provider, locale.as_deref()) {
                PostgresTextOrdering {
                    collation: PostgresCollation {
                        schema: row.try_get(1)?,
                        name: row.try_get(2)?,
                    },
                    use_native: true,
                }
            } else {
                PostgresTextOrdering::explicit_c()
            };
            ordering.insert(column, column_ordering);
        }
        for column in text_columns {
            if !ordering.contains_key(&column) {
                return Err(anyhow::anyhow!(
                    "PostgreSQL system catalog did not return collation for primary-key column `{column}`"
                ).into());
            }
        }
        Ok(ordering)
    }

    fn is_bytewise_collation(provider: &str, locale: Option<&str>) -> bool {
        // Only proven libc bytewise locales are reused. Other providers/locales use explicit C.
        provider == "c" && matches!(locale, Some("C" | "POSIX"))
    }

    fn binary_collated_pk_columns(
        column_types: &HashMap<String, (u32, String, String, String)>,
        table_name: &SchemaTableName,
        primary_keys: &[String],
        bypass_pk_order_validation: bool,
    ) -> ConnectorResult<HashSet<String>> {
        let mut binary_collated_columns = HashSet::new();
        for column_name in primary_keys {
            let (type_oid, type_kind, type_category, type_name) = column_types.get(column_name).ok_or_else(|| {
                anyhow::anyhow!(
                    "PostgreSQL system catalog did not return primary-key column `{column_name}` \
                     from table {}",
                    Self::get_normalized_table_name(table_name)
                )
            })?;
            if PgType::from_oid(*type_oid)
                .is_some_and(|pg_type| Self::is_binary_collated_pk_type(&pg_type))
            {
                binary_collated_columns.insert(column_name.clone());
            }
            if !bypass_pk_order_validation
                && let Some(reason) =
                    Self::unsupported_pk_type_reason(*type_oid, type_kind, type_category, type_name)
            {
                return Err(anyhow::anyhow!(
                    "PostgreSQL CDC primary-key column `{column_name}` has type OID {type_oid}, \
                     which is not supported because {reason}; set bypass_pk_order_validation=true \
                     to bypass this check"
                )
                .into());
            }
        }
        Ok(binary_collated_columns)
    }

    fn is_binary_collated_pk_type(pg_type: &PgType) -> bool {
        matches!(*pg_type, PgType::TEXT | PgType::VARCHAR)
    }

    /// Reject known ordering mismatches. Unknown types retain their existing decoding behavior.
    fn unsupported_pk_type_reason(
        type_oid: u32,
        type_kind: &str,
        type_category: &str,
        type_name: &str,
    ) -> Option<&'static str> {
        // Extension OIDs vary across databases. Match the name just as the string decoder does.
        if type_name == "citext" {
            return Some("citext case-insensitive ordering does not match decoded string ordering");
        }
        if type_kind == "e" {
            return Some("enum declaration order does not match decoded string ordering");
        }
        if type_category == "A" {
            return Some(
                "array dimension/lower-bound ordering is not preserved by the decoded list",
            );
        }
        match PgType::from_oid(type_oid) {
            Some(PgType::BPCHAR) => Some(
                "its blank-padding comparison semantics do not match RisingWave VARCHAR ordering",
            ),
            Some(PgType::JSONB) => {
                Some("its structural ordering does not match RisingWave JSONB ordering")
            }
            _ => None,
        }
    }

    async fn validate_server_encoding(client: &tokio_postgres::Client) -> ConnectorResult<()> {
        let encoding: String = client.query_one("SHOW server_encoding", &[]).await?.get(0);
        Self::check_server_encoding(&encoding)
    }

    fn check_server_encoding(encoding: &str) -> ConnectorResult<()> {
        if encoding.eq_ignore_ascii_case("UTF8") {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "PostgreSQL CDC TEXT/VARCHAR primary keys require server_encoding=UTF8 for \
                 deterministic C/binary ordering, but the upstream server uses `{encoding}`"
            )
            .into())
        }
    }

    /// Require an index that can satisfy every paginated snapshot query without rescanning and
    /// sorting the remaining table. Match native bytewise collations first, then select
    /// explicit C expressions where needed to use a compatible secondary index.
    async fn validate_cdc_ordering_index(
        client: &tokio_postgres::Client,
        table_name: &SchemaTableName,
        primary_keys: &[String],
        pk_ordering: &mut HashMap<String, PostgresTextOrdering>,
    ) -> ConnectorResult<()> {
        let rows = client
            .query(
                "SELECT idx.indexrelid, a.attname, coll_ns.nspname, coll.collname, \
                        (idx.indoption[key.pos] & 1::smallint) <> 0 AS descending, \
                        opc.opcdefault, (idx.indoption[key.pos] & 2::smallint) <> 0 AS nulls_first \
                 FROM pg_index idx \
                 JOIN pg_class tbl ON tbl.oid = idx.indrelid \
                 JOIN pg_namespace ns ON ns.oid = tbl.relnamespace \
                 JOIN pg_class index_rel ON index_rel.oid = idx.indexrelid \
                 JOIN pg_am am ON am.oid = index_rel.relam \
                 JOIN LATERAL generate_subscripts(idx.indkey, 1) AS key(pos) ON TRUE \
                 LEFT JOIN pg_attribute a ON a.attrelid = idx.indrelid \
                   AND a.attnum = idx.indkey[key.pos] \
                 JOIN pg_opclass opc ON opc.oid = idx.indclass[key.pos] \
                 LEFT JOIN pg_collation coll \
                   ON coll.oid = idx.indcollation[key.pos] \
                 LEFT JOIN pg_namespace coll_ns ON coll_ns.oid = coll.collnamespace \
                 WHERE ns.nspname = $1 AND tbl.relname = $2 \
                   AND am.amname = 'btree' \
                   AND idx.indisvalid AND idx.indisready AND idx.indislive \
                   AND idx.indpred IS NULL \
                   AND key.pos < array_lower(idx.indkey, 1) + idx.indnkeyatts \
                 ORDER BY idx.indexrelid, key.pos",
                &[&table_name.schema_name, &table_name.table_name],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to validate PostgreSQL CDC ordering indexes for table {}",
                    Self::get_normalized_table_name(table_name)
                )
            })?;

        let mut indexes = BTreeMap::<u32, Vec<PostgresIndexKey>>::new();
        for row in rows {
            let index_oid: u32 = row.get(0);
            indexes
                .entry(index_oid)
                .or_default()
                .push(PostgresIndexKey {
                    column_name: row.get(1),
                    collation: PostgresCollation::from_catalog(row.try_get(2)?, row.try_get(3)?)?,
                    descending: row.get(4),
                    default_opclass: row.get(5),
                    nulls_first: row.get(6),
                });
        }

        if Self::select_cdc_ordering_index(indexes.values(), primary_keys, pk_ordering) {
            return Ok(());
        }

        let table = Self::get_normalized_table_name(table_name);
        let required_order = Self::get_order_key(primary_keys, pk_ordering);
        Err(anyhow::anyhow!(
            "PostgreSQL CDC TEXT/VARCHAR primary-key ordering requires a valid, ready, \
             non-partial B-tree index whose leading keys are ({required_order}), but table \
             {table} has no such index; create one before starting CDC, for example: \
             CREATE INDEX ON {table} ({required_order})"
        )
        .into())
    }

    fn select_cdc_ordering_index<'a>(
        indexes: impl Iterator<Item = &'a Vec<PostgresIndexKey>> + Clone,
        primary_keys: &[String],
        pk_ordering: &mut HashMap<String, PostgresTextOrdering>,
    ) -> bool {
        // Prefer native bytewise column collations, which can use ordinary primary-key indexes.
        if indexes
            .clone()
            .any(|index| Self::index_supports_cdc_ordering(index, primary_keys, pk_ordering))
        {
            return true;
        }
        // Preserve support for explicit-C secondary indexes, including mixed native/C keys.
        // Choose the query expressions and validate their exact collation identities together.
        for index in indexes {
            let mut candidate = pk_ordering.clone();
            for key in index.iter().take(primary_keys.len()) {
                if let Some(column) = &key.column_name
                    && let Some(ordering) = candidate.get_mut(column)
                    && key.collation.as_ref() == Some(&PostgresTextOrdering::explicit_c().collation)
                {
                    *ordering = PostgresTextOrdering::explicit_c();
                }
            }
            if Self::index_supports_cdc_ordering(index, primary_keys, &candidate) {
                *pk_ordering = candidate;
                return true;
            }
        }
        false
    }

    fn index_supports_cdc_ordering(
        index: &[PostgresIndexKey],
        primary_keys: &[String],
        pk_ordering: &HashMap<String, PostgresTextOrdering>,
    ) -> bool {
        let Some(index_prefix) = index.get(..primary_keys.len()) else {
            return false;
        };
        let Some(first_key) = index_prefix.first() else {
            return false;
        };
        index_prefix
            .iter()
            .zip_eq_fast(primary_keys)
            .all(|(index_key, primary_key)| {
                index_key.column_name.as_deref() == Some(primary_key)
                    && index_key.default_opclass
                    && index_key.descending == first_key.descending
                    // Forward ASC or backward DESC must both yield ASC NULLS LAST.
                    && index_key.nulls_first == index_key.descending
                    && pk_ordering.get(primary_key).is_none_or(|ordering| {
                        index_key.collation.as_ref() == Some(&ordering.collation)
                    })
            })
    }

    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
        schema_table_name: SchemaTableName,
    ) -> ConnectorResult<Self> {
        tracing::info!(
            ?rw_schema,
            ?pk_indices,
            "create postgres external table reader"
        );
        // No TCP keepalive for CDC source
        let client = create_pg_client(&config.pg_connection_config()?, None).await?;
        let pk_column_names = pk_indices
            .iter()
            .map(|index| rw_schema.fields[*index].name.clone())
            .collect_vec();
        let mut pk_ordering = Self::discover_pk_ordering(
            &client,
            &schema_table_name,
            &pk_column_names,
            config.bypass_pk_order_validation,
        )
        .await?;
        if !config.bypass_pk_order_validation && !pk_ordering.is_empty() {
            Self::validate_server_encoding(&client).await?;
            Self::validate_cdc_ordering_index(
                &client,
                &schema_table_name,
                &pk_column_names,
                &mut pk_ordering,
            )
            .await?;
        }

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
            pk_ordering,
            client: tokio::sync::Mutex::new(client),
            schema_table_name,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        format!(
            "{}.{}",
            Self::quote_column(&table_name.schema_name),
            Self::quote_column(&table_name.table_name)
        )
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::Postgres(PostgresOffset::parse_debezium_offset(
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
        let order_key = Self::get_order_key(&primary_keys, &self.pk_ordering);
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

                    let order_key = Self::get_order_key(&primary_keys, &self.pk_ordering);
                    let scan_sql = format!(
                        "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {scan_limit}",
                        self.field_names,
                        Self::get_normalized_table_name(&table_name),
                        Self::filter_expression(&primary_keys, &self.pk_ordering),
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
    fn filter_expression(
        columns: &[String],
        pk_ordering: &HashMap<String, PostgresTextOrdering>,
    ) -> String {
        let mut col_expr = String::new();
        let mut arg_expr = String::new();
        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                col_expr.push_str(", ");
                arg_expr.push_str(", ");
            }
            col_expr.push_str(&Self::ordering_column_expression(column, pk_ordering));
            arg_expr.push_str(format!("${}", i + 1).as_str());
        }
        format!("({}) > ({})", col_expr, arg_expr)
    }

    // row filter expression: (v1, v2, v3) >= ($1, $2, $3) AND (v1, v2, v3) < ($1, $2, $3)
    fn split_filter_expression(
        columns: &[String],
        is_first_split: bool,
        is_last_split: bool,
        pk_ordering: &HashMap<String, PostgresTextOrdering>,
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
                left_col_expr.push_str(&Self::ordering_column_expression(column, pk_ordering));
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
                right_col_expr.push_str(&Self::ordering_column_expression(column, pk_ordering));
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

    fn get_order_key(
        primary_keys: &[String],
        pk_ordering: &HashMap<String, PostgresTextOrdering>,
    ) -> String {
        primary_keys
            .iter()
            .map(|column| Self::ordering_column_expression(column, pk_ordering))
            .join(",")
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column.replace('"', "\"\""))
    }

    fn ordering_column_expression(
        column: &str,
        pk_ordering: &HashMap<String, PostgresTextOrdering>,
    ) -> String {
        let quoted = Self::quote_column(column);
        if pk_ordering
            .get(column)
            .is_some_and(|ordering| !ordering.use_native)
        {
            format!("{quoted} COLLATE pg_catalog.\"C\"")
        } else {
            quoted
        }
    }

    async fn min_and_max(
        &self,
        split_column: &Field,
    ) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let split_column_expr =
            Self::ordering_column_expression(&split_column.name, &self.pk_ordering);
        let sql = format!(
            "SELECT MIN({}), MAX({}) FROM {}",
            split_column_expr,
            split_column_expr,
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
        let split_column_expr =
            Self::ordering_column_expression(&split_column.name, &self.pk_ordering);
        let sql = format!(
            "WITH t as (SELECT {} FROM {} WHERE {} >= $1 ORDER BY {} ASC LIMIT {}) SELECT CASE WHEN MAX({}) < $2 THEN MAX({}) ELSE NULL END FROM t",
            Self::quote_column(&split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            split_column_expr,
            split_column_expr,
            max_split_size,
            split_column_expr,
            split_column_expr,
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
        let split_column_expr =
            Self::ordering_column_expression(&split_column.name, &self.pk_ordering);
        let sql = format!(
            "SELECT MIN({}) FROM {} WHERE {} > $1 AND {} <$2",
            split_column_expr,
            Self::get_normalized_table_name(&self.schema_table_name),
            split_column_expr,
            split_column_expr,
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
            // Parallel backfill checkpoints whole splits, so rows within a split need no order.
            let scan_sql = format!(
                "SELECT {} FROM {} WHERE {}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                Self::split_filter_expression(
                    &split_column_names,
                    is_first_split,
                    is_last_split,
                    &self.pk_ordering
                ),
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
            "geometry" => Some(PgType::BYTEA_ARRAY), // PostGIS geometry array
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
            "geometry" => Some(PgType::BYTEA), // PostGIS geometry type
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
        PgType::POINT => DataType::Struct(risingwave_common::types::StructType::new(vec![
            ("x", DataType::Float32),
            ("y", DataType::Float32),
        ])),
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
        PgType::POINT_ARRAY => {
            DataType::list(DataType::Struct(risingwave_common::types::StructType::new(
                vec![("x", DataType::Float32), ("y", DataType::Float32)],
            )))
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
    use std::collections::{HashMap, HashSet};

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use tokio_postgres::types::Type as PgType;

    use crate::connector_common::PostgresExternalTable;
    use crate::source::cdc::external::postgres::{
        PostgresCollation, PostgresExternalTableReader, PostgresIndexKey, PostgresOffset,
        PostgresTextOrdering,
    };
    use crate::source::cdc::external::{ExternalTableConfig, ExternalTableReader, SchemaTableName};

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
            ssl_mode: Default::default(),
            ssl_root_cert: None,
            encrypt: "false".to_owned(),
            bypass_pk_order_validation: false,
        };

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

        let offset = PostgresOffset::parse_debezium_offset(debezium_offset_with_fields).unwrap();
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

        let result = PostgresOffset::parse_debezium_offset(debezium_offset_missing_fields);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("invalid postgres lsn_proc"));
    }

    #[test]
    fn test_filter_expression() {
        let no_binary_columns = HashMap::new();
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols, &no_binary_columns);
        assert_eq!(expr, "(\"v1\") > ($1)");

        let cols = vec!["v1".to_owned(), "v2".to_owned()];
        let expr = PostgresExternalTableReader::filter_expression(&cols, &no_binary_columns);
        assert_eq!(expr, "(\"v1\", \"v2\") > ($1, $2)");

        let binary_columns = ["v1".to_owned(), "v3".to_owned()]
            .into_iter()
            .map(|column| (column, PostgresTextOrdering::explicit_c()))
            .collect();
        let cols = vec![
            "v1".to_owned(),
            "v2".to_owned(),
            "quote\"inside".to_owned(),
            "v3".to_owned(),
        ];
        let expr = PostgresExternalTableReader::filter_expression(&cols, &binary_columns);
        assert_eq!(
            expr,
            "(\"v1\" COLLATE pg_catalog.\"C\", \"v2\", \"quote\"\"inside\", \"v3\" COLLATE \
             pg_catalog.\"C\") > \
             ($1, $2, $3, $4)"
        );
    }

    #[test]
    fn test_split_filter_expression() {
        let binary_columns = ["v1".to_owned()]
            .into_iter()
            .map(|column| (column, PostgresTextOrdering::explicit_c()))
            .collect();
        let cols = vec!["v1".to_owned()];
        let expr = PostgresExternalTableReader::split_filter_expression(
            &cols,
            true,
            true,
            &binary_columns,
        );
        assert_eq!(expr, "1 = 1");

        let expr = PostgresExternalTableReader::split_filter_expression(
            &cols,
            true,
            false,
            &binary_columns,
        );
        assert_eq!(expr, "(\"v1\" COLLATE pg_catalog.\"C\") < ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(
            &cols,
            false,
            true,
            &binary_columns,
        );
        assert_eq!(expr, "(\"v1\" COLLATE pg_catalog.\"C\") >= ($1)");

        let expr = PostgresExternalTableReader::split_filter_expression(
            &cols,
            false,
            false,
            &binary_columns,
        );
        assert_eq!(
            expr,
            "(\"v1\" COLLATE pg_catalog.\"C\") >= ($1) AND (\"v1\" COLLATE \
             pg_catalog.\"C\") < ($2)"
        );
    }

    #[test]
    fn test_text_pk_order_key_uses_binary_collation() {
        let cols = vec!["v1".to_owned(), "v2".to_owned(), "v3".to_owned()];
        let binary_columns = ["v1".to_owned(), "v3".to_owned()]
            .into_iter()
            .map(|column| (column, PostgresTextOrdering::explicit_c()))
            .collect();
        assert_eq!(
            PostgresExternalTableReader::get_order_key(&cols, &binary_columns),
            "\"v1\" COLLATE pg_catalog.\"C\",\"v2\",\"v3\" COLLATE pg_catalog.\"C\""
        );
    }

    #[test]
    fn test_postgres_collation_catalog_pair() {
        assert_eq!(PostgresCollation::from_catalog(None, None).unwrap(), None);
        assert_eq!(
            PostgresCollation::from_catalog(Some("pg_catalog".into()), Some("C".into())).unwrap(),
            Some(PostgresCollation {
                schema: "pg_catalog".into(),
                name: "C".into()
            }),
        );
        assert!(PostgresCollation::from_catalog(Some("pg_catalog".into()), None).is_err());
        assert!(PostgresCollation::from_catalog(None, Some("C".into())).is_err());
    }

    #[test]
    fn test_postgres_cdc_ordering_index_policy() {
        fn key(
            column_name: Option<&str>,
            collation: Option<(&str, &str)>,
            descending: bool,
            default_opclass: bool,
        ) -> PostgresIndexKey {
            PostgresIndexKey {
                column_name: column_name.map(str::to_owned),
                collation: collation.map(|(schema, name)| PostgresCollation {
                    schema: schema.to_owned(),
                    name: name.to_owned(),
                }),
                descending,
                nulls_first: descending,
                default_opclass,
            }
        }

        let primary_keys = vec!["tenant_id".to_owned(), "id".to_owned()];
        let binary_columns = ["id".to_owned()]
            .into_iter()
            .map(|column| (column, PostgresTextOrdering::explicit_c()))
            .collect();
        let compatible = vec![
            key(Some("tenant_id"), None, false, true),
            key(Some("id"), Some(("pg_catalog", "C")), false, true),
        ];
        assert!(PostgresExternalTableReader::index_supports_cdc_ordering(
            &compatible,
            &primary_keys,
            &binary_columns,
        ));

        let mut locale_collated = compatible.clone();
        locale_collated[1].collation.as_mut().unwrap().name = "en-x-icu".to_owned();
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &locale_collated,
            &primary_keys,
            &binary_columns,
        ));
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &compatible[..1],
            &primary_keys,
            &binary_columns,
        ));

        let mixed_direction = vec![
            key(Some("tenant_id"), None, false, true),
            key(Some("id"), Some(("pg_catalog", "C")), true, true),
        ];
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &mixed_direction,
            &primary_keys,
            &binary_columns,
        ));
        let all_descending = vec![
            key(Some("tenant_id"), None, true, true),
            key(Some("id"), Some(("pg_catalog", "C")), true, true),
        ];
        assert!(PostgresExternalTableReader::index_supports_cdc_ordering(
            &all_descending,
            &primary_keys,
            &binary_columns,
        ));

        for original in [&compatible, &all_descending] {
            for position in 0..primary_keys.len() {
                let mut incompatible_nulls = original.clone();
                incompatible_nulls[position].nulls_first =
                    !incompatible_nulls[position].nulls_first;
                assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
                    &incompatible_nulls,
                    &primary_keys,
                    &binary_columns,
                ));
            }
        }

        let non_default_opclass = vec![
            key(Some("tenant_id"), None, false, true),
            key(Some("id"), Some(("pg_catalog", "C")), false, false),
        ];
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &non_default_opclass,
            &primary_keys,
            &binary_columns,
        ));
        let expression_key = vec![
            key(Some("tenant_id"), None, false, true),
            key(None, Some(("pg_catalog", "C")), false, true),
        ];
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &expression_key,
            &primary_keys,
            &binary_columns,
        ));
    }

    #[test]
    fn test_text_pk_requires_utf8_server_encoding() {
        assert!(PostgresExternalTableReader::check_server_encoding("UTF8").is_ok());
        assert!(PostgresExternalTableReader::check_server_encoding("utf8").is_ok());
        let error = PostgresExternalTableReader::check_server_encoding("LATIN1").unwrap_err();
        assert!(error.to_string().contains("server_encoding=UTF8"));
    }

    #[test]
    fn test_postgres_bypass_preserves_text_order_and_catalog_checks() {
        let table = SchemaTableName {
            schema_name: "public".into(),
            table_name: "t".into(),
        };
        let columns = HashMap::from([
            (
                "text_key".into(),
                (PgType::TEXT.oid(), "b".into(), "S".into(), "text".into()),
            ),
            (
                "enum_key".into(),
                (u32::MAX, "e".into(), "E".into(), "my_enum".into()),
            ),
            (
                "unknown_key".into(),
                (u32::MAX - 1, "b".into(), "U".into(), "unknown".into()),
            ),
        ]);
        let keys = vec!["text_key".into(), "enum_key".into(), "unknown_key".into()];
        assert!(
            PostgresExternalTableReader::binary_collated_pk_columns(&columns, &table, &keys, false)
                .is_err()
        );
        assert_eq!(
            PostgresExternalTableReader::binary_collated_pk_columns(&columns, &table, &keys, true)
                .unwrap(),
            HashSet::from(["text_key".into()])
        );
        assert!(
            PostgresExternalTableReader::binary_collated_pk_columns(
                &columns,
                &table,
                &["unknown_key".into()],
                false
            )
            .unwrap()
            .is_empty()
        );
        assert!(
            PostgresExternalTableReader::binary_collated_pk_columns(
                &columns,
                &table,
                &["missing".into()],
                true
            )
            .is_err()
        );
    }

    #[test]
    fn test_postgres_pk_type_policy_is_blacklist() {
        for pg_type in [PgType::TEXT, PgType::VARCHAR] {
            assert!(PostgresExternalTableReader::is_binary_collated_pk_type(
                &pg_type
            ));
        }
        for oid in [
            PgType::INT4.oid(),
            PgType::TEXT.oid(),
            PgType::INTERVAL.oid(),
            PgType::MONEY.oid(),
            PgType::TIMETZ.oid(),
            u32::MAX,
        ] {
            assert!(
                PostgresExternalTableReader::unsupported_pk_type_reason(oid, "b", "U", "unknown")
                    .is_none()
            );
        }
        for oid in [PgType::BPCHAR.oid(), PgType::JSONB.oid()] {
            assert!(
                PostgresExternalTableReader::unsupported_pk_type_reason(oid, "b", "U", "unknown")
                    .is_some()
            );
        }
        // Catalog metadata catches enums and arrays even when their OID is not built in.
        assert!(
            PostgresExternalTableReader::unsupported_pk_type_reason(u32::MAX, "e", "E", "unknown")
                .is_some()
        );
        assert!(
            PostgresExternalTableReader::unsupported_pk_type_reason(u32::MAX, "b", "A", "unknown")
                .is_some()
        );
        assert!(
            PostgresExternalTableReader::unsupported_pk_type_reason(u32::MAX, "d", "U", "unknown")
                .is_none()
        );
    }

    #[test]
    fn test_citext_pk_requires_explicit_bypass() {
        let table = SchemaTableName {
            schema_name: "public".into(),
            table_name: "t".into(),
        };
        let columns = HashMap::from([(
            "id".into(),
            (u32::MAX, "b".into(), "S".into(), "citext".into()),
        )]);
        let keys = vec!["id".into()];
        let error =
            PostgresExternalTableReader::binary_collated_pk_columns(&columns, &table, &keys, false)
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("citext case-insensitive ordering")
        );
        assert!(
            PostgresExternalTableReader::binary_collated_pk_columns(&columns, &table, &keys, true,)
                .unwrap()
                .is_empty()
        );
    }

    /// Requires an UTF-8 PostgreSQL database with ICU collations and
    /// POSTGRES_TEST_CONNECTION_STRING set to a tokio-postgres connection string.
    #[ignore]
    #[tokio::test]
    async fn test_postgres_cdc_ordering_index_catalog() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let connection_string = std::env::var("POSTGRES_TEST_CONNECTION_STRING")
                .expect("set POSTGRES_TEST_CONNECTION_STRING to run this test");
            let (client, connection) =
                tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                    .await
                    .unwrap();
            let connection_task = tokio::spawn(async move { connection.await.unwrap() });
            client
                .batch_execute(
                    r#"CREATE TEMP TABLE cdc_ordering_index_test (
                    tenant_id integer NOT NULL,
                    id text COLLATE pg_catalog."en-x-icu" NOT NULL,
                    PRIMARY KEY (tenant_id, id)
                )"#,
                )
                .await
                .unwrap();
            let schema_name = client
                .query_one(
                    "SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            let table = SchemaTableName {
                schema_name,
                table_name: "cdc_ordering_index_test".into(),
            };
            let keys = vec!["tenant_id".into(), "id".into()];
            let mut binary_columns =
                PostgresExternalTableReader::discover_pk_ordering(&client, &table, &keys, false)
                    .await
                    .unwrap();
            assert_eq!(
                binary_columns,
                HashMap::from([("id".into(), PostgresTextOrdering::explicit_c())])
            );
            PostgresExternalTableReader::validate_server_encoding(&client)
                .await
                .unwrap();
            let error = PostgresExternalTableReader::validate_cdc_ordering_index(
                &client,
                &table,
                &keys,
                &mut binary_columns,
            )
            .await
            .unwrap_err();
            assert!(error.to_string().contains("has no such index"));
            assert!(error.to_string().contains(r#"CREATE INDEX ON"#,));
            // A partial index cannot cover the whole snapshot.
            client
                .batch_execute(
                    r#"CREATE INDEX ON cdc_ordering_index_test
                    (tenant_id, id COLLATE pg_catalog."C") WHERE tenant_id > 0"#,
                )
                .await
                .unwrap();
            assert!(
                PostgresExternalTableReader::validate_cdc_ordering_index(
                    &client,
                    &table,
                    &keys,
                    &mut binary_columns,
                )
                .await
                .is_err()
            );
            client
                .batch_execute(
                    r#"CREATE INDEX ON cdc_ordering_index_test
                    (tenant_id, id COLLATE pg_catalog."C")"#,
                )
                .await
                .unwrap();
            PostgresExternalTableReader::validate_cdc_ordering_index(
                &client,
                &table,
                &keys,
                &mut binary_columns,
            )
            .await
            .unwrap();
            // The temporary table and indexes disappear with the connection.
            drop(client);
            connection_task.await.unwrap();
        })
        .await
        .expect("PostgreSQL ordering-index validation timed out");
    }

    #[test]
    fn test_index_selection_preserves_collation_identity() {
        let keys = vec!["id".to_owned()];
        let native = PostgresTextOrdering {
            collation: PostgresCollation {
                schema: "pg_catalog".into(),
                name: "default".into(),
            },
            use_native: true,
        };
        let original = HashMap::from([("id".into(), native)]);
        let index = |collation: &str| {
            vec![PostgresIndexKey {
                column_name: Some("id".into()),
                collation: Some(PostgresCollation {
                    schema: "pg_catalog".into(),
                    name: collation.into(),
                }),
                descending: false,
                nulls_first: false,
                default_opclass: true,
            }]
        };

        // Even if an explicit-C index comes first, prefer the native matching index.
        let indexes = [index("C"), index("default")];
        let mut ordering = original.clone();
        assert!(PostgresExternalTableReader::select_cdc_ordering_index(
            indexes.iter(),
            &keys,
            &mut ordering,
        ));
        assert_eq!(ordering, original);
        assert_eq!(
            PostgresExternalTableReader::get_order_key(&keys, &ordering),
            r#""id""#
        );

        // A semantically equivalent but differently identified index requires matching SQL.
        let indexes = [index("C")];
        assert!(!PostgresExternalTableReader::index_supports_cdc_ordering(
            &indexes[0],
            &keys,
            &ordering,
        ));
        assert!(PostgresExternalTableReader::select_cdc_ordering_index(
            indexes.iter(),
            &keys,
            &mut ordering,
        ));
        assert_eq!(ordering["id"], PostgresTextOrdering::explicit_c());
        assert_eq!(
            PostgresExternalTableReader::get_order_key(&keys, &ordering),
            r#""id" COLLATE pg_catalog."C""#,
        );

        let indexes = [index("en-x-icu")];
        let mut ordering = original.clone();
        assert!(!PostgresExternalTableReader::select_cdc_ordering_index(
            indexes.iter(),
            &keys,
            &mut ordering,
        ));
        assert_eq!(ordering, original);
    }

    #[test]
    fn test_native_bytewise_collation_policy() {
        for locale in ["C", "POSIX"] {
            assert!(PostgresExternalTableReader::is_bytewise_collation(
                "c",
                Some(locale)
            ));
        }
        for (provider, locale) in [
            ("i", Some("C")),
            ("c", Some("en_US.UTF-8")),
            ("c", None),
            ("d", Some("C")),
        ] {
            assert!(!PostgresExternalTableReader::is_bytewise_collation(
                provider, locale
            ));
        }
    }

    /// Run with POSTGRES_TEST_CONNECTION_STRING pointing to a UTF-8 database with
    /// either a libc C/POSIX or ICU default. Exercises pagination and parallel split reads.
    #[ignore]
    #[tokio::test]
    async fn test_postgres_bytewise_ordering_catalog() {
        use futures::TryStreamExt;

        use crate::source::cdc::external::CdcTableSnapshotSplitOption;

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let connection_string = std::env::var("POSTGRES_TEST_CONNECTION_STRING")
                .expect("set POSTGRES_TEST_CONNECTION_STRING to run this test");
            let (mut client, connection) =
                tokio_postgres::connect(&connection_string, tokio_postgres::NoTls).await.unwrap();
            let connection_task = tokio::spawn(async move { connection.await.unwrap() });
            PostgresExternalTableReader::validate_server_encoding(&client).await.unwrap();
            let db = client.query_one(
                "SELECT COALESCE(to_jsonb(db)->>'datlocprovider', 'c'), datcollate::text                  FROM pg_database db WHERE datname = current_database()", &[],
            ).await.unwrap();
            let native_default = match db.get::<_, &str>(0) {
                "c" => {
                    assert!(matches!(db.get::<_, &str>(1), "C" | "POSIX"));
                    true
                }
                "i" => false,
                provider => panic!("unsupported test database provider: {provider}"),
            };
            let mixed_index = if native_default {
                r#"tenant, id COLLATE "C""#
            } else {
                r#"tenant COLLATE "C", id COLLATE "C""#
            };

            // Small fixtures need a planner hint to test index ordering independently of cost.
            client.batch_execute("SET enable_seqscan = off").await.unwrap();
            for (definition, secondary_index, native_columns) in [
                ("id text PRIMARY KEY", if native_default { None } else { Some(r#"id COLLATE "C""#) }, vec![native_default]),
                (r#"id text COLLATE "C" PRIMARY KEY"#, None, vec![true]),
                (r#"id text COLLATE "POSIX" PRIMARY KEY"#, None, vec![true]),
                (r#"id text COLLATE "en-x-icu" PRIMARY KEY"#, Some(r#"id COLLATE "C""#), vec![false]),
                (r#"tenant text, id text COLLATE "en-x-icu", PRIMARY KEY (tenant, id)"#,
                 Some(mixed_index), vec![native_default, false]),
                // Only an explicit-C index is available for these natively bytewise columns.
                ("id text NOT NULL", Some(r#"id COLLATE "C""#), vec![false]),
                ("tenant text NOT NULL, id text NOT NULL",
                 Some(mixed_index), vec![native_default, false]),
            ] {
                client.batch_execute(&format!(
                    "CREATE TEMP TABLE cdc_native_ordering_test ({definition})",
                )).await.unwrap();
                let keys: Vec<String> = if native_columns.len() == 1 {
                    vec!["id".into()]
                } else {
                    vec!["tenant".into(), "id".into()]
                };
                let table = SchemaTableName {
                    schema_name: client.query_one(
                        "SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()", &[],
                    ).await.unwrap().get(0),
                    table_name: "cdc_native_ordering_test".into(),
                };
                let mut ordering = PostgresExternalTableReader::discover_pk_ordering(
                    &client, &table, &keys, false,
                ).await.unwrap();
                if let Some(index) = secondary_index {
                    assert!(PostgresExternalTableReader::validate_cdc_ordering_index(
                        &client, &table, &keys, &mut ordering,
                    ).await.is_err(), "{definition}");
                    client.batch_execute(&format!(
                        "CREATE UNIQUE INDEX ON cdc_native_ordering_test ({index})",
                    )).await.unwrap();
                }
                PostgresExternalTableReader::validate_cdc_ordering_index(
                    &client, &table, &keys, &mut ordering,
                ).await.unwrap();
                for (key, expected_native) in keys.iter().zip(&native_columns) {
                    assert_eq!(ordering[key].use_native, *expected_native, "{definition}: {key}");
                }
                let values = "(VALUES ('B'), ('a'), ('z'), ('é'), ('中'), ('🙂')) v(id)";
                let select = if keys.len() == 1 {
                    format!("SELECT id FROM {values}")
                } else {
                    format!("SELECT tenant, id FROM {values} CROSS JOIN (VALUES ('B'), ('a')) t(tenant)")
                };
                client.batch_execute(&format!(
                    "INSERT INTO cdc_native_ordering_test {select}; ANALYZE cdc_native_ordering_test",
                )).await.unwrap();
                let fields = keys.iter().map(|key| Field::with_name(DataType::Varchar, key)).collect();
                let field_names = keys.iter().map(|key| PostgresExternalTableReader::quote_column(key))
                    .collect::<Vec<_>>().join(",");
                // Independent bytewise ordering oracle.
                let oracle_order = keys.iter().map(|key| format!(r#""{key}" COLLATE "C""#))
                    .collect::<Vec<_>>().join(",");
                let expected: Vec<OwnedRow> = client.query(&format!(
                    "SELECT {field_names} FROM cdc_native_ordering_test ORDER BY {oracle_order}",
                ), &[]).await.unwrap().iter().map(|row| OwnedRow::new(
                    (0..keys.len()).map(|i| Some(ScalarImpl::from(row.get::<_, &str>(i)))).collect(),
                )).collect();
                let order = PostgresExternalTableReader::get_order_key(&keys, &ordering);
                for filter in [
                    String::new(),
                    format!("WHERE {}", PostgresExternalTableReader::filter_expression(&keys, &ordering)
                        .replace("$1", "'B'").replace("$2", "'a'")),
                ] {
                    let plan = client.query(&format!(
                        "EXPLAIN SELECT {field_names} FROM cdc_native_ordering_test {filter} ORDER BY {order} LIMIT 2",
                    ), &[]).await.unwrap().iter().map(|row| row.get::<_, String>(0))
                        .collect::<Vec<_>>().join("\n");
                    assert!(plan.contains("Index") && !plan.contains("Sort"), "{definition}: {plan}");
                }
                let reader = PostgresExternalTableReader {
                    rw_schema: Schema { fields },
                    field_names,
                    pk_indices: (0..keys.len()).collect(),
                    pk_ordering: ordering,
                    client: tokio::sync::Mutex::new(client),
                    schema_table_name: table.clone(),
                };
                let mut actual = vec![];
                let mut cursor = None;
                loop {
                    let page: Vec<_> = reader.snapshot_read(table.clone(), cursor, keys.clone(), 2)
                        .try_collect().await.unwrap();
                    if page.is_empty() { break; }
                    cursor = page.last().cloned();
                    actual.extend(page);
                    assert!(actual.len() <= expected.len(), "pagination did not advance");
                }
                assert_eq!(actual, expected, "{definition}");
                // Exercise MIN/MAX, right-bound queries, and unordered scans with the same choice.
                for split_column in 0..keys.len() {
                    let splits: Vec<_> = reader.get_parallel_cdc_splits(CdcTableSnapshotSplitOption {
                        backfill_num_rows_per_split: 2,
                        backfill_as_even_splits: false,
                        backfill_split_pk_column_index: split_column as u32,
                    }).try_collect().await.unwrap();
                    let mut actual = vec![];
                    for split in splits {
                        actual.extend(reader.split_snapshot_read(
                            table.clone(), split.left_bound_inclusive, split.right_bound_exclusive,
                            vec![reader.rw_schema.fields[split_column].clone()],
                        ).try_collect::<Vec<_>>().await.unwrap());
                    }
                    // Splits need no row ordering, but must cover each row exactly once.
                    let mut actual = actual.into_iter().map(|row| format!("{row:?}")).collect::<Vec<_>>();
                    let mut expected = expected.iter().map(|row| format!("{row:?}")).collect::<Vec<_>>();
                    actual.sort();
                    expected.sort();
                    assert_eq!(actual, expected, "{definition}: split column {split_column}");
                }
                client = reader.client.into_inner();
                client.batch_execute("DROP TABLE cdc_native_ordering_test").await.unwrap();
            }
            drop(client);
            connection_task.await.unwrap();
        }).await.expect("native bytewise ordering test timed out");
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
