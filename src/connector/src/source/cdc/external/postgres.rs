// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::{for_await, try_stream};
use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{Datum, ScalarImpl, ToOwnedDatum};
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};
use tokio_postgres::types::PgLsn;

use crate::connector_common::create_pg_client;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::{postgres_cell_to_scalar_impl, postgres_row_to_owned_row};
use crate::source::CdcTableSnapshotSplit;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, CdcTableSnapshotSplitOption, DebeziumOffset,
    ExternalTableConfig, ExternalTableReader, SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PostgresOffset {
    pub txid: i64,
    // In postgres, an LSN is a 64-bit integer, representing a byte position in the write-ahead log stream.
    // It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash; for example, 16/B374D848
    pub lsn: u64,
}

// only compare the lsn field
impl PartialOrd for PostgresOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.lsn.partial_cmp(&other.lsn)
    }
}

impl PostgresOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            txid: dbz_offset
                .source_offset
                .txid
                .context("invalid postgres txid")?,
            lsn: dbz_offset
                .source_offset
                .lsn
                .context("invalid postgres lsn")?,
        })
    }
}

pub struct PostgresExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    pk_indices: Vec<usize>,
    client: tokio::sync::Mutex<tokio_postgres::Client>,
    split_column: Field,
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
        self.snapshot_read_inner(table_name, start_pk, primary_keys, limit)
    }

    #[try_stream(boxed, ok = CdcTableSnapshotSplit, error = ConnectorError)]
    async fn get_parallel_cdc_splits(&self, options: CdcTableSnapshotSplitOption) {
        let mut split_id = 1;
        let Some(backfill_num_rows_per_split) = options.backfill_num_rows_per_split else {
            // No parallel cdc splits.
            return Ok(());
        };
        if backfill_num_rows_per_split == 0 {
            // No parallel cdc splits.
            return Ok(());
        }
        // TODO: for numeric types, use evenly-sized partition to optimize performance.

        let Some((min_value, max_value)) = self.min_and_max().await? else {
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
        tracing::debug!(?self.schema_table_name, ?self.rw_schema, ?self.pk_indices, ?self.split_column, "get parallel cdc table snapshot splits");
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
                    backfill_num_rows_per_split,
                )
                .await?;
            if let Some(Some(ref inner)) = next_right
                && *inner == next_left_bound_inclusive
            {
                next_right = self
                    .next_greater_bound(&next_left_bound_inclusive, &max_value)
                    .await?;
            }
            if let Some(next_right) = next_right {
                match next_right {
                    None => {
                        // NULL found.
                        right_bound_exclusive = None;
                    }
                    Some(next_right) => {
                        next_left_bound_inclusive = next_right.to_owned();
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
            tracing::debug!(
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
            match split_id.checked_add(1) {
                Some(s) => {
                    split_id = s;
                }
                None => return Err(anyhow::anyhow!("too many CDC snapshot splits").into()),
            }
            yield split;
            if is_completed {
                break;
            }
        }
    }

    fn split_snapshot_read(
        &self,
        table_name: SchemaTableName,
        left: OwnedRow,
        right: OwnedRow,
        split_columns: Vec<Field>,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        self.split_snapshot_read_inner(table_name, left, right, split_columns)
    }
}

impl PostgresExternalTableReader {
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

        let client = create_pg_client(
            &config.username,
            &config.password,
            &config.host,
            &config.port,
            &config.database,
            &config.ssl_mode,
            &config.ssl_root_cert,
        )
        .await?;

        let field_names = rw_schema
            .fields
            .iter()
            .map(|f| Self::quote_column(&f.name))
            .join(",");

        let split_column = split_column(&rw_schema, &pk_indices);
        Ok(Self {
            rw_schema,
            field_names,
            pk_indices,
            client: tokio::sync::Mutex::new(client),
            split_column,
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
            Ok::<_, crate::error::ConnectorError>(postgres_row_to_owned_row(row, &self.rw_schema))
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

    async fn min_and_max(&self) -> ConnectorResult<Option<(ScalarImpl, ScalarImpl)>> {
        let sql = format!(
            "SELECT MIN({}), MAX({}) FROM {}",
            self.split_column.name,
            self.split_column.name,
            Self::get_normalized_table_name(&self.schema_table_name),
        );
        let client = self.client.lock().await;
        let rows = client.query(&sql, &[]).await?;
        if rows.is_empty() {
            Ok(None)
        } else {
            let row = &rows[0];
            let min = postgres_cell_to_scalar_impl(
                row,
                &self.split_column.data_type,
                0,
                &self.split_column.name,
            );
            let max = postgres_cell_to_scalar_impl(
                row,
                &self.split_column.data_type,
                1,
                &self.split_column.name,
            );
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
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "WITH t as (SELECT {} FROM {} WHERE {} >= $1 ORDER BY {} ASC LIMIT {}) SELECT CASE WHEN MAX({}) < $2 THEN MAX({}) ELSE NULL END FROM t",
            Self::quote_column(&self.split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&self.split_column.name),
            Self::quote_column(&self.split_column.name),
            max_split_size,
            Self::quote_column(&self.split_column.name),
            Self::quote_column(&self.split_column.name),
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
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl(
                &row,
                &self.split_column.data_type,
                0,
                &self.split_column.name,
            ))
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
    ) -> ConnectorResult<Option<Datum>> {
        let sql = format!(
            "SELECT MIN({}) FROM {} WHERE {} > $1 AND {} <$2",
            Self::quote_column(&self.split_column.name),
            Self::get_normalized_table_name(&self.schema_table_name),
            Self::quote_column(&self.split_column.name),
            Self::quote_column(&self.split_column.name),
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
            Ok::<_, ConnectorError>(postgres_cell_to_scalar_impl(
                &row,
                &self.split_column.data_type,
                0,
                &self.split_column.name,
            ))
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
            Ok::<_, crate::error::ConnectorError>(postgres_row_to_owned_row(row, &self.rw_schema))
        });

        pin_mut!(row_stream);
        #[for_await]
        for row in row_stream {
            let row = row?;
            yield row;
        }
    }
}

/// Use the first column of primary keys to split table.
fn split_column(rw_schema: &Schema, pk_indices: &[usize]) -> Field {
    rw_schema.fields[pk_indices[0]].clone()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use crate::connector_common::PostgresExternalTable;
    use crate::source::cdc::external::postgres::{PostgresExternalTableReader, PostgresOffset};
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
        };

        let table = PostgresExternalTable::connect(
            &config.username,
            &config.password,
            &config.host,
            config.port.parse::<u16>().unwrap(),
            &config.database,
            &config.schema,
            &config.table,
            &config.ssl_mode,
            &config.ssl_root_cert,
            false,
        )
        .await
        .unwrap();

        println!("columns: {:?}", &table.column_descs());
        println!("primary keys: {:?}", &table.pk_names());
    }

    #[test]
    fn test_postgres_offset() {
        let off1 = PostgresOffset { txid: 4, lsn: 2 };
        let off2 = PostgresOffset { txid: 1, lsn: 3 };
        let off3 = PostgresOffset { txid: 5, lsn: 1 };

        assert!(off1 < off2);
        assert!(off3 < off1);
        assert!(off2 > off3);
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
        let columns = vec![
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
