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
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};
use tokio_postgres::types::PgLsn;

use crate::connector_common::create_pg_client;
use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::postgres_row_to_owned_row;
use crate::parser::scalar_adapter::ScalarAdapter;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
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

// only compare the lsn field, prefer lsn_commit and lsn_proc if both available
impl PartialOrd for PostgresOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (
            self.lsn_commit,
            self.lsn_proc,
            other.lsn_commit,
            other.lsn_proc,
        ) {
            (_, Some(self_proc), _, Some(other_proc)) => {
                // if both have `lsn_commit` and `lsn_proc`, compare `lsn_commit` first, then `lsn_proc`
                // if `lsn_commit` is None, fall back to `lsn_proc`
                match self.lsn_commit.partial_cmp(&other.lsn_commit) {
                    Some(Ordering::Equal) => self_proc.partial_cmp(&other_proc),
                    other_result => other_result,
                }
            }
            _ => {
                // Fall back to lsn comparison when either lsn_commit or lsn_proc is missing
                self.lsn.partial_cmp(&other.lsn)
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
    client: tokio::sync::Mutex<tokio_postgres::Client>,
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
}

impl PostgresExternalTableReader {
    pub async fn new(
        config: ExternalTableConfig,
        rw_schema: Schema,
        pk_indices: Vec<usize>,
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

        Ok(Self {
            rw_schema,
            field_names,
            pk_indices,
            client: tokio::sync::Mutex::new(client),
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

    fn get_order_key(primary_keys: &Vec<String>) -> String {
        primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",")
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column)
    }
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
        let reader = PostgresExternalTableReader::new(config, rw_schema, vec![0, 1])
            .await
            .unwrap();

        let offset = reader.current_cdc_offset().await.unwrap();
        println!("CdcOffset: {:?}", offset);

        let start_pk = OwnedRow::new(vec![Some(ScalarImpl::from(3)), Some(ScalarImpl::from("c"))]);
        let stream = reader.snapshot_read(
            SchemaTableName {
                schema_name: "public".to_owned(),
                table_name: "t1".to_owned(),
            },
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
