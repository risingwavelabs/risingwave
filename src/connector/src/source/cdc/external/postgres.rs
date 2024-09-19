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

use std::cmp::Ordering;
use std::collections::HashMap;

use anyhow::{anyhow, Context};
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, StructType};
use risingwave_common::util::iter_util::ZipEqFast;
use sea_schema::postgres::def::{ColumnType, TableInfo};
use sea_schema::postgres::discovery::SchemaDiscovery;
use serde_derive::{Deserialize, Serialize};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::PgPool;
use thiserror_ext::AsReport;
use tokio_postgres::types::PgLsn;
use tokio_postgres::NoTls;

use crate::error::{ConnectorError, ConnectorResult};
use crate::parser::postgres_row_to_owned_row;
use crate::parser::scalar_adapter::ScalarAdapter;
#[cfg(not(madsim))]
use crate::source::cdc::external::maybe_tls_connector::MaybeMakeTlsConnector;
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName, SslMode,
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

pub struct PostgresExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl PostgresExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        tracing::debug!("connect to postgres external table");
        let mut options = PgConnectOptions::new()
            .username(&config.username)
            .password(&config.password)
            .host(&config.host)
            .port(config.port.parse::<u16>().unwrap())
            .database(&config.database)
            .ssl_mode(match config.ssl_mode {
                SslMode::Disabled => PgSslMode::Disable,
                SslMode::Preferred => PgSslMode::Prefer,
                SslMode::Required => PgSslMode::Require,
                SslMode::VerifyCa => PgSslMode::VerifyCa,
                SslMode::VerifyFull => PgSslMode::VerifyFull,
            });

        if config.ssl_mode == SslMode::VerifyCa || config.ssl_mode == SslMode::VerifyFull {
            if let Some(ref root_cert) = config.ssl_root_cert {
                options = options.ssl_root_cert(root_cert.as_str());
            }
        }

        let connection = PgPool::connect_with(options).await?;
        let schema_discovery = SchemaDiscovery::new(connection, config.schema.as_str());
        // fetch column schema and primary key
        let empty_map = HashMap::new();
        let table_schema = schema_discovery
            .discover_table(
                TableInfo {
                    name: config.table.clone(),
                    of_type: None,
                },
                &empty_map,
            )
            .await?;

        let mut column_descs = vec![];
        for col in &table_schema.columns {
            let data_type = type_to_rw_type(&col.col_type)?;
            column_descs.push(ColumnDesc::named(
                col.name.clone(),
                ColumnId::placeholder(),
                data_type,
            ));
        }

        if table_schema.primary_key_constraints.is_empty() {
            return Err(anyhow!("Postgres table doesn't define the primary key").into());
        }
        let mut pk_names = vec![];
        table_schema.primary_key_constraints.iter().for_each(|pk| {
            pk_names.extend(pk.columns.clone());
        });

        Ok(Self {
            column_descs,
            pk_names,
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

fn type_to_rw_type(col_type: &ColumnType) -> ConnectorResult<DataType> {
    let dtype = match col_type {
        ColumnType::SmallInt | ColumnType::SmallSerial => DataType::Int16,
        ColumnType::Integer | ColumnType::Serial => DataType::Int32,
        ColumnType::BigInt | ColumnType::BigSerial => DataType::Int64,
        ColumnType::Money | ColumnType::Decimal(_) | ColumnType::Numeric(_) => DataType::Decimal,
        ColumnType::Real => DataType::Float32,
        ColumnType::DoublePrecision => DataType::Float64,
        ColumnType::Varchar(_) | ColumnType::Char(_) | ColumnType::Text => DataType::Varchar,
        ColumnType::Bytea => DataType::Bytea,
        ColumnType::Timestamp(_) => DataType::Timestamp,
        ColumnType::TimestampWithTimeZone(_) => DataType::Timestamptz,
        ColumnType::Date => DataType::Date,
        ColumnType::Time(_) | ColumnType::TimeWithTimeZone(_) => DataType::Time,
        ColumnType::Interval(_) => DataType::Interval,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Point => DataType::Struct(StructType::new(vec![
            ("x", DataType::Float32),
            ("y", DataType::Float32),
        ])),
        ColumnType::Uuid => DataType::Varchar,
        ColumnType::Xml => DataType::Varchar,
        ColumnType::Json => DataType::Jsonb,
        ColumnType::JsonBinary => DataType::Jsonb,
        ColumnType::Array(def) => {
            let item_type = match def.col_type.as_ref() {
                Some(ty) => type_to_rw_type(ty.as_ref())?,
                None => {
                    return Err(anyhow!("ARRAY type missing element type").into());
                }
            };

            DataType::List(Box::new(item_type))
        }
        ColumnType::PgLsn => DataType::Int64,
        ColumnType::Cidr
        | ColumnType::Inet
        | ColumnType::MacAddr
        | ColumnType::MacAddr8
        | ColumnType::Int4Range
        | ColumnType::Int8Range
        | ColumnType::NumRange
        | ColumnType::TsRange
        | ColumnType::TsTzRange
        | ColumnType::DateRange
        | ColumnType::Enum(_) => DataType::Varchar,

        ColumnType::Line => {
            return Err(anyhow!("LINE type not supported").into());
        }
        ColumnType::Lseg => {
            return Err(anyhow!("LSEG type not supported").into());
        }
        ColumnType::Box => {
            return Err(anyhow!("BOX type not supported").into());
        }
        ColumnType::Path => {
            return Err(anyhow!("PATH type not supported").into());
        }
        ColumnType::Polygon => {
            return Err(anyhow!("POLYGON type not supported").into());
        }
        ColumnType::Circle => {
            return Err(anyhow!("CIRCLE type not supported").into());
        }
        ColumnType::Bit(_) => {
            return Err(anyhow!("BIT type not supported").into());
        }
        ColumnType::TsVector => {
            return Err(anyhow!("TSVECTOR type not supported").into());
        }
        ColumnType::TsQuery => {
            return Err(anyhow!("TSQUERY type not supported").into());
        }
        ColumnType::Unknown(name) => {
            // NOTES: user-defined enum type is classified as `Unknown`
            tracing::warn!("Unknown Postgres data type: {name}, map to varchar");
            DataType::Varchar
        }
    };

    Ok(dtype)
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

        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .user(&config.username)
            .password(&config.password)
            .host(&config.host)
            .port(config.port.parse::<u16>().unwrap())
            .dbname(&config.database);

        let (_verify_ca, verify_hostname) = match config.ssl_mode {
            SslMode::VerifyCa => (true, false),
            SslMode::VerifyFull => (true, true),
            _ => (false, false),
        };

        #[cfg(not(madsim))]
        let connector = match config.ssl_mode {
            SslMode::Disabled => {
                pg_config.ssl_mode(tokio_postgres::config::SslMode::Disable);
                MaybeMakeTlsConnector::NoTls(NoTls)
            }
            SslMode::Preferred => {
                pg_config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
                match SslConnector::builder(SslMethod::tls()) {
                    Ok(mut builder) => {
                        // disable certificate verification for `prefer`
                        builder.set_verify(SslVerifyMode::NONE);
                        MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
                    }
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), "SSL connector error");
                        MaybeMakeTlsConnector::NoTls(NoTls)
                    }
                }
            }
            SslMode::Required => {
                pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
                let mut builder = SslConnector::builder(SslMethod::tls())?;
                // disable certificate verification for `require`
                builder.set_verify(SslVerifyMode::NONE);
                MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
            }

            SslMode::VerifyCa | SslMode::VerifyFull => {
                pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
                let mut builder = SslConnector::builder(SslMethod::tls())?;
                if let Some(ssl_root_cert) = config.ssl_root_cert {
                    builder.set_ca_file(ssl_root_cert).map_err(|e| {
                        anyhow!(format!("bad ssl root cert error: {}", e.to_report_string()))
                    })?;
                }
                let mut connector = MakeTlsConnector::new(builder.build());
                if !verify_hostname {
                    connector.set_callback(|config, _| {
                        config.set_verify_hostname(false);
                        Ok(())
                    });
                }
                MaybeMakeTlsConnector::Tls(connector)
            }
        };
        #[cfg(madsim)]
        let connector = NoTls;

        let (client, connection) = pg_config.connect(connector).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e.as_report(), "postgres connection error");
            }
        });

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
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};

    use crate::source::cdc::external::postgres::{
        PostgresExternalTable, PostgresExternalTableReader, PostgresOffset,
    };
    use crate::source::cdc::external::{ExternalTableConfig, ExternalTableReader, SchemaTableName};

    #[ignore]
    #[tokio::test]
    async fn test_postgres_schema() {
        let config = ExternalTableConfig {
            connector: "postgres-cdc".to_string(),
            host: "localhost".to_string(),
            port: "8432".to_string(),
            username: "myuser".to_string(),
            password: "123456".to_string(),
            database: "mydb".to_string(),
            schema: "public".to_string(),
            table: "mytest".to_string(),
            ssl_mode: Default::default(),
            ssl_root_cert: None,
        };

        let table = PostgresExternalTable::connect(config).await.unwrap();

        println!("columns: {:?}", &table.column_descs);
        println!("primary keys: {:?}", &table.pk_names);
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
        let cols = vec!["v1".to_string()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\") > ($1)");

        let cols = vec!["v1".to_string(), "v2".to_string()];
        let expr = PostgresExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(\"v1\", \"v2\") > ($1, $2)");

        let cols = vec!["v1".to_string(), "v2".to_string(), "v3".to_string()];
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
                schema_name: "public".to_string(),
                table_name: "t1".to_string(),
            },
            Some(start_pk),
            vec!["v1".to_string(), "v2".to_string()],
            1000,
        );

        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
