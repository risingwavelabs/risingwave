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

use std::collections::HashMap;

use anyhow::{Context, anyhow};
use chrono::{DateTime, NaiveDateTime};
use futures::stream::BoxStream;
use futures::{StreamExt, pin_mut};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_common::params::Params;
use mysql_common::value::Value;
use risingwave_common::bail;
use risingwave_common::catalog::{CDC_OFFSET_COLUMN_NAME, ColumnDesc, ColumnId, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, F32, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use sea_schema::mysql::def::{ColumnDefault, ColumnKey, ColumnType};
use sea_schema::mysql::discovery::SchemaDiscovery;
use sea_schema::mysql::query::SchemaQueryBuilder;
use sea_schema::sea_query::{Alias, IntoIden};
use serde_derive::{Deserialize, Serialize};
use sqlx::MySqlPool;
use sqlx::mysql::MySqlConnectOptions;
use thiserror_ext::AsReport;

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName, SslMode, mysql_row_to_owned_row,
};

#[derive(Debug, Clone, Default, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MySqlOffset {
    pub filename: String,
    pub position: u64,
}

impl MySqlOffset {
    pub fn new(filename: String, position: u64) -> Self {
        Self { filename, position }
    }
}

impl MySqlOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            filename: dbz_offset
                .source_offset
                .file
                .context("binlog file not found in offset")?,
            position: dbz_offset
                .source_offset
                .pos
                .context("binlog position not found in offset")?,
        })
    }
}

pub struct MySqlExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl MySqlExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        tracing::debug!("connect to mysql");
        let options = MySqlConnectOptions::new()
            .username(&config.username)
            .password(&config.password)
            .host(&config.host)
            .port(config.port.parse::<u16>().unwrap())
            .database(&config.database)
            .ssl_mode(match config.ssl_mode {
                SslMode::Disabled | SslMode::Preferred => sqlx::mysql::MySqlSslMode::Disabled,
                SslMode::Required => sqlx::mysql::MySqlSslMode::Required,
                _ => {
                    return Err(anyhow!("unsupported SSL mode").into());
                }
            });

        let connection = MySqlPool::connect_with(options).await?;
        let mut schema_discovery = SchemaDiscovery::new(connection, config.database.as_str());

        // discover system version first
        let system_info = schema_discovery.discover_system().await?;
        schema_discovery.query = SchemaQueryBuilder::new(system_info.clone());

        let schema = Alias::new(config.database.as_str()).into_iden();
        let table = Alias::new(config.table.as_str()).into_iden();
        let columns = schema_discovery
            .discover_columns(schema, table, &system_info)
            .await?;

        let mut column_descs = vec![];
        let mut pk_names = vec![];
        for col in columns {
            let data_type = mysql_type_to_rw_type(&col.col_type)?;
            // column name in mysql is case-insensitive, convert to lowercase
            let col_name = col.name.to_lowercase();
            let column_desc = if let Some(default) = col.default {
                let snapshot_value = derive_default_value(default.clone(), &data_type)
                    .unwrap_or_else(|e| {
                        tracing::warn!(
                            column = col_name,
                            ?default,
                            %data_type,
                            error = %e.as_report(),
                            "failed to derive column default value, fallback to `NULL`",
                        );
                        None
                    });

                ColumnDesc::named_with_default_value(
                    col_name.clone(),
                    ColumnId::placeholder(),
                    data_type.clone(),
                    snapshot_value,
                )
            } else {
                ColumnDesc::named(col_name.clone(), ColumnId::placeholder(), data_type)
            };

            column_descs.push(column_desc);
            if matches!(col.key, ColumnKey::Primary) {
                pk_names.push(col_name);
            }
        }

        if pk_names.is_empty() {
            return Err(anyhow!("MySQL table doesn't define the primary key").into());
        }

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

fn derive_default_value(default: ColumnDefault, data_type: &DataType) -> ConnectorResult<Datum> {
    let datum = match default {
        ColumnDefault::Null => None,
        ColumnDefault::Int(val) => match data_type {
            DataType::Int16 => Some(ScalarImpl::Int16(val as _)),
            DataType::Int32 => Some(ScalarImpl::Int32(val as _)),
            DataType::Int64 => Some(ScalarImpl::Int64(val)),
            DataType::Varchar => {
                // should be the Enum type which is mapped to Varchar
                Some(ScalarImpl::from(val.to_string()))
            }
            _ => bail!("unexpected default value type for integer"),
        },
        ColumnDefault::Real(val) => match data_type {
            DataType::Float32 => Some(ScalarImpl::Float32(F32::from(val as f32))),
            DataType::Float64 => Some(ScalarImpl::Float64(val.into())),
            DataType::Decimal => Some(ScalarImpl::Decimal(
                Decimal::try_from(val).context("failed to convert default value to decimal")?,
            )),
            _ => bail!("unexpected default value type for real"),
        },
        ColumnDefault::String(mut val) => {
            // mysql timestamp is mapped to timestamptz, we use UTC timezone to
            // interpret its value
            if data_type == &DataType::Timestamptz {
                val = timestamp_val_to_timestamptz(val.as_str())?;
            }
            Some(ScalarImpl::from_text(val.as_str(), data_type).map_err(|e| anyhow!(e)).context(
                "failed to parse mysql default value expression, only constant is supported",
            )?)
        }
        ColumnDefault::CurrentTimestamp | ColumnDefault::CustomExpr(_) => {
            bail!("MySQL CURRENT_TIMESTAMP and custom expression default value not supported")
        }
    };
    Ok(datum)
}

pub fn timestamp_val_to_timestamptz(value_text: &str) -> ConnectorResult<String> {
    let format = "%Y-%m-%d %H:%M:%S";
    let naive_datetime = NaiveDateTime::parse_from_str(value_text, format)
        .map_err(|err| anyhow!("failed to parse mysql timestamp value").context(err))?;
    let postgres_timestamptz: DateTime<chrono::Utc> =
        DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive_datetime, chrono::Utc);
    Ok(postgres_timestamptz
        .format("%Y-%m-%d %H:%M:%S%:z")
        .to_string())
}

pub fn type_name_to_mysql_type(ty_name: &str) -> Option<ColumnType> {
    macro_rules! column_type {
        ($($name:literal => $variant:ident),* $(,)?) => {
            match ty_name.to_lowercase().as_str() {
                $(
                    $name => Some(ColumnType::$variant(Default::default())),
                )*
                "json" => Some(ColumnType::Json),
                "date" => Some(ColumnType::Date),
                "bool" => Some(ColumnType::Bool),
                "tinyblob" => Some(ColumnType::TinyBlob),
                "mediumblob" => Some(ColumnType::MediumBlob),
                "longblob" => Some(ColumnType::LongBlob),
                _ => None,
            }
        };
    }

    column_type! {
        "bit" => Bit,
        "tinyint" => TinyInt,
        "smallint" => SmallInt,
        "mediumint" => MediumInt,
        "int" => Int,
        "bigint" => BigInt,
        "decimal" => Decimal,
        "float" => Float,
        "double" => Double,
        "time" => Time,
        "datetime" => DateTime,
        "timestamp" => Timestamp,
        "char" => Char,
        "nchar" => NChar,
        "varchar" => Varchar,
        "nvarchar" => NVarchar,
        "binary" => Binary,
        "varbinary" => Varbinary,
        "text" => Text,
        "tinytext" => TinyText,
        "mediumtext" => MediumText,
        "longtext" => LongText,
        "blob" => Blob,
        "enum" => Enum,
        "set" => Set,
        "geometry" => Geometry,
        "point" => Point,
        "linestring" => LineString,
        "polygon" => Polygon,
        "multipoint" => MultiPoint,
        "multilinestring" => MultiLineString,
        "multipolygon" => MultiPolygon,
        "geometrycollection" => GeometryCollection,
    }
}

pub fn mysql_type_to_rw_type(col_type: &ColumnType) -> ConnectorResult<DataType> {
    let dtype = match col_type {
        ColumnType::Serial => DataType::Int32,
        ColumnType::Bit(attr) => {
            if let Some(1) = attr.maximum {
                DataType::Boolean
            } else {
                return Err(
                    anyhow!("BIT({}) type not supported", attr.maximum.unwrap_or(0)).into(),
                );
            }
        }
        ColumnType::TinyInt(_) | ColumnType::SmallInt(_) => DataType::Int16,
        ColumnType::Bool => DataType::Boolean,
        ColumnType::MediumInt(_) => DataType::Int32,
        ColumnType::Int(_) => DataType::Int32,
        ColumnType::BigInt(_) => DataType::Int64,
        ColumnType::Decimal(_) => DataType::Decimal,
        ColumnType::Float(_) => DataType::Float32,
        ColumnType::Double(_) => DataType::Float64,
        ColumnType::Date => DataType::Date,
        ColumnType::Time(_) => DataType::Time,
        ColumnType::DateTime(_) => DataType::Timestamp,
        ColumnType::Timestamp(_) => DataType::Timestamptz,
        ColumnType::Year => DataType::Int32,
        ColumnType::Char(_)
        | ColumnType::NChar(_)
        | ColumnType::Varchar(_)
        | ColumnType::NVarchar(_) => DataType::Varchar,
        ColumnType::Binary(_) | ColumnType::Varbinary(_) => DataType::Bytea,
        ColumnType::Text(_)
        | ColumnType::TinyText(_)
        | ColumnType::MediumText(_)
        | ColumnType::LongText(_) => DataType::Varchar,
        ColumnType::Blob(_)
        | ColumnType::TinyBlob
        | ColumnType::MediumBlob
        | ColumnType::LongBlob => DataType::Bytea,
        ColumnType::Enum(_) => DataType::Varchar,
        ColumnType::Json => DataType::Jsonb,
        ColumnType::Set(_) => {
            return Err(anyhow!("SET type not supported").into());
        }
        ColumnType::Geometry(_) => {
            return Err(anyhow!("GEOMETRY type not supported").into());
        }
        ColumnType::Point(_) => {
            return Err(anyhow!("POINT type not supported").into());
        }
        ColumnType::LineString(_) => {
            return Err(anyhow!("LINE string type not supported").into());
        }
        ColumnType::Polygon(_) => {
            return Err(anyhow!("POLYGON type not supported").into());
        }
        ColumnType::MultiPoint(_) => {
            return Err(anyhow!("MULTI POINT type not supported").into());
        }
        ColumnType::MultiLineString(_) => {
            return Err(anyhow!("MULTI LINE STRING type not supported").into());
        }
        ColumnType::MultiPolygon(_) => {
            return Err(anyhow!("MULTI POLYGON type not supported").into());
        }
        ColumnType::GeometryCollection(_) => {
            return Err(anyhow!("GEOMETRY COLLECTION type not supported").into());
        }
        ColumnType::Unknown(_) => {
            return Err(anyhow!("Unknown MySQL data type").into());
        }
    };

    Ok(dtype)
}

pub struct MySqlExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    pool: mysql_async::Pool,
}

impl ExternalTableReader for MySqlExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut conn = self.pool.get_conn().await?;

        let sql = "SHOW MASTER STATUS".to_owned();
        let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
        let row = rs
            .iter_mut()
            .exactly_one()
            .ok()
            .context("expect exactly one row when reading binlog offset")?;
        drop(conn);
        Ok(CdcOffset::MySql(MySqlOffset {
            filename: row.take("File").unwrap(),
            position: row.take("Position").unwrap(),
        }))
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

    async fn disconnect(&self) -> ConnectorResult<()> {
        self.pool.clone().disconnect().await?;
        Ok(())
    }
}

impl MySqlExternalTableReader {
    pub fn new(config: ExternalTableConfig, rw_schema: Schema) -> ConnectorResult<Self> {
        let mut opts_builder = mysql_async::OptsBuilder::default()
            .user(Some(config.username))
            .pass(Some(config.password))
            .ip_or_hostname(config.host)
            .tcp_port(config.port.parse::<u16>().unwrap())
            .db_name(Some(config.database));

        opts_builder = match config.ssl_mode {
            SslMode::Disabled | SslMode::Preferred => opts_builder.ssl_opts(None),
            // verify-ca and verify-full are same as required for mysql now
            SslMode::Required | SslMode::VerifyCa | SslMode::VerifyFull => {
                let ssl_without_verify = mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true);
                opts_builder.ssl_opts(Some(ssl_without_verify))
            }
        };
        let pool = mysql_async::Pool::new(opts_builder);

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != CDC_OFFSET_COLUMN_NAME)
            .map(|f| Self::quote_column(f.name.as_str()))
            .join(",");

        Ok(Self {
            rw_schema,
            field_names,
            pool,
        })
    }

    pub fn get_normalized_table_name(table_name: &SchemaTableName) -> String {
        // schema name is the database name in mysql
        format!("`{}`.`{}`", table_name.schema_name, table_name.table_name)
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        Box::new(move |offset| {
            Ok(CdcOffset::MySql(MySqlOffset::parse_debezium_offset(
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
        limit: u32,
    ) {
        let order_key = primary_keys
            .iter()
            .map(|col| Self::quote_column(col))
            .join(",");
        let sql = if start_pk_row.is_none() {
            format!(
                "SELECT {} FROM {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                order_key,
            )
        } else {
            let filter_expr = Self::filter_expression(&primary_keys);
            format!(
                "SELECT {} FROM {} WHERE {} ORDER BY {} LIMIT {limit}",
                self.field_names,
                Self::get_normalized_table_name(&table_name),
                filter_expr,
                order_key,
            )
        };

        let mut conn = self.pool.get_conn().await?;
        // Set session timezone to UTC
        conn.exec_drop("SET time_zone = \"+00:00\"", ()).await?;

        if start_pk_row.is_none() {
            let rs_stream = sql.stream::<mysql_async::Row, _>(&mut conn).await?;
            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                Ok::<_, ConnectorError>(mysql_row_to_owned_row(&mut row, &self.rw_schema))
            });
            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        } else {
            let field_map = self
                .rw_schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.data_type.clone()))
                .collect::<HashMap<_, _>>();

            // fill in start primary key params
            let params: Vec<_> = primary_keys
                .iter()
                .zip_eq_fast(start_pk_row.unwrap().into_iter())
                .map(|(pk, datum)| {
                    if let Some(value) = datum {
                        let ty = field_map.get(pk.as_str()).unwrap();
                        let val = match ty {
                            DataType::Boolean => Value::from(value.into_bool()),
                            DataType::Int16 => Value::from(value.into_int16()),
                            DataType::Int32 => Value::from(value.into_int32()),
                            DataType::Int64 => Value::from(value.into_int64()),
                            DataType::Float32 => Value::from(value.into_float32().into_inner()),
                            DataType::Float64 => Value::from(value.into_float64().into_inner()),
                            DataType::Varchar => Value::from(String::from(value.into_utf8())),
                            DataType::Date => Value::from(value.into_date().0),
                            DataType::Time => Value::from(value.into_time().0),
                            DataType::Timestamp => Value::from(value.into_timestamp().0),
                            _ => bail!("unsupported primary key data type: {}", ty),
                        };
                        ConnectorResult::Ok((pk.to_lowercase(), val))
                    } else {
                        bail!("primary key {} cannot be null", pk);
                    }
                })
                .try_collect::<_, _, ConnectorError>()?;

            tracing::debug!("snapshot read params: {:?}", &params);
            let rs_stream = sql
                .with(Params::from(params))
                .stream::<mysql_async::Row, _>(&mut conn)
                .await?;

            let row_stream = rs_stream.map(|row| {
                // convert mysql row into OwnedRow
                let mut row = row?;
                Ok::<_, ConnectorError>(mysql_row_to_owned_row(&mut row, &self.rw_schema))
            });
            pin_mut!(row_stream);
            #[for_await]
            for row in row_stream {
                let row = row?;
                yield row;
            }
        };
    }

    // mysql cannot leverage the given key to narrow down the range of scan,
    // we need to rewrite the comparison conditions by our own.
    // (a, b) > (x, y) => (`a` > x) OR ((`a` = x) AND (`b` > y))
    fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!(
            "({} > :{})",
            Self::quote_column(&columns[0]),
            columns[0].to_lowercase()
        ));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!(
                        "({} = :{})",
                        Self::quote_column(col),
                        col.to_lowercase()
                    ));
                } else {
                    condition.push_str(&format!(
                        " AND ({} = :{})",
                        Self::quote_column(col),
                        col.to_lowercase()
                    ));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > :{})",
                Self::quote_column(&columns[i - 1]),
                columns[i - 1].to_lowercase()
            ));
            conditions.push(format!("({})", condition));
        }
        if columns.len() > 1 {
            conditions.join(" OR ")
        } else {
            conditions.join("")
        }
    }

    fn quote_column(column: &str) -> String {
        format!("`{}`", column)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use futures::pin_mut;
    use futures_async_stream::for_await;
    use maplit::{convert_args, hashmap};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
    use risingwave_common::types::DataType;

    use crate::source::cdc::external::mysql::MySqlExternalTable;
    use crate::source::cdc::external::{
        CdcOffset, ExternalTableConfig, ExternalTableReader, MySqlExternalTableReader, MySqlOffset,
        SchemaTableName,
    };

    #[ignore]
    #[tokio::test]
    async fn test_mysql_schema() {
        let config = ExternalTableConfig {
            connector: "mysql-cdc".to_owned(),
            host: "localhost".to_owned(),
            port: "8306".to_owned(),
            username: "root".to_owned(),
            password: "123456".to_owned(),
            database: "mydb".to_owned(),
            schema: "".to_owned(),
            table: "part".to_owned(),
            ssl_mode: Default::default(),
            ssl_root_cert: None,
            encrypt: "false".to_owned(),
        };

        let table = MySqlExternalTable::connect(config).await.unwrap();
        println!("columns: {:?}", &table.column_descs);
        println!("primary keys: {:?}", &table.pk_names);
    }

    #[test]
    fn test_mysql_filter_expr() {
        let cols = vec!["id".to_owned()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(expr, "(`id` > :id)");

        let cols = vec!["aa".to_owned(), "bb".to_owned(), "cc".to_owned()];
        let expr = MySqlExternalTableReader::filter_expression(&cols);
        assert_eq!(
            expr,
            "(`aa` > :aa) OR ((`aa` = :aa) AND (`bb` > :bb)) OR ((`aa` = :aa) AND (`bb` = :bb) AND (`cc` > :cc))"
        );
    }

    #[test]
    fn test_mysql_binlog_offset() {
        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let off1_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 1062363217, "snapshot": true }, "isHeartbeat": false }"#;
        let off2_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000007", "pos": 659687560, "snapshot": true }, "isHeartbeat": false }"#;
        let off3_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;
        let off4_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000008", "pos": 7665875, "snapshot": true }, "isHeartbeat": false }"#;

        let off0 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off0_str).unwrap());
        let off1 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off1_str).unwrap());
        let off2 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off2_str).unwrap());
        let off3 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off3_str).unwrap());
        let off4 = CdcOffset::MySql(MySqlOffset::parse_debezium_offset(off4_str).unwrap());

        assert!(off0 <= off1);
        assert!(off1 > off2);
        assert!(off2 < off3);
        assert_eq!(off3, off4);
    }

    // manual test case
    #[ignore]
    #[tokio::test]
    async fn test_mysql_table_reader() {
        let columns = vec![
            ColumnDesc::named("v1", ColumnId::new(1), DataType::Int32),
            ColumnDesc::named("v2", ColumnId::new(2), DataType::Decimal),
            ColumnDesc::named("v3", ColumnId::new(3), DataType::Varchar),
            ColumnDesc::named("v4", ColumnId::new(4), DataType::Date),
        ];
        let rw_schema = Schema {
            fields: columns.iter().map(Field::from).collect(),
        };
        let props: HashMap<String, String> = convert_args!(hashmap!(
                "hostname" => "localhost",
                "port" => "8306",
                "username" => "root",
                "password" => "123456",
                "database.name" => "mytest",
                "table.name" => "t1"));

        let config =
            serde_json::from_value::<ExternalTableConfig>(serde_json::to_value(props).unwrap())
                .unwrap();
        let reader = MySqlExternalTableReader::new(config, rw_schema).unwrap();
        let offset = reader.current_cdc_offset().await.unwrap();
        println!("BinlogOffset: {:?}", offset);

        let off0_str = r#"{ "sourcePartition": { "server": "test" }, "sourceOffset": { "ts_sec": 1670876905, "file": "binlog.000001", "pos": 105622, "snapshot": true }, "isHeartbeat": false }"#;
        let parser = MySqlExternalTableReader::get_cdc_offset_parser();
        println!("parsed offset: {:?}", parser(off0_str).unwrap());
        let table_name = SchemaTableName {
            schema_name: "mytest".to_owned(),
            table_name: "t1".to_owned(),
        };

        let stream = reader.snapshot_read(table_name, None, vec!["v1".to_owned()], 1000);
        pin_mut!(stream);
        #[for_await]
        for row in stream {
            println!("OwnedRow: {:?}", row);
        }
    }
}
