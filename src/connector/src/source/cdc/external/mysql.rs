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

use std::collections::HashMap;

use anyhow::Context;
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use mysql_async::prelude::*;
use mysql_common::params::Params;
use mysql_common::value::Value;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, OFFSET_COLUMN_NAME};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use serde_derive::{Deserialize, Serialize};

use crate::error::{ConnectorError, ConnectorResult};
use crate::source::cdc::external::{
    mysql_row_to_owned_row, CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig,
    ExternalTableReader, SchemaTableName, SslMode,
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
    columns: Vec<ColumnDesc>,
    pk_names: Vec<String>,
}

impl MySqlExternalTable {
    pub async fn connect(config: ExternalTableConfig) -> ConnectorResult<Self> {
        // TODO: connect to external db and read the schema
        tracing::debug!("connect to mysql");

        Ok(Self {
            columns: vec![],
            pk_names: vec![],
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.columns
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }
}

pub struct MySqlExternalTableReader {
    rw_schema: Schema,
    field_names: String,
    // use mutex to provide shared mutable access to the connection
    conn: tokio::sync::Mutex<mysql_async::Conn>,
}

impl ExternalTableReader for MySqlExternalTableReader {
    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        let mut conn = self.conn.lock().await;

        let sql = "SHOW MASTER STATUS".to_string();
        let mut rs = conn.query::<mysql_async::Row, _>(sql).await?;
        let row = rs
            .iter_mut()
            .exactly_one()
            .ok()
            .context("expect exactly one row when reading binlog offset")?;

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
}

impl MySqlExternalTableReader {
    pub async fn new(
        with_properties: HashMap<String, String>,
        rw_schema: Schema,
    ) -> ConnectorResult<Self> {
        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(with_properties).unwrap(),
        )
        .context("failed to extract mysql connector properties")?;

        let mut opts_builder = mysql_async::OptsBuilder::default()
            .user(Some(config.username))
            .pass(Some(config.password))
            .ip_or_hostname(config.host)
            .tcp_port(config.port.parse::<u16>().unwrap())
            .db_name(Some(config.database));

        opts_builder = match config.sslmode {
            SslMode::Disabled | SslMode::Preferred => opts_builder.ssl_opts(None),
            SslMode::Required => {
                let ssl_without_verify = mysql_async::SslOpts::default()
                    .with_danger_accept_invalid_certs(true)
                    .with_danger_skip_domain_validation(true);
                opts_builder.ssl_opts(Some(ssl_without_verify))
            }
        };

        let conn = mysql_async::Conn::new(mysql_async::Opts::from(opts_builder)).await?;

        let field_names = rw_schema
            .fields
            .iter()
            .filter(|f| f.name != OFFSET_COLUMN_NAME)
            .map(|f| Self::quote_column(f.name.as_str()))
            .join(",");

        Ok(Self {
            rw_schema,
            field_names,
            conn: tokio::sync::Mutex::new(conn),
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

        let mut conn = self.conn.lock().await;

        // Set session timezone to UTC
        conn.exec_drop("SET time_zone = \"+00:00\"", ()).await?;

        if start_pk_row.is_none() {
            let rs_stream = sql.stream::<mysql_async::Row, _>(&mut *conn).await?;
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
                        ConnectorResult::Ok((pk.clone(), val))
                    } else {
                        bail!("primary key {} cannot be null", pk);
                    }
                })
                .try_collect::<_, _, ConnectorError>()?;

            let rs_stream = sql
                .with(Params::from(params))
                .stream::<mysql_async::Row, _>(&mut *conn)
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
    pub(crate) fn filter_expression(columns: &[String]) -> String {
        let mut conditions = vec![];
        // push the first condition
        conditions.push(format!(
            "({} > :{})",
            Self::quote_column(&columns[0]),
            columns[0]
        ));
        for i in 2..=columns.len() {
            // '=' condition
            let mut condition = String::new();
            for (j, col) in columns.iter().enumerate().take(i - 1) {
                if j == 0 {
                    condition.push_str(&format!("({} = :{})", Self::quote_column(col), col));
                } else {
                    condition.push_str(&format!(" AND ({} = :{})", Self::quote_column(col), col));
                }
            }
            // '>' condition
            condition.push_str(&format!(
                " AND ({} > :{})",
                Self::quote_column(&columns[i - 1]),
                columns[i - 1]
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
