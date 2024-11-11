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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Decimal};
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use simd_json::prelude::ArrayTrait;
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use with_options::WithOptions;

use super::{
    SinkError, SinkWriterMetrics, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

fn default_max_batch_rows() -> usize {
    1024
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct PostgresConfig {
    #[serde(rename = "postgres.host")]
    pub host: String,
    #[serde(rename = "postgres.port")]
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    #[serde(rename = "postgres.user")]
    pub user: String,
    #[serde(rename = "postgres.password")]
    pub password: String,
    #[serde(rename = "postgres.database")]
    pub database: String,
    #[serde(rename = "postgres.table")]
    pub table: String,
    #[serde(
        rename = "postgres.max_batch_rows",
        default = "default_max_batch_rows"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
}

impl PostgresConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<PostgresConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct PostgresSink {
    pub config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl PostgresSink {
    pub fn new(
        mut config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for PostgresSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = PostgresConfig::from_btreemap(param.properties)?;
        PostgresSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl Sink for PostgresSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<PostgresSinkWriter>;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert Postgres sink (please define in `primary_key` field)")));
        }

        // NOTE(kwannoel): postgresql types should be superset of rw.
        // for f in self.schema.fields() {
        //     check_data_type_compatibility(&f.data_type)?;
        // }

        // Query table metadata from SQL Server.
        let mut sql_server_table_metadata = HashMap::new();
        let mut sql_client = PostgresClient::new(&self.config).await?;
        let query_table_metadata_error = || {
            SinkError::Postgres(anyhow!(format!(
                "Postgres table {} metadata error",
                self.config.table
            )))
        };

        // Query the column information from the `information_schema.columns` table
        let table_name = "your_table_name";
        let rows = sqlx::query(
            r#"
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position;
        "#
        )
            .bind(table_name)
            .fetch_all(&pool)
            .await?;

        // Print the schema details
        for row in rows {
            let column_name: &str = row.get("column_name");
            let data_type: &str = row.get("data_type");
            let is_nullable: &str = row.get("is_nullable");

            println!("Column: {}, Type: {}, Nullable: {}", column_name, data_type, is_nullable);
        }

        // Validate Column name and Primary Key
        for (idx, col) in self.schema.fields().iter().enumerate() {
            let rw_is_pk = self.pk_indices.contains(&idx);
            match sql_server_table_metadata.get(&col.name) {
                None => {
                    return Err(SinkError::Postgres(anyhow!(format!(
                        "column {} not found in the downstream SQL Server table {}",
                        col.name, self.config.table
                    ))));
                }
                Some(sql_server_is_pk) => {
                    if self.is_append_only {
                        continue;
                    }
                    if rw_is_pk && !*sql_server_is_pk {
                        return Err(SinkError::Postgres(anyhow!(format!(
                            "column {} specified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name, self.config.table,
                        ))));
                    }
                    if !rw_is_pk && *sql_server_is_pk {
                        return Err(SinkError::Postgres(anyhow!(format!(
                            "column {} unspecified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name, self.config.table,
                        ))));
                    }
                }
            }
        }

        if !self.is_append_only {
            let sql_server_pk_count = sql_server_table_metadata
                .values()
                .filter(|is_pk| **is_pk)
                .count();
            if sql_server_pk_count != self.pk_indices.len() {
                return Err(SinkError::Postgres(anyhow!(format!(
                    "primary key does not match between RisingWave sink ({}) and SQL Server table {} ({})",
                    self.pk_indices.len(),
                    self.config.table,
                    sql_server_pk_count,
                ))));
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }
}

enum SqlOp {
    Insert(OwnedRow),
    Merge(OwnedRow),
    Delete(OwnedRow),
}

pub struct PostgresSinkWriter {
    config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    sql_client: PostgresClient,
    ops: Vec<SqlOp>,
}

impl PostgresSinkWriter {
    async fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let sql_client = PostgresClient::new(&config).await?;
        let writer = Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            sql_client,
            ops: vec![],
        };
        Ok(writer)
    }

    async fn delete_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Delete(row.into_owned_row()));
        Ok(())
    }

    async fn upsert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Merge(row.into_owned_row()));
        Ok(())
    }

    async fn insert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Insert(row.into_owned_row()));
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        use std::fmt::Write;
        if self.ops.is_empty() {
            return Ok(());
        }
        let mut query_str = String::new();
        let col_num = self.schema.fields.len();
        let mut next_param_id = 1;
        let non_pk_col_indices = (0..col_num)
            .filter(|idx| !self.pk_indices.contains(idx))
            .collect::<Vec<usize>>();
        let all_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let all_source_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[SOURCE].[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_match = self
            .pk_indices
            .iter()
            .map(|idx| {
                format!(
                    "[SOURCE].[{}]=[TARGET].[{}]",
                    &self.schema[*idx].name, &self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let param_placeholders = |param_id: &mut usize| {
            let params = (*param_id..(*param_id + col_num))
                .map(|i| format!("@P{}", i))
                .collect::<Vec<_>>()
                .join(",");
            *param_id += col_num;
            params
        };
        let set_all_source_col = non_pk_col_indices
            .iter()
            .map(|idx| {
                format!(
                    "[{}]=[SOURCE].[{}]",
                    &self.schema[*idx].name, &self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        // TODO: avoid repeating the SQL
        for op in &self.ops {
            match op {
                SqlOp::Insert(_) => {
                    write!(
                        &mut query_str,
                        "INSERT INTO [{}] ({}) VALUES ({});",
                        self.config.table,
                        all_col_names,
                        param_placeholders(&mut next_param_id),
                    )
                    .unwrap();
                }
                SqlOp::Merge(_) => {
                    write!(
                        &mut query_str,
                        r#"MERGE [{}] AS [TARGET]
                        USING (VALUES ({})) AS [SOURCE] ({})
                        ON {}
                        WHEN MATCHED THEN UPDATE SET {}
                        WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                        self.config.table,
                        param_placeholders(&mut next_param_id),
                        all_col_names,
                        pk_match,
                        set_all_source_col,
                        all_col_names,
                        all_source_col_names,
                    )
                    .unwrap();
                }
                SqlOp::Delete(_) => {
                    write!(
                        &mut query_str,
                        r#"DELETE FROM [{}] WHERE {};"#,
                        self.config.table,
                        self.pk_indices
                            .iter()
                            .map(|idx| {
                                let condition =
                                    format!("[{}]=@P{}", self.schema[*idx].name, next_param_id);
                                next_param_id += 1;
                                condition
                            })
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    )
                    .unwrap();
                }
            }
        }

        let mut query = Query::new(query_str);
        for op in self.ops.drain(..) {
            match op {
                SqlOp::Insert(row) => {
                    bind_params(&mut query, row, &self.schema, 0..col_num)?;
                }
                SqlOp::Merge(row) => {
                    bind_params(&mut query, row, &self.schema, 0..col_num)?;
                }
                SqlOp::Delete(row) => {
                    bind_params(
                        &mut query,
                        row,
                        &self.schema,
                        self.pk_indices.iter().copied(),
                    )?;
                }
            }
        }
        query.execute(&mut self.sql_client.inner_client).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for PostgresSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    if self.is_append_only {
                        self.insert_one(row).await?;
                    } else {
                        self.upsert_one(row).await?;
                    }
                }
                Op::UpdateInsert => {
                    debug_assert!(!self.is_append_only);
                    self.upsert_one(row).await?;
                }
                Op::Delete => {
                    debug_assert!(!self.is_append_only);
                    self.delete_one(row).await?;
                }
                Op::UpdateDelete => {}
            }
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.flush().await?;
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct PostgresClient {
    pub inner_client: Client<tokio_util::compat::Compat<TcpStream>>,
}

impl PostgresClient {
    async fn new(msconfig: &PostgresConfig) -> Result<Self> {
        let mut config = Config::new();
        config.host(&msconfig.host);
        config.port(msconfig.port);
        config.authentication(AuthMethod::sql_server(&msconfig.user, &msconfig.password));
        config.database(&msconfig.database);
        config.trust_cert();
        Self::new_with_config(config).await
    }

    pub async fn new_with_config(mut config: Config) -> Result<Self> {
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .context("failed to connect to sql server")
            .map_err(SinkError::Postgres)?;
        tcp.set_nodelay(true)
            .context("failed to setting nodelay when connecting to sql server")
            .map_err(SinkError::Postgres)?;

        let client = match Client::connect(config.clone(), tcp.compat_write()).await {
            // Connection successful.
            Ok(client) => client,
            // The server wants us to redirect to a different address
            Err(tiberius::error::Error::Routing { host, port }) => {
                config.host(&host);
                config.port(port);
                let tcp = TcpStream::connect(config.get_addr())
                    .await
                    .context("failed to connect to sql server after routing")
                    .map_err(SinkError::Postgres)?;
                tcp.set_nodelay(true)
                    .context(
                        "failed to setting nodelay when connecting to sql server after routing",
                    )
                    .map_err(SinkError::Postgres)?;
                // we should not have more than one redirect, so we'll short-circuit here.
                Client::connect(config, tcp.compat_write()).await?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            inner_client: client,
        })
    }
}

fn bind_params(
    query: &mut Query<'_>,
    row: impl Row,
    schema: &Schema,
    col_indices: impl Iterator<Item = usize>,
) -> Result<()> {
    use risingwave_common::types::ScalarRefImpl;
    for col_idx in col_indices {
        match row.datum_at(col_idx) {
            Some(data_ref) => match data_ref {
                ScalarRefImpl::Int16(v) => query.bind(v),
                ScalarRefImpl::Int32(v) => query.bind(v),
                ScalarRefImpl::Int64(v) => query.bind(v),
                ScalarRefImpl::Float32(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Float64(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Utf8(v) => query.bind(v.to_owned()),
                ScalarRefImpl::Bool(v) => query.bind(v),
                ScalarRefImpl::Decimal(v) => match v {
                    Decimal::Normalized(d) => {
                        query.bind(decimal_to_sql(&d));
                    }
                    Decimal::NaN | Decimal::PositiveInf | Decimal::NegativeInf => {
                        tracing::warn!(
                            "Inf, -Inf, Nan in RisingWave decimal is converted into SQL Server null!"
                        );
                        query.bind(None as Option<Numeric>);
                    }
                },
                ScalarRefImpl::Date(v) => query.bind(v.0),
                ScalarRefImpl::Timestamp(v) => query.bind(v.0),
                ScalarRefImpl::Timestamptz(v) => query.bind(v.timestamp_micros()),
                ScalarRefImpl::Time(v) => query.bind(v.0),
                ScalarRefImpl::Bytea(v) => query.bind(v.to_vec()),
                ScalarRefImpl::Interval(_) => return Err(data_type_not_supported("Interval")),
                ScalarRefImpl::Jsonb(_) => return Err(data_type_not_supported("Jsonb")),
                ScalarRefImpl::Struct(_) => return Err(data_type_not_supported("Struct")),
                ScalarRefImpl::List(_) => return Err(data_type_not_supported("List")),
                ScalarRefImpl::Int256(_) => return Err(data_type_not_supported("Int256")),
                ScalarRefImpl::Serial(_) => return Err(data_type_not_supported("Serial")),
                ScalarRefImpl::Map(_) => return Err(data_type_not_supported("Map")),
            },
            None => match schema[col_idx].data_type {
                DataType::Boolean => {
                    query.bind(None as Option<bool>);
                }
                DataType::Int16 => {
                    query.bind(None as Option<i16>);
                }
                DataType::Int32 => {
                    query.bind(None as Option<i32>);
                }
                DataType::Int64 => {
                    query.bind(None as Option<i64>);
                }
                DataType::Float32 => {
                    query.bind(None as Option<f32>);
                }
                DataType::Float64 => {
                    query.bind(None as Option<f64>);
                }
                DataType::Decimal => {
                    query.bind(None as Option<Numeric>);
                }
                DataType::Date => {
                    query.bind(None as Option<chrono::NaiveDate>);
                }
                DataType::Time => {
                    query.bind(None as Option<chrono::NaiveTime>);
                }
                DataType::Timestamp => {
                    query.bind(None as Option<chrono::NaiveDateTime>);
                }
                DataType::Timestamptz => {
                    query.bind(None as Option<i64>);
                }
                DataType::Varchar => {
                    query.bind(None as Option<String>);
                }
                DataType::Bytea => {
                    query.bind(None as Option<Vec<u8>>);
                }
                DataType::Interval => return Err(data_type_not_supported("Interval")),
                DataType::Struct(_) => return Err(data_type_not_supported("Struct")),
                DataType::List(_) => return Err(data_type_not_supported("List")),
                DataType::Jsonb => return Err(data_type_not_supported("Jsonb")),
                DataType::Serial => return Err(data_type_not_supported("Serial")),
                DataType::Int256 => return Err(data_type_not_supported("Int256")),
                DataType::Map(_) => return Err(data_type_not_supported("Map")),
            },
        };
    }
    Ok(())
}

fn data_type_not_supported(data_type_name: &str) -> SinkError {
    SinkError::Postgres(anyhow!(format!(
        "{data_type_name} is not supported in SQL Server"
    )))
}

fn check_data_type_compatibility(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal
        | DataType::Date
        | DataType::Varchar
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Bytea => Ok(()),
        DataType::Interval => Err(data_type_not_supported("Interval")),
        DataType::Struct(_) => Err(data_type_not_supported("Struct")),
        DataType::List(_) => Err(data_type_not_supported("List")),
        DataType::Jsonb => Err(data_type_not_supported("Jsonb")),
        DataType::Serial => Err(data_type_not_supported("Serial")),
        DataType::Int256 => Err(data_type_not_supported("Int256")),
        DataType::Map(_) => Err(data_type_not_supported("Map")),
    }
}

/// The implementation is copied from tiberius crate.
fn decimal_to_sql(decimal: &rust_decimal::Decimal) -> Numeric {
    let unpacked = decimal.unpack();

    let mut value = (((unpacked.hi as u128) << 64)
        + ((unpacked.mid as u128) << 32)
        + unpacked.lo as u128) as i128;

    if decimal.is_sign_negative() {
        value = -value;
    }

    Numeric::new_with_scale(value, decimal.scale() as u8)
}
