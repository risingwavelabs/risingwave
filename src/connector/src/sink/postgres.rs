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
    #[serde(rename = "postgres.max_batch_rows", default = "default_max_batch_rows")]
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
    client: tokio_postgres::Client,
}

impl PostgresClient {
    async fn new(pg_config: &PostgresConfig) -> Result<Self> {
        let connection_string = format!(
            "host={} port={} user={} password={} dbname={}",
            pg_config.host, pg_config.port, pg_config.user, pg_config.password, pg_config.database
        );
        let (client, connection) =
            tokio_postgres::connect(&connection_string, tokio_postgres::NoTls)
                .await
                .context("Failed to connect to Postgres for Sinking")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("connection error: {}", e);
            }
        });
        Ok(Self { client })
    }
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
