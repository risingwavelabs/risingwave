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

use core::num::NonZeroU64;
use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use phf::{Set, phf_set};
use risingwave_common::array::{ArrayImpl, DataChunk, Op, PrimitiveArray, StreamChunk, Utf8Array};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::types::DataType;
use risingwave_pb::connector_service::sink_metadata::SerializedMetadata;
use risingwave_pb::connector_service::{SinkMetadata, sink_metadata};
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::jdbc_jni_client::{self, JdbcJniClient};
use crate::sink::remote::CoordinatedRemoteSinkWriter;
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink, SinkCommitCoordinator,
    SinkCommittedEpochSubscriber, SinkError, SinkParam, SinkWriterMetrics, SinkWriterParam,
};

pub const SNOWFLAKE_SINK: &str = "snowflake";
pub const SNOWFLAKE_SINK_ROW_ID: &str = "__row_id";
pub const SNOWFLAKE_SINK_OP: &str = "__op";
pub const DEFAULT_SCHEDULE: &str = "1 HOUR";

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(rename = "type")]
    pub r#type: String,

    #[serde(rename = "snowflake.cdc_table_name")]
    pub snowflake_cdc_table_name: Option<String>,

    #[serde(rename = "snowflake.target_table_name")]
    pub snowflake_target_table_name: Option<String>,

    #[serde(rename = "snowflake.database")]
    pub snowflake_database: Option<String>,

    #[serde(rename = "snowflake.schema")]
    pub snowflake_schema: Option<String>,

    #[serde(rename = "snowflake.schedule")]
    pub snowflake_schedule: Option<String>,

    #[serde(rename = "snowflake.warehouse")]
    pub snowflake_warehouse: Option<String>,

    #[serde(rename = "snowflake.jdbc.url")]
    pub jdbc_url: Option<String>,

    #[serde(rename = "snowflake.username")]
    pub username: Option<String>,

    #[serde(rename = "snowflake.password")]
    pub password: Option<String>,

    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[with_option(allow_alter_on_fly)]
    pub commit_checkpoint_interval: u64,

    /// Enable auto schema change for upsert sink.
    /// If enabled, the sink will automatically alter the target table to add new columns.
    #[serde(default)]
    #[serde(rename = "auto.schema.change")]
    #[serde_as(as = "DisplayFromStr")]
    pub auto_schema_change: bool,

    #[serde(default)]
    #[serde(rename = "create_table_if_not_exists")]
    #[serde_as(as = "DisplayFromStr")]
    pub create_table_if_not_exists: bool,
}

impl SnowflakeConfig {
    pub fn from_btreemap(properties: &BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
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

    pub fn build_snowflake_task_ctx_jdbc_client(
        &self,
        is_append_only: bool,
        schema: &Schema,
        pk_indices: &Vec<usize>,
    ) -> Result<Option<(SnowflakeTaskContext, JdbcJniClient)>> {
        if !self.auto_schema_change && is_append_only && !self.create_table_if_not_exists {
            // append-only + no auto schema change is not need to create a client
            return Ok(None);
        }
        let target_table_name =
            self.snowflake_target_table_name
                .clone()
                .ok_or(SinkError::Config(anyhow!(
                    "snowflake.target_table_name is required"
                )))?;
        let database = self
            .snowflake_database
            .clone()
            .ok_or(SinkError::Config(anyhow!("snowflake.database is required")))?
            .to_owned();
        let schema_name = self
            .snowflake_schema
            .clone()
            .ok_or(SinkError::Config(anyhow!("snowflake.schema is required")))?
            .to_owned();
        let mut snowflake_task_ctx = SnowflakeTaskContext {
            target_table_name: target_table_name.clone(),
            database,
            schema_name,
            schema: schema.clone(),
            ..Default::default()
        };

        let jdbc_url = self
            .jdbc_url
            .clone()
            .ok_or(SinkError::Config(anyhow!("snowflake.jdbc.url is required")))?
            .to_owned();
        let username = self
            .username
            .clone()
            .ok_or(SinkError::Config(anyhow!("snowflake.username is required")))?;
        let password = self
            .password
            .clone()
            .ok_or(SinkError::Config(anyhow!("snowflake.password is required")))?;
        let jdbc_url = format!("{}?user={}&password={}", jdbc_url, username, password);
        let client = JdbcJniClient::new(jdbc_url)?;

        if !is_append_only {
            let cdc_table_name = self
                .snowflake_cdc_table_name
                .clone()
                .ok_or(SinkError::Config(anyhow!(
                    "snowflake.cdc_table_name is required"
                )))?;
            snowflake_task_ctx.cdc_table_name = Some(cdc_table_name.clone());
            snowflake_task_ctx.schedule = Some(
                self.snowflake_schedule
                    .clone()
                    .unwrap_or(DEFAULT_SCHEDULE.to_owned()),
            );
            snowflake_task_ctx.warehouse = Some(self.snowflake_warehouse.clone().ok_or(
                SinkError::Config(anyhow!("snowflake.warehouse is required")),
            )?);
            snowflake_task_ctx.pk_column_names = Some(
                schema
                    .fields
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| pk_indices.contains(index))
                    .map(|(_, field)| field.name.clone())
                    .collect(),
            );
            snowflake_task_ctx.all_column_names = Some(
                schema
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
            );
            snowflake_task_ctx.task_name = Some(format!(
                "rw_snowflake_sink_from_{cdc_table_name}_to_{target_table_name}"
            ));
        }
        Ok(Some((snowflake_task_ctx, client)))
    }
}

impl EnforceSecret for SnowflakeConfig {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "snowflake.username",
        "snowflake.password",
        "snowflake.jdbc.url",
    };
}

#[derive(Clone, Debug)]
pub struct SnowflakeSink {
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    param: SinkParam,
}

impl EnforceSecret for SnowflakeSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::sink::ConnectorResult<()> {
        for prop in prop_iter {
            SnowflakeConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SnowflakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeConfig::from_btreemap(&param.properties)?;
        let is_append_only = param.sink_type.is_append_only();
        let pk_indices = param.downstream_pk.clone();
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            param,
        })
    }
}

impl Sink for SnowflakeSink {
    type Coordinator = SnowflakeSinkCommitter;
    type LogSinker = CoordinatedLogSinker<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::SnowflakeSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
        if let Some((snowflake_task_ctx, client)) =
            self.config.build_snowflake_task_ctx_jdbc_client(
                self.is_append_only,
                &self.schema,
                &self.pk_indices,
            )?
        {
            let client = SnowflakeJniClient::new(client, snowflake_task_ctx);
            client.execute_create_table()?;
        }

        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        SnowflakeConfig::from_btreemap(config)?;
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = SnowflakeSinkWriter::new(
            self.config.clone(),
            self.is_append_only,
            writer_param.clone(),
            self.param.clone(),
        )
        .await?;

        let commit_checkpoint_interval =
            NonZeroU64::new(self.config.commit_checkpoint_interval).expect(
                "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
            );

        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            commit_checkpoint_interval,
        )
        .await
    }

    fn is_coordinated_sink(&self) -> bool {
        true
    }

    async fn new_coordinator(
        &self,
        _db: DatabaseConnection,
        _iceberg_compact_stat_sender: Option<UnboundedSender<IcebergSinkCompactionUpdate>>,
    ) -> Result<Self::Coordinator> {
        let coordinator = SnowflakeSinkCommitter::new(
            self.config.clone(),
            &self.schema,
            &self.pk_indices,
            self.is_append_only,
        )?;
        Ok(coordinator)
    }
}

struct AugmentedChunk {
    current_epoch: u64,
    current_row_count: usize,
    is_append_only: bool,
}

impl AugmentedChunk {
    fn new(current_epoch: u64, is_append_only: bool) -> Self {
        Self {
            current_epoch,
            current_row_count: 0,
            is_append_only,
        }
    }

    fn reset_epoch(&mut self, current_epoch: u64) {
        if self.is_append_only || current_epoch == self.current_epoch {
            return;
        }
        self.current_epoch = current_epoch;
        self.current_row_count = 0;
    }

    fn augmented_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk> {
        if self.is_append_only {
            return Ok(chunk);
        }
        let (data_chunk, ops) = chunk.into_parts();
        let chunk_row_count = data_chunk.capacity();
        let (columns, visibility) = data_chunk.into_parts();

        let op_column = ops.iter().map(|op| op.to_i16() as i32).collect::<Vec<_>>();
        let row_column_strings: Vec<String> = (0..chunk_row_count)
            .map(|i| format!("{}_{}", self.current_epoch, self.current_row_count + i))
            .collect();

        let row_column_refs: Vec<&str> = row_column_strings.iter().map(|s| s.as_str()).collect();
        self.current_row_count += chunk_row_count;

        let mut arrays: Vec<Arc<ArrayImpl>> = columns;
        arrays.push(Arc::new(ArrayImpl::Utf8(Utf8Array::from_iter(
            row_column_refs,
        ))));
        arrays.push(Arc::new(ArrayImpl::Int32(
            PrimitiveArray::<i32>::from_iter(op_column),
        )));

        let chunk = DataChunk::new(arrays, visibility);
        let ops = vec![Op::Insert; chunk_row_count];
        let chunk = StreamChunk::from_parts(ops, chunk);
        Ok(chunk)
    }
}
pub struct SnowflakeSinkWriter {
    augmented_row: AugmentedChunk,
    jdbc_sink_writer: CoordinatedRemoteSinkWriter,
}

impl SnowflakeSinkWriter {
    pub async fn new(
        config: SnowflakeConfig,
        is_append_only: bool,
        writer_param: SinkWriterParam,
        mut param: SinkParam,
    ) -> Result<Self> {
        let metrics = SinkWriterMetrics::new(&writer_param);
        let properties = &param.properties;
        let column_descs = &mut param.columns;
        let full_table_name = if is_append_only {
            format!(
                r#""{}"."{}"."{}""#,
                config.snowflake_database.clone().unwrap_or_default(),
                config.snowflake_schema.clone().unwrap_or_default(),
                config
                    .snowflake_target_table_name
                    .clone()
                    .unwrap_or_default()
            )
        } else {
            let max_column_id = column_descs
                .iter()
                .map(|column| column.column_id.get_id())
                .max()
                .unwrap_or(0);
            (*column_descs).push(ColumnDesc::named(
                SNOWFLAKE_SINK_ROW_ID,
                ColumnId::new(max_column_id + 1),
                DataType::Varchar,
            ));
            (*column_descs).push(ColumnDesc::named(
                SNOWFLAKE_SINK_OP,
                ColumnId::new(max_column_id + 2),
                DataType::Int32,
            ));
            format!(
                r#""{}"."{}"."{}""#,
                config.snowflake_database.clone().unwrap_or_default(),
                config.snowflake_schema.clone().unwrap_or_default(),
                config.snowflake_cdc_table_name.clone().unwrap_or_default()
            )
        };
        let new_properties = BTreeMap::from([
            ("table.name".to_owned(), full_table_name),
            ("connector".to_owned(), "snowflake".to_owned()),
            (
                "jdbc.url".to_owned(),
                config.jdbc_url.clone().unwrap_or_default(),
            ),
            ("type".to_owned(), "append-only".to_owned()),
            (
                "user".to_owned(),
                config.username.clone().unwrap_or_default(),
            ),
            (
                "password".to_owned(),
                config.password.clone().unwrap_or_default(),
            ),
            (
                "primary_key".to_owned(),
                properties.get("primary_key").cloned().unwrap_or_default(),
            ),
            ("schema.name".to_owned(), config.snowflake_schema.clone().unwrap_or_default()),
            ("database.name".to_owned(), config.snowflake_database.clone().unwrap_or_default()),
        ]);
        param.properties = new_properties;

        let jdbc_sink_writer =
            CoordinatedRemoteSinkWriter::new(param.clone(), metrics.clone()).await?;
        Ok(Self {
            augmented_row: AugmentedChunk::new(0, is_append_only),
            jdbc_sink_writer,
        })
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.augmented_row.reset_epoch(epoch);
        self.jdbc_sink_writer.begin_epoch(epoch).await?;
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        let chunk = self.augmented_row.augmented_chunk(chunk)?;
        self.jdbc_sink_writer.write_batch(chunk).await?;
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        self.jdbc_sink_writer.barrier(is_checkpoint).await?;
        Ok(Some(SinkMetadata {
            metadata: Some(sink_metadata::Metadata::Serialized(SerializedMetadata {
                metadata: vec![],
            })),
        }))
    }

    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        self.jdbc_sink_writer.abort().await?;
        Ok(())
    }
}

#[derive(Default)]
pub struct SnowflakeTaskContext {
    // required for task creation
    pub target_table_name: String,
    pub database: String,
    pub schema_name: String,
    pub schema: Schema,

    // only upsert
    pub task_name: Option<String>,
    pub cdc_table_name: Option<String>,
    pub schedule: Option<String>,
    pub warehouse: Option<String>,
    pub pk_column_names: Option<Vec<String>>,
    pub all_column_names: Option<Vec<String>>,
}
pub struct SnowflakeSinkCommitter {
    client: Option<SnowflakeJniClient>,
}

impl SnowflakeSinkCommitter {
    pub fn new(
        config: SnowflakeConfig,
        schema: &Schema,
        pk_indices: &Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = if let Some((snowflake_task_ctx, client)) =
            config.build_snowflake_task_ctx_jdbc_client(is_append_only, schema, pk_indices)?
        {
            Some(SnowflakeJniClient::new(client, snowflake_task_ctx))
        } else {
            None
        };
        Ok(Self { client })
    }
}

#[async_trait]
impl SinkCommitCoordinator for SnowflakeSinkCommitter {
    async fn init(&mut self, _subscriber: SinkCommittedEpochSubscriber) -> Result<Option<u64>> {
        if let Some(client) = &self.client {
            client.execute_create_merge_into_task()?;
        }
        Ok(None)
    }

    async fn commit(
        &mut self,
        _epoch: u64,
        _metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<()> {
        if let Some(add_columns) = add_columns {
            self.client
                .as_mut()
                .ok_or_else(|| {
                    SinkError::Config(anyhow!("Snowflake sink committer is not initialized."))
                })?
                .execute_alter_add_columns(
                    &add_columns
                        .iter()
                        .map(|f| (f.name.clone(), f.data_type.to_string()))
                        .collect::<Vec<_>>(),
                )?;
        }
        Ok(())
    }
}

impl Drop for SnowflakeSinkCommitter {
    fn drop(&mut self) {
        if let Some(client) = &self.client {
            client.execute_drop_task().ok();
        }
    }
}

pub struct SnowflakeJniClient {
    jdbc_client: JdbcJniClient,
    snowflake_task_context: SnowflakeTaskContext,
}

impl SnowflakeJniClient {
    pub fn new(jdbc_client: JdbcJniClient, snowflake_task_context: SnowflakeTaskContext) -> Self {
        Self {
            jdbc_client,
            snowflake_task_context,
        }
    }

    pub fn execute_alter_add_columns(&mut self, columns: &Vec<(String, String)>) -> Result<()> {
        self.execute_drop_task()?;
        if let Some(names) = self.snowflake_task_context.all_column_names.as_mut() {
            names.extend(columns.iter().map(|(name, _)| name.clone()));
        }
        if let Some(cdc_table_name) = &self.snowflake_task_context.cdc_table_name {
            let alter_add_column_cdc_table_sql = build_alter_add_column_sql(
                cdc_table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                columns,
            );
            self.jdbc_client
                .execute_sql_sync(&alter_add_column_cdc_table_sql)?;
        }

        let alter_add_column_target_table_sql = build_alter_add_column_sql(
            &self.snowflake_task_context.target_table_name,
            &self.snowflake_task_context.database,
            &self.snowflake_task_context.schema_name,
            columns,
        );
        self.jdbc_client
            .execute_sql_sync(&alter_add_column_target_table_sql)?;

        self.execute_create_merge_into_task()?;
        Ok(())
    }

    pub fn execute_create_merge_into_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let create_task_sql = build_create_merge_into_task_sql(&self.snowflake_task_context);
            let start_task_sql = build_start_task_sql(&self.snowflake_task_context);
            self.jdbc_client.execute_sql_sync(&create_task_sql)?;
            self.jdbc_client.execute_sql_sync(&start_task_sql)?;
        }
        Ok(())
    }

    pub fn execute_drop_task(&self) -> Result<()> {
        if self.snowflake_task_context.task_name.is_some() {
            let sql = build_drop_task_sql(&self.snowflake_task_context);
            if let Err(e) = self.jdbc_client.execute_sql_sync(&sql) {
                tracing::error!(
                    "Failed to drop Snowflake sink task {:?}: {:?}",
                    self.snowflake_task_context.task_name,
                    e.as_report()
                );
            } else {
                tracing::info!(
                    "Snowflake sink task {:?} dropped",
                    self.snowflake_task_context.task_name
                );
            }
        }
        Ok(())
    }

    pub fn execute_create_table(&self) -> Result<()> {
        // create target table
        let create_target_table_sql = build_create_table_sql(
            &self.snowflake_task_context.target_table_name,
            &self.snowflake_task_context.database,
            &self.snowflake_task_context.schema_name,
            &self.snowflake_task_context.schema,
            false,
        )?;
        self.jdbc_client
            .execute_sql_sync(&create_target_table_sql)?;
        if let Some(cdc_table_name) = &self.snowflake_task_context.cdc_table_name {
            let create_cdc_table_sql = build_create_table_sql(
                cdc_table_name,
                &self.snowflake_task_context.database,
                &self.snowflake_task_context.schema_name,
                &self.snowflake_task_context.schema,
                true,
            )?;
            self.jdbc_client.execute_sql_sync(&create_cdc_table_sql)?;
        }
        Ok(())
    }
}

fn build_create_table_sql(
    table_name: &str,
    database: &str,
    schema_name: &str,
    schema: &Schema,
    need_op_and_row_id: bool,
) -> Result<String> {
    let full_table_name = format!(r#""{}"."{}"."{}""#, database, schema_name, table_name);
    let mut columns: Vec<String> = schema
        .fields
        .iter()
        .map(|field| {
            let data_type = convert_snowflake_data_type(&field.data_type)?;
            Ok(format!(r#""{}" {}"#, field.name, data_type))
        })
        .collect::<Result<Vec<String>>>()?;
    if need_op_and_row_id {
        columns.push(format!(r#""{}" STRING"#, SNOWFLAKE_SINK_ROW_ID));
        columns.push(format!(r#""{}" INT"#, SNOWFLAKE_SINK_OP));
    }
    let columns_str = columns.join(", ");
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}) ENABLE_SCHEMA_EVOLUTION  = true",
        full_table_name, columns_str
    ))
}

fn convert_snowflake_data_type(data_type: &DataType) -> Result<String> {
    let data_type = match data_type {
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INTEGER".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "FLOAT4".to_owned(),
        DataType::Float64 => "FLOAT8".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Varchar => "STRING".to_owned(),
        DataType::Date => "DATE".to_owned(),
        DataType::Timestamp => "TIMESTAMP".to_owned(),
        DataType::Timestamptz => "TIMESTAMP_TZ".to_owned(),
        DataType::Jsonb => "STRING".to_owned(),
        DataType::Decimal => "DECIMAL".to_owned(),
        DataType::Bytea => "BINARY".to_owned(),
        DataType::Time => "TIME".to_owned(),
        _ => {
            return Err(SinkError::Config(anyhow!(
                "Dont support auto create table for datatype: {}",
                data_type
            )));
        }
    };
    Ok(data_type)
}

fn build_alter_add_column_sql(
    table_name: &str,
    database: &str,
    schema: &str,
    columns: &Vec<(String, String)>,
) -> String {
    let full_table_name = format!(r#""{}"."{}"."{}""#, database, schema, table_name);
    jdbc_jni_client::build_alter_add_column_sql(&full_table_name, columns)
}

fn build_start_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema_name: schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema,
        task_name.as_ref().unwrap()
    );
    format!("ALTER TASK {} RESUME", full_task_name)
}

fn build_drop_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        database,
        schema_name: schema,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema,
        task_name.as_ref().unwrap()
    );
    format!("DROP TASK IF EXISTS {}", full_task_name)
}

fn build_create_merge_into_task_sql(snowflake_task_context: &SnowflakeTaskContext) -> String {
    let SnowflakeTaskContext {
        task_name,
        cdc_table_name,
        target_table_name,
        schedule,
        warehouse,
        pk_column_names,
        all_column_names,
        database,
        schema_name,
        ..
    } = snowflake_task_context;
    let full_task_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema_name,
        task_name.as_ref().unwrap()
    );
    let full_cdc_table_name = format!(
        r#""{}"."{}"."{}""#,
        database,
        schema_name,
        cdc_table_name.as_ref().unwrap()
    );
    let full_target_table_name = format!(
        r#""{}"."{}"."{}""#,
        database, schema_name, target_table_name
    );

    let pk_names_str = pk_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#""{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");
    let pk_names_eq_str = pk_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"target."{}" = source."{}""#, name, name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"target."{}" = source."{}""#, name, name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#""{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_insert_str = all_column_names
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| format!(r#"source."{}""#, name))
        .collect::<Vec<String>>()
        .join(", ");

    format!(
        r#"CREATE OR REPLACE TASK {task_name}
WAREHOUSE = {warehouse}
SCHEDULE = '{schedule}'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("{snowflake_sink_row_id}"), '0') INTO :max_row_id
    FROM {cdc_table_name};

    MERGE INTO {target_table_name} AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_names_str} ORDER BY "{snowflake_sink_row_id}" DESC) AS dedupe_id
            FROM {cdc_table_name}
            WHERE "{snowflake_sink_row_id}" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON {pk_names_eq_str}
    WHEN MATCHED AND source."{snowflake_sink_op}" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."{snowflake_sink_op}" IN (1, 3) THEN UPDATE SET {all_column_names_set_str}
    WHEN NOT MATCHED AND source."{snowflake_sink_op}" IN (1, 3) THEN INSERT ({all_column_names_str}) VALUES ({all_column_names_insert_str});

    DELETE FROM {cdc_table_name}
    WHERE "{snowflake_sink_row_id}" <= :max_row_id;
END;"#,
        task_name = full_task_name,
        warehouse = warehouse.as_ref().unwrap(),
        schedule = schedule.as_ref().unwrap(),
        cdc_table_name = full_cdc_table_name,
        target_table_name = full_target_table_name,
        pk_names_str = pk_names_str,
        pk_names_eq_str = pk_names_eq_str,
        all_column_names_set_str = all_column_names_set_str,
        all_column_names_str = all_column_names_str,
        all_column_names_insert_str = all_column_names_insert_str,
        snowflake_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
        snowflake_sink_op = SNOWFLAKE_SINK_OP,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::jdbc_jni_client::normalize_sql;

    #[test]
    fn test_snowflake_sink_commit_coordinator() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: Some("test_task".to_owned()),
            cdc_table_name: Some("test_cdc_table".to_owned()),
            target_table_name: "test_target_table".to_owned(),
            schedule: Some("1 HOUR".to_owned()),
            warehouse: Some("test_warehouse".to_owned()),
            pk_column_names: Some(vec!["v1".to_owned()]),
            all_column_names: Some(vec!["v1".to_owned(), "v2".to_owned()]),
            database: "test_db".to_owned(),
            schema_name: "test_schema".to_owned(),
            schema: Schema { fields: vec![] },
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK "test_db"."test_schema"."test_task"
WAREHOUSE = test_warehouse
SCHEDULE = '1 HOUR'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("__row_id"), '0') INTO :max_row_id
    FROM "test_db"."test_schema"."test_cdc_table";

    MERGE INTO "test_db"."test_schema"."test_target_table" AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "v1" ORDER BY "__row_id" DESC) AS dedupe_id
            FROM "test_db"."test_schema"."test_cdc_table"
            WHERE "__row_id" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target."v1" = source."v1"
    WHEN MATCHED AND source."__op" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."__op" IN (1, 3) THEN UPDATE SET target."v1" = source."v1", target."v2" = source."v2"
    WHEN NOT MATCHED AND source."__op" IN (1, 3) THEN INSERT ("v1", "v2") VALUES (source."v1", source."v2");

    DELETE FROM "test_db"."test_schema"."test_cdc_table"
    WHERE "__row_id" <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator_multi_pk() {
        let snowflake_task_context = SnowflakeTaskContext {
            task_name: Some("test_task_multi_pk".to_owned()),
            cdc_table_name: Some("cdc_multi_pk".to_owned()),
            target_table_name: "target_multi_pk".to_owned(),
            schedule: Some("5 MINUTE".to_owned()),
            warehouse: Some("multi_pk_warehouse".to_owned()),
            pk_column_names: Some(vec!["id1".to_owned(), "id2".to_owned()]),
            all_column_names: Some(vec!["id1".to_owned(), "id2".to_owned(), "val".to_owned()]),
            database: "test_db".to_owned(),
            schema_name: "test_schema".to_owned(),
            schema: Schema { fields: vec![] },
        };
        let task_sql = build_create_merge_into_task_sql(&snowflake_task_context);
        let expected = r#"CREATE OR REPLACE TASK "test_db"."test_schema"."test_task_multi_pk"
WAREHOUSE = multi_pk_warehouse
SCHEDULE = '5 MINUTE'
AS
BEGIN
    LET max_row_id STRING;

    SELECT COALESCE(MAX("__row_id"), '0') INTO :max_row_id
    FROM "test_db"."test_schema"."cdc_multi_pk";

    MERGE INTO "test_db"."test_schema"."target_multi_pk" AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY "id1", "id2" ORDER BY "__row_id" DESC) AS dedupe_id
            FROM "test_db"."test_schema"."cdc_multi_pk"
            WHERE "__row_id" <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target."id1" = source."id1" AND target."id2" = source."id2"
    WHEN MATCHED AND source."__op" IN (2, 4) THEN DELETE
    WHEN MATCHED AND source."__op" IN (1, 3) THEN UPDATE SET target."id1" = source."id1", target."id2" = source."id2", target."val" = source."val"
    WHEN NOT MATCHED AND source."__op" IN (1, 3) THEN INSERT ("id1", "id2", "val") VALUES (source."id1", source."id2", source."val");

    DELETE FROM "test_db"."test_schema"."cdc_multi_pk"
    WHERE "__row_id" <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }
}
