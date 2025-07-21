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
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use bytes::BytesMut;
use opendal::Operator;
use phf::{Set, phf_set};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_pb::connector_service::SinkMetadata;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use serde_json::{Map, Value};
use serde_with::{DisplayFromStr, serde_as};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::decouple_checkpoint_log_sink::default_commit_checkpoint_interval;
use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::file_sink::opendal_sink::FileSink;
use crate::sink::file_sink::s3::{S3Config, S3Sink};
use crate::sink::snowflake::snowflake_jni_client::{JdbcJniClient, SnowflakeJniClient};
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT, Sink, SinkCommitCoordinator,
    SinkCommittedEpochSubscriber, SinkError, SinkParam,
};

pub mod snowflake_jni_client;

pub const SNOWFLAKE_SINK: &str = "snowflake";
pub const SNOWFLAKE_SINK_ROW_ID: &str = "__row_id";
pub const SNOWFLAKE_SINK_OP: &str = "__op";
pub const DEFAULT_SCHEDULE: &str = "1 HOUR";

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub s3_inner: S3Config,

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
}

impl SnowflakeConfig {
    pub fn from_btreemap(properties: &BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.s3_inner.r#type != SINK_TYPE_APPEND_ONLY
            && config.s3_inner.r#type != SINK_TYPE_UPSERT
        {
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
        if !self.auto_schema_change {
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
            target_table_name,
            database,
            schema: schema_name,
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
        let client = JdbcJniClient::new(jdbc_url, username, password)?;

        if !is_append_only {
            snowflake_task_ctx.cdc_table_name = Some(self.snowflake_cdc_table_name.clone().ok_or(
                SinkError::Config(anyhow!("snowflake.cdc_table_name is required")),
            )?);
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
        self.config.build_snowflake_task_ctx_jdbc_client(
            self.is_append_only,
            &self.schema,
            &self.pk_indices,
        )?;
        Ok(())
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
            self.schema.clone(),
            self.is_append_only,
            writer_param.executor_id,
        )?;

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

struct AugmentedRow {
    row_encoder: JsonEncoder,
    current_epoch: u64,
    current_row_count: usize,
    is_append_only: bool,
}

impl AugmentedRow {
    fn new(current_epoch: u64, is_append_only: bool, schema: Schema) -> Self {
        let row_encoder = JsonEncoder::new(
            schema,
            None,
            crate::sink::encoder::DateHandlingMode::String,
            TimestampHandlingMode::String,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::String,
            JsonbHandlingMode::String,
        );
        Self {
            row_encoder,
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

    fn augmented_row(&mut self, row: impl Row, op: Op) -> Result<Map<String, Value>> {
        let mut row = self.row_encoder.encode(row)?;
        if self.is_append_only {
            return Ok(row);
        }
        self.current_row_count += 1;
        row.insert(
            SNOWFLAKE_SINK_ROW_ID.to_owned(),
            Value::String(format!("{}_{}", self.current_epoch, self.current_row_count)),
        );
        row.insert(
            SNOWFLAKE_SINK_OP.to_owned(),
            Value::Number(serde_json::Number::from(op.to_i16())),
        );
        Ok(row)
    }
}
pub struct SnowflakeSinkWriter {
    config: SnowflakeConfig,
    s3_operator: Operator,
    augmented_row: AugmentedRow,
    opendal_writer: Option<opendal::Writer>,
    executor_id: u64,
}
async fn build_opendal_writer(
    config: &SnowflakeConfig,
    executor_id: u64,
    operator: &Operator,
) -> Result<opendal::Writer> {
    let mut base_path = config.s3_inner.common.path.clone().unwrap_or("".to_owned());
    if !base_path.ends_with('/') {
        base_path.push('/');
    }
    let create_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let object_name = format!(
        "{}{}_{}.{}",
        base_path,
        executor_id,
        create_time.as_secs(),
        "json",
    );
    Ok(operator.writer_with(&object_name).concurrent(8).await?)
}

impl SnowflakeSinkWriter {
    pub fn new(
        config: SnowflakeConfig,
        schema: Schema,
        is_append_only: bool,
        executor_id: u64,
    ) -> Result<Self> {
        let s3_operator = FileSink::<S3Sink>::new_s3_sink(&config.s3_inner)?;
        Ok(Self {
            config,
            s3_operator,
            opendal_writer: None,
            executor_id,
            augmented_row: AugmentedRow::new(0, is_append_only, schema),
        })
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    type CommitMetadata = Option<SinkMetadata>;

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.augmented_row.reset_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.opendal_writer.is_none() {
            let opendal_writer =
                build_opendal_writer(&self.config, self.executor_id, &self.s3_operator).await?;
            self.opendal_writer = Some(opendal_writer);
        }
        let mut chunk_buf = BytesMut::new();
        for (op, row) in chunk.rows() {
            let encoded_row = self.augmented_row.augmented_row(row, op)?;
            writeln!(chunk_buf, "{}", Value::Object(encoded_row)).unwrap(); // write to a `BytesMut` should never fail
        }
        self.opendal_writer
            .as_mut()
            .ok_or_else(|| SinkError::File("Sink writer is not created.".to_owned()))?
            .write(chunk_buf.freeze())
            .await?;
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if is_checkpoint && let Some(mut writer) = self.opendal_writer.take() {
            writer
                .close()
                .await
                .map_err(|e| SinkError::File(e.to_report_string()))?;
        }
        Ok(None)
    }

    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }
}

#[derive(Default)]
pub struct SnowflakeTaskContext {
    // required for task creation
    pub target_table_name: String,
    pub database: String,
    pub schema: String,

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

    async fn commit(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        // self.snowflake_task_context
        //     .all_column_names
        //     .extend(columns.iter().map(|(name, _typ)| name.to_string()));
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
