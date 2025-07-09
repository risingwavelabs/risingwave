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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use bytes::BytesMut;
use opendal::Operator;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::util::env_var::env_var_is_true;
use risingwave_pb::connector_service::SinkMetadata;
use serde::Deserialize;
use serde_json::{Value, Map};
use serde_with::serde_as;
use tonic::async_trait;
use with_options::WithOptions;

use super::opendal_sink::{BatchingStrategy, FileSink};
use crate::connector_common::DISABLE_DEFAULT_CREDENTIAL;
use crate::deserialize_optional_bool_from_string;
use crate::sink::clickhouse::ClickHouseConfig;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::encoder::{JsonEncoder, JsonbHandlingMode, TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode, RowEncoder};
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::file_sink::s3::{S3Config, S3Sink};
use crate::sink::writer::SinkWriter;
use crate::sink::{Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::source::UnknownFields;

pub const SNOWFLAKE_SINK: &str = "snowflake";
const SNOWFLAKE_SINK_ROW_ID: &str = "__row_id";
const SNOWFLAKE_SINK_OP: &str = "__op";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub s3_inner: S3Config,
}

impl SnowflakeConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.s3_inner.r#type != SINK_TYPE_APPEND_ONLY && config.s3_inner.r#type != SINK_TYPE_UPSERT {
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

#[derive(Clone, Debug)]
pub struct SnowflakeSink{
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl TryFrom<SinkParam> for SnowflakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl Sink for SnowflakeSink {
    type Coordinator = SnowflakeSinkCommitter;
    type LogSinker = CoordinatedLogSinker<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    async fn validate(&self) -> Result<()> {
        if self.config.s3_inner.r#type == SINK_TYPE_UPSERT {
            // checkout xxx
        }
        Ok(())
    }

    fn validate_alter_config(config: &BTreeMap<String, String>) -> Result<()> {
        SnowflakeConfig::from_btreemap(config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: crate::sink::SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = SnowflakeSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
            writer_param.executor_id,
        )?;
        Ok(CoordinatedLogSinker::new(writer, writer_param))
    }
}

struct AugmentedRow {
    row_encoder: JsonEncoder,
    current_epoch: u64,
    current_row_count: usize,
    is_append_only: bool,
}

impl AugmentedRow {
    fn new(current_epoch: u64,is_append_only: bool, schema: Schema) -> Self {
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
            SNOWFLAKE_SINK_ROW_ID.to_string(),
            Value::String(format!("{}_{}", self.current_epoch, self.current_row_count)),
        );
        row.insert(
            SNOWFLAKE_SINK_OP.to_string(),
            Value::Number(serde_json::Number::from(op.to_i16())),
        );
        Ok(row)
    }
}
pub struct SnowflakeSinkWriter {
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    s3_operator: Operator,
    augmented_row: AugmentedRow,
    opendal_writer: Option<opendal::Writer>,
    executor_id: u64,
}
async fn build_opendal_writer(config: &SnowflakeConfig, executor_id: u64, operator: &Operator) -> Result<opendal::Writer>{
    let base_path = config.s3_inner.common.path.clone().unwrap_or("".to_owned());
    let create_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
    let object_name =  format!(
        "{}{}_{}.{}",
        base_path,
        executor_id,
        create_time.as_secs(),
        "json",
    );
    Ok(operator
        .writer_with(&object_name)
        .concurrent(8)
        .await?)
}

impl SnowflakeSinkWriter {
    pub fn new(
        config: SnowflakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        executor_id: u64,
    ) -> Result<Self> {
        let s3_operator = FileSink::<S3Sink>::new_s3_sink(&config.s3_inner)?;
        Ok(Self {
            config,
            schema: schema.clone(),
            pk_indices,
            is_append_only,
            s3_operator,
            opendal_writer: None,
            executor_id,
            augmented_row: AugmentedRow::new(0, is_append_only, schema),
        })
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    type Coordinator = SnowflakeSinkCommitter;
    
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.augmented_row.reset_epoch(epoch);
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.opendal_writer.is_none() {
            let opendal_writer = build_opendal_writer(&self.config, self.executor_id, &self.s3_operator)
                .await?;
            self.opendal_writer = Some(opendal_writer);
        }
        let mut chunk_buf = BytesMut::new();
        for (op,row) in chunk.rows() {
            let encoded_row = self.augmented_row.augmented_row(row, op)?;
            writeln!(
                chunk_buf,
                "{}",
                Value::Object(encoded_row)
            )
            .unwrap(); // write to a `BytesMut` should never fail
        }
        self.opendal_writer.as_mut().ok_or_else(|| SinkError::File("Sink writer is not created.".to_owned()))?.write(chunk_buf.freeze()).await?;
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Option<SinkMetadata>> {
        if is_checkpoint && let Some(writer) = self.opendal_writer.take() {
            writer.close().await.map_err(|e| SinkError::File(e.to_string()))?;
        }
        Ok(None)
    }

    async fn abort(&mut self) -> Result<()> {
        // TODO: abort should clean up all the data written in this epoch.
        Ok(())
    }
    
}
