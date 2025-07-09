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
use crate::sink::{Result, Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::source::UnknownFields;

pub const SNOWFLAKE_SINK: &str = "snowflake";
const SNOWFLAKE_SINK_ROW_ID: &str = "__row_id";
const SNOWFLAKE_SINK_OP: &str = "__op";
const DEFAULT_SCHEDULE: &str = "1 HOUR";

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


pub struct SnowflakeSinkCommitCoordinator {
    task_name: String,
    cdc_table_name: String,
    target_table_name: String,
    schedule: String,
    warehouse: String,
    pk_column_names: Vec<String>,
    all_column_names: Vec<String>,
    snowflake_client: SnowflakeClient,
}

impl SnowflakeSinkCommitCoordinator {
    pub fn new(properties: SnowflakeConfig, schema: &Schema, pk_indices: &Vec<usize>) -> Result<Self> {
        let cdc_table_name = properties.snowflake_cdc_table_name.ok_or(SinkError::Config(anyhow!("snowflake.cdc_table_name is required")))?;
        let target_table_name = properties.snowflake_target_table_name.ok_or(SinkError::Config(anyhow!("snowflake.target_table_name is required")))?;
        let task_name = format!("rw_snowflake_sink_from_{cdc_table_name}_to_{target_table_name}");
        let schedule = properties.snowflake_schedule.unwrap_or(DEFAULT_SCHEDULE.to_owned());
        let warehouse = properties.snowflake_warehouse.ok_or(SinkError::Config(anyhow!("snowflake.warehouse is required")))?;
        let pk_column_names = schema.fields.iter().enumerate().filter(|(index, _)| pk_indices.contains(index)).map(|(_, field)| field.name.clone()).collect();
        let all_column_names = schema.fields.iter().map(|field| field.name.clone()).collect();
        let post = properties.snowflake_post.ok_or(SinkError::Config(anyhow!("snowflake.post is required")))?;
        let token = properties.snowflake_token.ok_or(SinkError::Config(anyhow!("snowflake.token is required")))?;
        Ok(Self {
            task_name,
            cdc_table_name,
            target_table_name,
            schedule,
            warehouse,
            pk_column_names,
            all_column_names,
            snowflake_client: SnowflakeClient::new(post, token),
        })
    }
}

#[async_trait]
impl SinkCommitCoordinator for SnowflakeSinkCommitCoordinator {
    async fn init(&mut self, _subscriber: SinkCommittedEpochSubscriber) -> Result<Option<u64>> {
        let task_sql = build_create_snowflake_sink_task_sql(
            &self.task_name,
            &self.schedule,
            &self.warehouse,
            &self.cdc_table_name,
            &self.target_table_name,
            &self.pk_column_names,
            &self.all_column_names,
        );
        self.snowflake_client.execute_sql_sync(&task_sql)?;
        let start_sql = build_start_snowflake_sink_task_sql(&self.task_name);
        self.snowflake_client.execute_sql_sync(&start_sql)?;
        tracing::info!("Snowflake sink task {} created and started", self.task_name);
        Ok(None)
    }

    async fn commit(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(())
    }
}

impl Drop for SnowflakeSinkCommitCoordinator {
    fn drop(&mut self) {
        let drop_sql = build_drop_snowflake_sink_task_sql(&self.task_name);
        if let Err(e) = self.snowflake_client.execute_sql_sync(&drop_sql) {
            tracing::error!("Failed to drop Snowflake sink task {}: {}", self.task_name, e);
        } else {
            tracing::info!("Snowflake sink task {} dropped", self.task_name);
        }
    }
}

pub struct SnowflakeClient{
    snowflake_url: String,
    snowflake_token: String,
}

impl SnowflakeClient {
    pub fn new(snowflake_post: String, snowflake_token: String) -> Self {
        let snowflake_url = format!("https://{}/api/v2/statements", snowflake_post);
        Self {
            snowflake_url,
            snowflake_token,
        }
    }

    pub fn execute_sql_sync(&self, sql: &str) -> Result<()> {
        let client = reqwest::blocking::Client::new();
        let body = json!({
            "statement": sql,
            "timeout": 60
        });

        let res = client
            .post(&self.snowflake_url)
            .bearer_auth(&self.snowflake_token)
            .json(&body)
            .send()
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        
        if res.status().is_success() {
            Ok(())
        } else {
            Err(SinkError::Config(anyhow!("Failed to execute SQL: {}, err is {:?}", sql, res.text())))
        }
    }
}

fn build_start_snowflake_sink_task_sql(task_name: &str) -> String {
    format!("ALTER TASK {} RESUME", task_name)
}

fn build_drop_snowflake_sink_task_sql(task_name: &str) -> String {
    format!("DROP TASK IF EXISTS {}", task_name)
}

fn build_create_snowflake_sink_task_sql(
    task_name: &str,
    schedule: &str,
    warehouse: &str,
    cdc_table_name: &str,
    target_table_name: &str,
    pk_column_names: &[String],
    all_column_names: &[String],
) -> String {
    let pk_names_str = pk_column_names.join(", ");
    let pk_names_eq_str = pk_column_names
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .iter()
        .map(|name| format!("target.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names.join(", ");
    let all_column_names_insert_str = all_column_names
        .iter()
        .map(|name| format!("source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");

    format!(
r#"CREATE OR REPLACE TASK {task_name}
WAREHOUSE = {warehouse}
SCHEDULE = '{schedule}'
AS
BEGIN
    LET max_row_id INT;

    SELECT COALESCE(MAX(_changelog_row_id), 0) INTO :max_row_id
    FROM {cdc_table_name};

    MERGE INTO {target_table_name} AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {pk_names_str} ORDER BY _changelog_row_id DESC) AS dedupe_id
            FROM {cdc_table_name}
            WHERE _changelog_row_id <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON {pk_names_eq_str}
    WHEN MATCHED AND source.changelog_op IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.changelog_op IN (1, 3) THEN UPDATE SET {all_column_names_set_str}
    WHEN NOT MATCHED AND source.changelog_op IN (1, 3) THEN INSERT ({all_column_names_str}) VALUES ({all_column_names_insert_str});

    DELETE FROM {cdc_table_name}
    WHERE _changelog_row_id <= :max_row_id;
END;"#,
        task_name = task_name,
        warehouse = warehouse,
        schedule = schedule,
        cdc_table_name = cdc_table_name,
        target_table_name = target_table_name,
        pk_names_str = pk_names_str,
        pk_names_eq_str = pk_names_eq_str,
        all_column_names_set_str = all_column_names_set_str,
        all_column_names_str = all_column_names_str,
        all_column_names_insert_str = all_column_names_insert_str
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_sql(s: &str) -> String {
        s.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator() {
        let task_sql = build_create_snowflake_sink_task_sql(
            "test_task",
            "1 HOUR",
            "test_warehouse",
            "test_cdc_table",
            "test_target_table",
            &["v1".to_string()],
            &["v1".to_string(), "v2".to_string()],
        );
        let expected = r#"CREATE OR REPLACE TASK test_task
WAREHOUSE = test_warehouse
SCHEDULE = '1 HOUR'
AS
BEGIN
    LET max_row_id INT;

    SELECT COALESCE(MAX(_changelog_row_id), 0) INTO :max_row_id
    FROM test_cdc_table;

    MERGE INTO test_target_table AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY v1 ORDER BY _changelog_row_id DESC) AS dedupe_id
            FROM test_cdc_table
            WHERE _changelog_row_id <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target.v1 = source.v1
    WHEN MATCHED AND source.changelog_op IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.changelog_op IN (1, 3) THEN UPDATE SET target.v1 = source.v1, target.v2 = source.v2
    WHEN NOT MATCHED AND source.changelog_op IN (1, 3) THEN INSERT (v1, v2) VALUES (source.v1, source.v2);

    DELETE FROM test_cdc_table
    WHERE _changelog_row_id <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }

    #[test]
    fn test_snowflake_sink_commit_coordinator_multi_pk() {
        let task_sql = build_create_snowflake_sink_task_sql(
            "test_task_multi_pk",
            "5 MINUTE",
            "multi_pk_warehouse",
            "cdc_multi_pk",
            "target_multi_pk",
            &["id1".to_string(), "id2".to_string()],
            &["id1".to_string(), "id2".to_string(), "val".to_string()],
        );
        let expected = r#"CREATE OR REPLACE TASK test_task_multi_pk
WAREHOUSE = multi_pk_warehouse
SCHEDULE = '5 MINUTE'
AS
BEGIN
    LET max_row_id INT;

    SELECT COALESCE(MAX(_changelog_row_id), 0) INTO :max_row_id
    FROM cdc_multi_pk;

    MERGE INTO target_multi_pk AS target
    USING (
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY id1, id2 ORDER BY _changelog_row_id DESC) AS dedupe_id
            FROM cdc_multi_pk
            WHERE _changelog_row_id <= :max_row_id
        ) AS subquery
        WHERE dedupe_id = 1
    ) AS source
    ON target.id1 = source.id1 AND target.id2 = source.id2
    WHEN MATCHED AND source.changelog_op IN (2, 4) THEN DELETE
    WHEN MATCHED AND source.changelog_op IN (1, 3) THEN UPDATE SET target.id1 = source.id1, target.id2 = source.id2, target.val = source.val
    WHEN NOT MATCHED AND source.changelog_op IN (1, 3) THEN INSERT (id1, id2, val) VALUES (source.id1, source.id2, source.val);

    DELETE FROM cdc_multi_pk
    WHERE _changelog_row_id <= :max_row_id;
END;"#;
        assert_eq!(normalize_sql(&task_sql), normalize_sql(expected));
    }
}
