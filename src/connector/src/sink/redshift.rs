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

use core::num::NonZero;
use std::sync::Arc;
use std::time::Duration;

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
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{MissedTickBehavior, interval};
use tonic::async_trait;
use with_options::WithOptions;

use crate::connector_common::IcebergSinkCompactionUpdate;
use crate::enforce_secret::EnforceSecret;
use crate::sink::coordinate::CoordinatedLogSinker;
use crate::sink::jdbc_jni_client::{JdbcJniClient, build_alter_add_column_sql};
use crate::sink::remote::CoordinatedRemoteSinkWriter;
use crate::sink::snowflake::{SNOWFLAKE_SINK_OP, SNOWFLAKE_SINK_ROW_ID};
use crate::sink::writer::SinkWriter;
use crate::sink::{
    Result, Sink, SinkCommitCoordinator, SinkCommittedEpochSubscriber, SinkError, SinkParam,
    SinkWriterMetrics,
};

pub const REDSHIFT_SINK: &str = "redshift";

#[serde_as]
#[derive(Debug, Clone, Deserialize, WithOptions)]
pub struct RedShiftConfig {
    #[serde(rename = "jdbc.url")]
    pub jdbc_url: String,

    #[serde(rename = "user")]
    pub username: Option<String>,

    #[serde(rename = "password")]
    pub password: Option<String>,

    #[serde(rename = "table.name")]
    pub table: String,

    #[serde(rename = "cdc_table.name")]
    pub cdc_table: Option<String>,

    #[serde(default)]
    #[serde(rename = "create_table_if_not_exists")]
    #[serde_as(as = "DisplayFromStr")]
    pub create_table_if_not_exists: bool,

    #[serde(default = "default_schedule")]
    #[serde(rename = "schedule_seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub schedule: u64,

    #[serde(default = "default_batch_insert_rows")]
    #[serde(rename = "batch.insert.rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub batch_insert_rows: u32,
}

fn default_schedule() -> u64 {
    3600 // Default to 1 hour
}

fn default_batch_insert_rows() -> u32 {
    4096 // Default batch size
}

impl RedShiftConfig {
    pub fn build_client(&self) -> Result<JdbcJniClient> {
        let mut jdbc_url = self.jdbc_url.clone();
        if let Some(username) = &self.username {
            jdbc_url = format!("{}?user={}", jdbc_url, username);
        }
        if let Some(password) = &self.password {
            jdbc_url = format!("{}&password={}", jdbc_url, password);
        }
        JdbcJniClient::new(jdbc_url)
    }
}

#[derive(Debug)]
pub struct RedshiftSink {
    config: RedShiftConfig,
    param: SinkParam,
    is_append_only: bool,
    schema: Schema,
    pk_indices: Vec<usize>,
}
impl EnforceSecret for RedshiftSink {
    const ENFORCE_SECRET_PROPERTIES: Set<&'static str> = phf_set! {
        "user",
        "password",
        "jdbc.url"
    };
}

impl TryFrom<SinkParam> for RedshiftSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let config = serde_json::from_value::<RedShiftConfig>(
            serde_json::to_value(param.properties.clone()).unwrap(),
        )
        .map_err(|e| SinkError::Config(anyhow!(e)))?;
        let is_append_only = param.sink_type.is_append_only();
        let schema = param.schema().clone();
        let pk_indices = param.downstream_pk.clone();
        Ok(Self {
            config,
            param,
            is_append_only,
            schema,
            pk_indices,
        })
    }
}

impl Sink for RedshiftSink {
    type Coordinator = RedshiftSinkCommitter;
    type LogSinker = CoordinatedLogSinker<RedShiftSinkWriter>;

    const SINK_NAME: &'static str = REDSHIFT_SINK;

    async fn validate(&self) -> Result<()> {
        if self.config.create_table_if_not_exists {
            let client = self.config.build_client()?;
            let schema = self.param.schema();
            let build_table_sql = build_create_table_sql(&self.config.table, &schema, false)?;
            client.execute_sql_sync(&vec![build_table_sql])?;
            if !self.is_append_only {
                let cdc_table = self.config.cdc_table.as_ref().ok_or_else(|| {
                    SinkError::Config(anyhow!("cdc_table.name is required for append-only sink"))
                })?;
                let build_cdc_table_sql = build_create_table_sql(cdc_table, &schema, true)?;
                client.execute_sql_sync(&vec![build_cdc_table_sql])?;
            }
        }
        Ok(())
    }

    fn support_schema_change() -> bool {
        true
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = RedShiftSinkWriter::new(
            self.config.clone(),
            self.is_append_only,
            writer_param.clone(),
            self.param.clone(),
        )
        .await?;
        CoordinatedLogSinker::new(
            &writer_param,
            self.param.clone(),
            writer,
            NonZero::new(1).unwrap(),
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
        let pk_column_names: Vec<_> = self
            .schema
            .fields
            .iter()
            .enumerate()
            .filter(|(index, _)| self.pk_indices.contains(index))
            .map(|(_, field)| field.name.clone())
            .collect();
        if pk_column_names.is_empty() && !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "Primary key columns not found. Please set the `primary_key` column in the sink properties, or ensure that the sink contains the primary key columns from the upstream."
            )));
        }
        let all_column_names = self
            .schema
            .fields
            .iter()
            .map(|field| field.name.clone())
            .collect();
        let coordinator = RedshiftSinkCommitter::new(
            self.config.clone(),
            self.is_append_only,
            &pk_column_names,
            &all_column_names,
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

pub struct RedShiftSinkWriter {
    augmented_row: AugmentedChunk,
    jdbc_sink_writer: CoordinatedRemoteSinkWriter,
}

impl RedShiftSinkWriter {
    pub async fn new(
        config: RedShiftConfig,
        is_append_only: bool,
        writer_param: super::SinkWriterParam,
        mut param: SinkParam,
    ) -> Result<Self> {
        let metrics = SinkWriterMetrics::new(&writer_param);
        let column_descs = &mut param.columns;
        param.properties.remove("create_table_if_not_exists");
        param.properties.remove("schedule_seconds");
        let full_table_name = if is_append_only {
            config.table
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
            config.cdc_table.ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "cdc_table.name is required for non-append-only sink"
                ))
            })?
        };
        param.properties.remove("cdc_table.name");
        param
            .properties
            .insert("table.name".to_owned(), full_table_name.clone());
        param
            .properties
            .insert("type".to_owned(), "append-only".to_owned());

        let jdbc_sink_writer =
            CoordinatedRemoteSinkWriter::new(param.clone(), metrics.clone()).await?;
        Ok(Self {
            augmented_row: AugmentedChunk::new(0, is_append_only),
            jdbc_sink_writer,
        })
    }
}

#[async_trait]
impl SinkWriter for RedShiftSinkWriter {
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

pub struct RedshiftSinkCommitter {
    client: JdbcJniClient,
    table_name: String,
    cdc_table_name: Option<String>,
    pk_column_names: Vec<String>,
    all_column_names: Vec<String>,
    schedule: u64,
    is_append_only: bool,
    _periodic_task_handle: Option<tokio::task::JoinHandle<()>>,
    shutdown_sender: Option<tokio::sync::mpsc::UnboundedSender<()>>,
}

impl RedshiftSinkCommitter {
    pub fn new(
        config: RedShiftConfig,
        is_append_only: bool,
        pk_column_names: &Vec<String>,
        all_column_names: &Vec<String>,
    ) -> Result<Self> {
        let client = config.build_client()?;
        let schedule = config.schedule;
        let (periodic_task_handle, shutdown_sender) = if !is_append_only {
            let target_table_name = config.table.clone();
            let cdc_table_name = config.cdc_table.clone().ok_or_else(|| {
                SinkError::Config(anyhow!(
                    "cdc_table.name is required for non-append-only sink"
                ))
            })?;
            // Create shutdown channel
            let (shutdown_sender, shutdown_receiver) = unbounded_channel();

            // Clone client for the periodic task
            let task_client = config.build_client()?;

            let pk_column_names = pk_column_names.clone();
            let all_column_names = all_column_names.clone();
            // Start periodic task that runs every hour
            let periodic_task_handle = tokio::spawn(async move {
                Self::run_periodic_query_task(
                    task_client,
                    &cdc_table_name,
                    &target_table_name,
                    pk_column_names,
                    all_column_names,
                    schedule,
                    shutdown_receiver,
                )
                .await;
            });
            (Some(periodic_task_handle), Some(shutdown_sender))
        } else {
            (None, None)
        };

        Ok(Self {
            client,
            table_name: config.table.clone(),
            cdc_table_name: config.cdc_table.clone(),
            pk_column_names: pk_column_names.clone(),
            all_column_names: all_column_names.clone(),
            is_append_only,
            schedule,
            _periodic_task_handle: periodic_task_handle,
            shutdown_sender,
        })
    }

    /// Runs a periodic query task every hour
    async fn run_periodic_query_task(
        client: JdbcJniClient,
        cdc_table_name: &str,
        target_table_name: &str,
        pk_column_names: Vec<String>,
        all_column_names: Vec<String>,
        schedule: u64,
        mut shutdown_receiver: tokio::sync::mpsc::UnboundedReceiver<()>,
    ) {
        let mut interval_timer = interval(Duration::from_secs(schedule)); // 1 hour = 3600 seconds
        interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let sql = build_create_merge_into_task_sql(
            cdc_table_name,
            target_table_name,
            &pk_column_names,
            &all_column_names,
        );
        loop {
            tokio::select! {
                // Check for shutdown signal
                _ = shutdown_receiver.recv() => {
                    tracing::info!("Periodic query task received shutdown signal, stopping");
                    break;
                }
                // Execute periodic query
                _ = interval_timer.tick() => {

                    match client.execute_sql_sync(&sql) {
                        Ok(_) => {
                            tracing::info!("Periodic query executed successfully for table: {}", target_table_name);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to execute periodic query for table {}: {}", target_table_name, e);
                        }
                    }
                }
            }
        }
    }
}

impl Drop for RedshiftSinkCommitter {
    fn drop(&mut self) {
        // Send shutdown signal to the periodic task
        if let Some(shutdown_sender) = &self.shutdown_sender
            && let Err(e) = shutdown_sender.send(())
        {
            tracing::warn!("Failed to send shutdown signal to periodic task: {}", e);
        }
        tracing::info!("RedshiftSinkCommitter dropped, periodic task stopped");
    }
}

#[async_trait]
impl SinkCommitCoordinator for RedshiftSinkCommitter {
    async fn init(&mut self, _subscriber: SinkCommittedEpochSubscriber) -> Result<Option<u64>> {
        Ok(None)
    }

    async fn commit(
        &mut self,
        _epoch: u64,
        _metadata: Vec<SinkMetadata>,
        add_columns: Option<Vec<Field>>,
    ) -> Result<()> {
        if let Some(add_columns) = add_columns {
            if let Some(shutdown_sender) = &self.shutdown_sender {
                // Send shutdown signal to the periodic task before altering the table
                shutdown_sender.send(()).map_err(|e| {
                    SinkError::Config(anyhow!("Failed to send shutdown signal: {}", e))
                })?;
            }
            let sql = build_alter_add_column_sql(
                &self.table_name,
                &add_columns
                    .iter()
                    .map(|f| (f.name.clone(), f.data_type.to_string()))
                    .collect::<Vec<_>>(),
            );
            self.client.execute_sql_sync(&vec![sql])?;
            if !self.is_append_only {
                let cdc_table_name = self.cdc_table_name.as_ref().ok_or_else(|| {
                    SinkError::Config(anyhow!(
                        "cdc_table.name is required for non-append-only sink"
                    ))
                })?;
                let sql = build_alter_add_column_sql(
                    cdc_table_name,
                    &add_columns
                        .iter()
                        .map(|f| (f.name.clone(), f.data_type.to_string()))
                        .collect::<Vec<_>>(),
                );
                self.client.execute_sql_sync(&vec![sql])?;
                self.all_column_names
                    .extend(add_columns.iter().map(|f| f.name.clone()));

                let (shutdown_sender, shutdown_receiver) = unbounded_channel();
                let client = self.client.clone();
                let cdc_table_name = self.cdc_table_name.clone().unwrap();
                let target_table_name = self.table_name.clone();
                let pk_column_names = self.pk_column_names.clone();
                let all_column_names = self.all_column_names.clone();
                let schedule = self.schedule;
                let periodic_task_handle = tokio::spawn(async move {
                    Self::run_periodic_query_task(
                        client,
                        &cdc_table_name,
                        &target_table_name,
                        pk_column_names,
                        all_column_names,
                        schedule,
                        shutdown_receiver,
                    )
                    .await;
                });
                self.shutdown_sender = Some(shutdown_sender);
                self._periodic_task_handle = Some(periodic_task_handle);
            }
        }
        Ok(())
    }
}

pub fn build_create_table_sql(
    full_table_name: &str,
    schema: &Schema,
    need_op_and_row_id: bool,
) -> Result<String> {
    let mut columns: Vec<String> = schema
        .fields
        .iter()
        .map(|field| {
            let data_type = convert_redshift_data_type(&field.data_type)?;
            Ok(format!("{} {}", field.name, data_type))
        })
        .collect::<Result<Vec<String>>>()?;
    if need_op_and_row_id {
        columns.push(format!("{} VARCHAR", SNOWFLAKE_SINK_ROW_ID));
        columns.push(format!("{} INT", SNOWFLAKE_SINK_OP));
    }
    let columns_str = columns.join(", ");
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        full_table_name, columns_str
    ))
}

fn convert_redshift_data_type(data_type: &DataType) -> Result<String> {
    let data_type = match data_type {
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INTEGER".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "REAL".to_owned(),
        DataType::Float64 => "FLOAT".to_owned(),
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Varchar => "VARCHAR".to_owned(),
        DataType::Date => "DATE".to_owned(),
        DataType::Timestamp => "TIMESTAMP".to_owned(),
        DataType::Timestamptz => "TIMESTAMPTZ".to_owned(),
        DataType::Jsonb => "VARCHAR".to_owned(),
        DataType::Decimal => "DECIMAL".to_owned(),
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

fn build_create_merge_into_task_sql(
    cdc_table_name: &str,
    target_table_name: &str,
    pk_column_names: &Vec<String>,
    all_column_names: &Vec<String>,
) -> Vec<String> {
    let pk_names_str = pk_column_names.join(", ");
    let pk_names_eq_str = pk_column_names
        .iter()
        .map(|name| format!("{target_table_name}.{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(" AND ");
    let all_column_names_set_str = all_column_names
        .iter()
        .map(|name| format!("{name} = source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");
    let all_column_names_str = all_column_names.join(", ");
    let all_column_names_insert_str = all_column_names
        .iter()
        .map(|name| format!("source.{name}", name = name))
        .collect::<Vec<String>>()
        .join(", ");

    vec![
        format!(
            r#"
            CREATE TEMP TABLE max_id_table AS
            SELECT COALESCE(MAX({redshift_sink_row_id}), '0') AS max_row_id
            FROM {cdc_table_name};
            "#,
            redshift_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
            cdc_table_name = cdc_table_name,
        ),
        format!(
            r#"
            DELETE FROM {target_table_name}
            USING (
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {pk_names_str}
                        ORDER BY {redshift_sink_row_id} DESC
                    ) AS dedupe_id
                    FROM {cdc_table_name}, max_id_table
                    WHERE {cdc_table_name}.{redshift_sink_row_id} <= max_id_table.max_row_id
                ) AS subquery
                WHERE dedupe_id = 1 AND {redshift_sink_op} IN (2, 4)
            ) AS source
            WHERE {pk_names_eq_str};
            "#,
            target_table_name = target_table_name,
            pk_names_str = pk_names_str,
            redshift_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
            cdc_table_name = cdc_table_name,
            redshift_sink_op = SNOWFLAKE_SINK_OP,
            pk_names_eq_str = pk_names_eq_str,
        ),
        format!(
            r#"
            MERGE INTO {target_table_name}
            USING (
                SELECT *
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY {pk_names_str}
                        ORDER BY {redshift_sink_row_id} DESC
                    ) AS dedupe_id
                    FROM {cdc_table_name}, max_id_table
                    WHERE {cdc_table_name}.{redshift_sink_row_id} <= max_id_table.max_row_id
                ) AS subquery
                WHERE dedupe_id = 1 AND {redshift_sink_op} IN (1, 3)
            ) AS source
            ON {pk_names_eq_str}
            WHEN MATCHED THEN
                UPDATE SET {all_column_names_set_str}
            WHEN NOT MATCHED THEN
                INSERT ({all_column_names_str}) VALUES ({all_column_names_insert_str});
            "#,
            target_table_name = target_table_name,
            pk_names_str = pk_names_str,
            redshift_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
            cdc_table_name = cdc_table_name,
            redshift_sink_op = SNOWFLAKE_SINK_OP,
            pk_names_eq_str = pk_names_eq_str,
            all_column_names_set_str = all_column_names_set_str,
            all_column_names_str = all_column_names_str,
            all_column_names_insert_str = all_column_names_insert_str,
        ),
        format!(
            r#"
            DELETE FROM {cdc_table_name}
            USING max_id_table
            WHERE {cdc_table_name}.{redshift_sink_row_id} <= max_id_table.max_row_id;
            "#,
            cdc_table_name = cdc_table_name,
            redshift_sink_row_id = SNOWFLAKE_SINK_ROW_ID,
        ),
        "DROP TABLE IF EXISTS max_id_table;".to_owned(),
    ]
}
