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
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use opendal::{FuturesAsyncWriter, Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::arrow_schema_iceberg::{self, SchemaRef};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use with_options::WithOptions;

use crate::sink::batching_log_sink::BatchingLogSinkerOf;
use crate::sink::catalog::SinkEncode;
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkFormatDesc, SinkParam, SinkWriter,
};
use crate::source::TryFromBTreeMap;
use crate::with_options::WithOptions;

/// The `FileSink` struct represents a file sink that uses the `OpendalSinkBackend` trait for its backend implementation.
///
/// # Type Parameters
///
/// - S: The type parameter S represents the concrete implementation of the `OpendalSinkBackend` trait used by this file sink.
#[derive(Debug, Clone)]
pub struct FileSink<S: OpendalSinkBackend> {
    pub(crate) op: Operator,
    /// The path to the file where the sink writes data.
    pub(crate) path: String,
    /// The schema describing the structure of the data being written to the file sink.
    pub(crate) schema: Schema,
    pub(crate) is_append_only: bool,
    /// The batching strategy for sinking data to files.
    pub(crate) batching_strategy: BatchingStrategy,

    /// The description of the sink's format.
    pub(crate) format_desc: SinkFormatDesc,
    pub(crate) engine_type: EngineType,
    pub(crate) _marker: PhantomData<S>,
}

/// The `OpendalSinkBackend` trait unifies the behavior of various sink backends
/// implemented through `OpenDAL`(`<https://github.com/apache/opendal>`).
///
/// # Type Parameters
///
/// - Properties: Represents the necessary parameters for establishing a backend.
///
/// # Constants
///
/// - `SINK_NAME`: A static string representing the name of the sink.
///
/// # Functions
///
/// - `from_btreemap`: Automatically parse the required parameters from the input create sink statement.
/// - `new_operator`: Creates a new operator using the provided backend properties.
/// - `get_path`: Returns the path of the sink file specified by the user's create sink statement.
pub trait OpendalSinkBackend: Send + Sync + 'static + Clone + PartialEq {
    type Properties: TryFromBTreeMap + Send + Sync + Clone + WithOptions;
    const SINK_NAME: &'static str;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties>;
    fn new_operator(properties: Self::Properties) -> Result<Operator>;
    fn get_path(properties: Self::Properties) -> String;
    fn get_engine_type() -> EngineType;
    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy;
}

#[derive(Clone, Debug)]
pub enum EngineType {
    Gcs,
    S3,
    Fs,
    Azblob,
    Webhdfs,
}

impl<S: OpendalSinkBackend> Sink for FileSink<S> {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = BatchingLogSinkerOf;

    const SINK_NAME: &'static str = S::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports append-only mode at present. \
                    Please change the query to append-only, and specify it \
                    explicitly after the `FORMAT ... ENCODE ...` statement. \
                    For example, `FORMAT xxx ENCODE xxx(force_append_only='true')`"
            )));
        }
        if self.format_desc.encode != SinkEncode::Parquet {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports `PARQUET` encode at present."
            )));
        }
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = OpenDalSinkWriter::new(
            self.op.clone(),
            &self.path,
            self.schema.clone(),
            writer_param.executor_id,
            self.format_desc.encode.clone(),
            self.engine_type.clone(),
            self.batching_strategy.clone(),
        )?;
        Ok(BatchingLogSinkerOf::new(writer))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_btreemap(param.properties)?;
        let path = S::get_path(config.clone()).to_string();
        let op = S::new_operator(config.clone())?;
        let mut batching_strategy = S::get_batching_strategy(config.clone());
        // If no batching strategy is defined, the default batching strategy will be used, which writes a file every 10 seconds.
        if batching_strategy.max_row_count.is_none() && batching_strategy.rollover_seconds.is_none()
        {
            batching_strategy.rollover_seconds = Some(10);
        }
        let engine_type = S::get_engine_type();
        Ok(Self {
            op,
            path,
            schema,
            is_append_only: param.sink_type.is_append_only(),
            batching_strategy,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            engine_type,
            _marker: PhantomData,
        })
    }
}

impl<S: OpendalSinkBackend> FileSink<S> {}
pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    operator: Operator,
    sink_writer: Option<FileWriterEnum>,
    write_path: String,
    epoch: Option<u64>,
    executor_id: u64,
    encode_type: SinkEncode,
    engine_type: EngineType,
    pub(crate) batching_strategy: BatchingStrategy,
    current_bached_row_num: usize,
    created_time: SystemTime,
    written_chunk_id: Option<usize>,
}

/// The `FileWriterEnum` enum represents different types of file writers used for various sink
/// implementations.
///
/// # Variants
///
/// - `ParquetFileWriter`: Represents a Parquet file writer using the `AsyncArrowWriter<W>`
///   for writing data to a Parquet file. It accepts an implementation of W: `AsyncWrite` + `Unpin` + `Send`
///   as the underlying writer. In this case, the `OpendalWriter` serves as the underlying writer.
///
/// The choice of writer used during the actual writing process depends on the encode type of the sink.
enum FileWriterEnum {
    ParquetFileWriter(AsyncArrowWriter<Compat<FuturesAsyncWriter>>),
}
impl OpenDalSinkWriter {
    // Writes a stream chunk to the sink and tries to close the writer
    /// according to the batching strategy.
    ///
    /// This method writes a chunk and attempts to complete the file write
    /// based on the writer's internal batching strategy. If the write is
    /// successful, it returns the last `chunk_id` that was written;
    /// otherwise, it returns None.
    ///
    ///
    /// This method is currently intended for use by the file sink's writer.
    pub async fn write_batch_and_try_finish(
        &mut self,
        chunk: StreamChunk,
        chunk_id: usize,
    ) -> Result<Option<usize>> {
        if self.sink_writer.is_none() {
            self.create_sink_writer().await?;
        };

        // While writing this chunk (via `append_only`), it checks if the maximum row count
        // has been reached to determine if the file should be finalized.
        // Then, it checks the rollover interval to see if the time-based batching threshold
        // has been met. If either condition results in a successful write, the write is marked as complete.
        let finish_write =
            self.append_only(chunk).await? || self.try_finish_write_via_rollover_interval().await?;
        self.written_chunk_id = Some(chunk_id);
        if finish_write {
            Ok(Some(chunk_id))
        } else {
            Ok(None)
        }
    }

    /// Attempts to complete the file write using the writer's internal batching strategy.
    ///
    /// If the write is completed, returns the last `chunk_id` that was written;
    /// otherwise, returns None.
    ///
    /// For the file sink, currently, the sink decoupling feature is not enabled.
    /// When a checkpoint arrives, the force commit is performed to write the data to the file.
    /// In the future if flush and checkpoint is decoupled, we should enable sink decouple accordingly.
    pub async fn try_finish(&mut self) -> Result<Option<usize>> {
        match self.try_finish_write_via_rollover_interval().await? {
            true => Ok(self.written_chunk_id),
            false => Ok(None),
        }
    }
}
#[async_trait]
impl SinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, _chunk: StreamChunk) -> Result<()> {
        Ok(())
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = Some(epoch);
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        executor_id: u64,
        encode_type: SinkEncode,
        engine_type: EngineType,
        batching_strategy: BatchingStrategy,
    ) -> Result<Self> {
        let arrow_schema = convert_rw_schema_to_arrow_schema(rw_schema)?;
        Ok(Self {
            schema: Arc::new(arrow_schema),
            write_path: write_path.to_string(),
            operator,
            sink_writer: None,
            epoch: None,
            executor_id,

            encode_type,
            engine_type,
            batching_strategy,
            current_bached_row_num: 0,
            created_time: SystemTime::now(),
            written_chunk_id: None,
        })
    }

    pub fn partition_granularity(&self) -> String {
        match self.created_time.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                let datetime = Utc
                    .timestamp_opt(duration.as_secs() as i64, 0)
                    .single()
                    .expect("Failed to convert timestamp to DateTime<Utc>");
                match self.batching_strategy.partition_granularity {
                    PartitionGranularity::None => "".to_string(),
                    PartitionGranularity::Day => datetime.format("%Y-%m-%d/").to_string(),
                    PartitionGranularity::Month => datetime.format("/%Y-%m/").to_string(),
                    PartitionGranularity::Hour => datetime.format("/%Y-%m-%d %H:00/").to_string(),
                }
            }
            Err(_) => "Invalid time".to_string(),
        }
    }

    /// Attempts to finalize the writing process based on a specified rollover interval.
    ///
    /// This asynchronous function checks if the duration since the writer was
    /// created exceeds the configured rollover interval (`rollover_seconds`).
    /// If the duration condition is met and a sink writer is available, it
    /// closes the current writer (e.g., a Parquet file writer) if any bytes
    /// have been written. The function logs the completion message and
    /// increments the writer index for subsequent writes.
    ///
    /// Returns:
    /// - `Ok(true)` if the write was finalized successfully due to the
    ///   rollover interval being exceeded.
    /// - `Ok(false)` if the rollover interval has not been met, indicating that
    ///   no action was taken.
    async fn try_finish_write_via_rollover_interval(&mut self) -> Result<bool> {
        if self.duration_seconds_since_writer_created()
            >= self
                .batching_strategy
                .clone()
                .rollover_seconds
                .unwrap_or(usize::MAX)
            && let Some(sink_writer) = self.sink_writer.take()
        {
            match sink_writer {
                FileWriterEnum::ParquetFileWriter(w) => {
                    if w.bytes_written() > 0 {
                        let metadata = w.close().await?;
                        tracing::info!(
                            "The duration {:?}s of writing to this file has exceeded the rollover_interval, writer {:?}_{:?}finish write file, metadata: {:?}",
                            self.duration_seconds_since_writer_created(),
                            self.created_time
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs(),
                            self.executor_id,
                            metadata
                        );
                        return Ok(true);
                    }
                }
            };
        }

        Ok(false)
    }

    /// Attempts to finalize the writing process if the current number of
    /// batched rows has reached the specified threshold.
    ///
    /// This asynchronous function checks if the number of rows accumulated
    /// (`current_bached_row_num`) meets or exceeds the maximum row count
    /// defined in the `batching_strategy`. If the threshold is met, it
    /// closes the current writer (e.g., a Parquet file writer), logs the
    /// completion message, and resets the row count. It also increments
    /// the writer index for subsequent writes.
    ///
    /// Returns:
    /// - `Ok(true)` if the write was finalized successfully.
    /// - `Ok(false)` if the threshold has not been met, indicating that
    ///   no action was taken.
    async fn try_finish_write_via_batched_rows(&mut self) -> Result<bool> {
        if self.current_bached_row_num
            >= self
                .batching_strategy
                .clone()
                .max_row_count
                .unwrap_or(usize::MAX)
            && let Some(sink_writer) = self.sink_writer.take()
        {
            match sink_writer {
                FileWriterEnum::ParquetFileWriter(w) => {
                    let metadata = w.close().await?;
                    tracing::info!(
                                "The number of written rows {} has reached the preset max_row_count, writer {:?}_{:?} finish write file, metadata: {:?}",
                                self.current_bached_row_num,
                                self.created_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs(),
                                self.executor_id,
                                metadata
                            );
                    self.current_bached_row_num = 0;
                }
            };

            return Ok(true);
        }
        Ok(false)
    }

    fn duration_seconds_since_writer_created(&self) -> usize {
        let now = SystemTime::now();
        now.duration_since(self.created_time)
            .expect("Time went backwards")
            .as_secs() as usize
    }

    async fn create_object_writer(&mut self) -> Result<OpendalWriter> {
        // Todo: specify more file suffixes based on encode_type.
        let suffix = match self.encode_type {
            SinkEncode::Parquet => "parquet",
            _ => unimplemented!(),
        };

        // With batching in place, the file writing process is decoupled from checkpoints.
        // The current file naming convention is as follows:
        // 1. A subdirectory is defined based on `partition_granularity` (e.g., by day、hour or month or none.).
        // 2. The file name includes the `executor_id` and the creation time in seconds since the UNIX epoch.
        // If the engine type is `Fs`, the path is automatically handled, and the filename does not include a path prefix.
        let object_name = match self.engine_type {
            // For the local fs sink, the data will be automatically written to the defined path.
            // Therefore, there is no need to specify the path in the file name.
            EngineType::Fs => {
                format!(
                    "{}{}_{}.{}",
                    self.partition_granularity(),
                    self.executor_id,
                    self.created_time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs(),
                    suffix
                )
            }
            _ => format!(
                "{}/{}{}_{}.{}",
                self.write_path,
                self.partition_granularity(),
                self.executor_id,
                self.created_time
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                suffix,
            ),
        };

        tracing::info!("create new writer, file name: {:?}", object_name);
        Ok(self
            .operator
            .writer_with(&object_name)
            .concurrent(8)
            .await?)
    }

    async fn create_sink_writer(&mut self) -> Result<()> {
        let object_writer = self.create_object_writer().await?;
        match self.encode_type {
            SinkEncode::Parquet => {
                let props = WriterProperties::builder();
                let parquet_writer: tokio_util::compat::Compat<opendal::FuturesAsyncWriter> =
                    object_writer.into_futures_async_write().compat_write();
                self.sink_writer = Some(FileWriterEnum::ParquetFileWriter(
                    AsyncArrowWriter::try_new(
                        parquet_writer,
                        self.schema.clone(),
                        Some(props.build()),
                    )?,
                ));
                self.current_bached_row_num = 0;

                self.created_time = SystemTime::now();
            }
            _ => unimplemented!(),
        }

        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<bool> {
        let (data_chunk, _) = chunk.compact().into_parts();
        match self
            .sink_writer
            .as_mut()
            .ok_or_else(|| SinkError::File("Sink writer is not created.".to_string()))?
        {
            FileWriterEnum::ParquetFileWriter(w) => {
                let batch =
                    IcebergArrowConvert.to_record_batch(self.schema.clone(), &data_chunk)?;
                let batch_row_nums = batch.num_rows();
                w.write(&batch).await?;
                self.current_bached_row_num += batch_row_nums;
                let res = self.try_finish_write_via_batched_rows().await?;
                Ok(res)
            }
        }
    }
}

fn convert_rw_schema_to_arrow_schema(
    rw_schema: risingwave_common::catalog::Schema,
) -> anyhow::Result<arrow_schema_iceberg::Schema> {
    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });
    let mut arrow_fields = vec![];
    for rw_field in &rw_schema.fields {
        let arrow_field = IcebergArrowConvert
            .to_arrow_field(&rw_field.name.clone(), &rw_field.data_type.clone())?;

        arrow_fields.push(arrow_field);
    }

    Ok(arrow_schema_iceberg::Schema::new(arrow_fields))
}

/// `BatchingStrategy` represents the strategy for batching data before writing to files.
///
/// This struct contains settings that control how data is collected and
/// partitioned based on specified criteria:
///
/// - `max_row_count`: Optional maximum number of rows to accumulate before writing.
/// - `rollover_seconds`: Optional time interval (in seconds) to trigger a write,
///   regardless of the number of accumulated rows.
/// - `partition_granularity`: Specifies how files are organized into directories
///   based on creation time (e.g., by day, month, or hour).
#[derive(Deserialize, Debug, Clone)]
pub struct BatchingStrategy {
    pub max_row_count: Option<usize>,
    pub rollover_seconds: Option<usize>,
    pub partition_granularity: PartitionGranularity,
}

/// `PartitionGranularity` defines the granularity of file partitions based on creation time.
///
/// Each variant specifies how files are organized into directories:
/// - `None`: No partitioning.
/// - `Day`: Files are written in a directory for each day.
/// - `Month`: Files are written in a directory for each month.
/// - `Hour`: Files are written in a directory for each hour.
#[derive(Deserialize, Debug, Clone)]
pub enum PartitionGranularity {
    None,
    Day,
    Month,
    Hour,
}

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FileSinkBatchingStrategyConfig {
    #[serde(rename = "max_row_count")]
    pub max_row_count: Option<String>,
    #[serde(rename = "rollover_seconds")]
    pub rollover_seconds: Option<String>,
    #[serde(rename = "partition_granularity")]
    pub partition_granularity: Option<String>,
}

pub fn parse_partition_granularity(granularity: &str) -> PartitionGranularity {
    let granularity = granularity.trim().to_lowercase();
    if matches!(&granularity[..], "day" | "month" | "hour") {
        match granularity.as_str() {
            "day" => PartitionGranularity::Day,
            "month" => PartitionGranularity::Month,
            "hour" => PartitionGranularity::Hour,
            _ => PartitionGranularity::None,
        }
    } else {
        PartitionGranularity::None
    }
}
