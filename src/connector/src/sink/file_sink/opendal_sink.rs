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
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use arrow_schema_iceberg::SchemaRef;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use opendal::{FuturesAsyncWriter, Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use with_options::WithOptions;

use crate::sink::batching_log_sink::BatchingLogSinkerOf;
use crate::sink::catalog::SinkEncode;
use crate::sink::log_store::ChunkId;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
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
    type LogSinker = BatchingLogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = S::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::FileSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;
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
            self.is_append_only,
            writer_param.executor_id,
            self.format_desc.encode.clone(),
            self.engine_type.clone(),
            self.batching_strategy.clone(),
        )?;
        let commit_checkpoint_interval = NonZeroU64::new(1).unwrap();
        Ok(BatchingLogSinkerOf::new(
            writer,
            self.batching_strategy.clone(),
            writer_param.sink_metrics,
            commit_checkpoint_interval,
        ))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_btreemap(param.properties)?;
        let path = S::get_path(config.clone()).to_string();
        let op = S::new_operator(config.clone())?;
        let batching_strategy = S::get_batching_strategy(config.clone());
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

pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    operator: Operator,
    sink_writer: Option<FileWriterEnum>,
    is_append_only: bool,
    write_path: String,
    epoch: Option<u64>,
    executor_id: u64,
    encode_type: SinkEncode,
    engine_type: EngineType,
    pub(crate) batching_strategy: BatchingStrategy,
    current_bached_row_num: usize,
    current_writer_idx: usize,
    created_time: SystemTime,
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

#[async_trait]
impl SinkWriter for OpenDalSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        todo!()
        // if self.sink_writer.is_none() {
        //     self.create_sink_writer(self.current_writer_idx).await?;
        // }
        // if self.is_append_only {
        //     self.append_only(chunk).await
        // } else {
        //     // currently file sink only supports append only mode.
        //     unimplemented!()
        // }
    }

    async fn write_batch_and_try_finish(
        &mut self,
        chunk: StreamChunk,
        chunk_id: usize,
    ) -> Result<bool> {
        self.try_finish_write_via_rollover_interval().await?;

        if self.sink_writer.is_none() {
            self.create_sink_writer(self.current_writer_idx).await?;
        }
        if self.is_append_only {
            self.append_only(chunk).await
        } else {
            // currently file sink only supports append only mode.
            unimplemented!()
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.epoch = Some(epoch);
        Ok(())
    }

    /// For the file sink, currently, the sink decoupling feature is not enabled.
    /// When a checkpoint arrives, the force commit is performed to write the data to the file.
    /// In the future if flush and checkpoint is decoupled, we should enable sink decouple accordingly.
    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        self.try_finish_write_via_rollover_interval().await?;
        Ok(())
    }
}

impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        is_append_only: bool,
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
            is_append_only,
            epoch: None,
            executor_id,

            encode_type,
            engine_type,
            batching_strategy,
            current_bached_row_num: 0,
            current_writer_idx: 0,
            created_time: SystemTime::now(),
        })
    }

    pub fn partition_granularity(&self) -> String {
        match self.created_time.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                let datetime: DateTime<Utc> = DateTime::from_utc(
                    NaiveDateTime::from_timestamp(duration.as_secs() as i64, 0),
                    Utc,
                );
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

    async fn try_finish_write_via_rollover_interval(&mut self) -> Result<()> {
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
                            "The duration {:?}s of writing to this file has exceeded the rollover_interval, writer {:?}_{:?}_{:?} finish write file, metadata: {:?}",
                            self.duration_seconds_since_writer_created(),
                            self.created_time
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards")
                            .as_secs(),
                            self.executor_id,
                            self.current_writer_idx,
                            metadata
                        );
                        self.current_writer_idx += 1;
                    }
                }
            };
        }

        Ok(())
    }

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
                                "The number of written rows {} has reached the preset max_row_count, writer {:?}_{:?}_{:?} finish write file, metadata: {:?}",
                                self.current_bached_row_num,
                                self.created_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs(),
                                self.executor_id,
                                self.current_writer_idx,
                                metadata
                            );
                    self.current_bached_row_num = 0;
                }
            };
            self.current_writer_idx += 1;

            return Ok(true);
        }
        return Ok(false);
    }

    fn duration_seconds_since_writer_created(&self) -> usize {
        let now = SystemTime::now();
        now.duration_since(self.created_time)
            .expect("Time went backwards")
            .as_secs() as usize
    }

    async fn create_object_writer(&mut self, writer_idx: usize) -> Result<OpendalWriter> {
        // Todo: specify more file suffixes based on encode_type.
        let suffix = match self.encode_type {
            SinkEncode::Parquet => "parquet",
            _ => unimplemented!(),
        };

        // Note: sink decoupling is not currently supported, which means that output files will not be batched across checkpoints.
        // The current implementation writes files every time a checkpoint arrives, so the naming convention is `epoch + executor_id + .suffix`.
        let object_name = match self.engine_type {
            // For the local fs sink, the data will be automatically written to the defined path.
            // Therefore, there is no need to specify the path in the file name.
            EngineType::Fs => {
                format!(
                    "{}{}_{}_{}.{}",
                    self.partition_granularity(),
                    self.executor_id,
                    self.created_time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs(),
                    writer_idx,
                    suffix
                )
            }
            _ => format!(
                "{}/{}{}_{}_{}.{}",
                self.write_path,
                self.partition_granularity(),
                self.executor_id,
                self.created_time
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                writer_idx,
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

    async fn create_sink_writer(&mut self, writer_idx: usize) -> Result<()> {
        let object_writer = self.create_object_writer(writer_idx).await?;
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
                return Ok(res);
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
#[derive(Deserialize, Debug, Clone)]
pub struct BatchingStrategy {
    pub max_row_count: Option<usize>,
    pub max_file_size: Option<usize>,
    pub rollover_seconds: Option<usize>,
    pub partition_granularity: PartitionGranularity,
}

#[derive(Deserialize, Debug, Clone)]
pub enum PartitionGranularity {
    None,
    Day,
    Month,
    Hour,
}
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct FileSinkBatchingStrategy {
    #[serde(rename = "max_row_count")]
    pub max_row_count: Option<String>,
    #[serde(rename = "max_file_size")]
    pub max_file_size: Option<usize>,
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
