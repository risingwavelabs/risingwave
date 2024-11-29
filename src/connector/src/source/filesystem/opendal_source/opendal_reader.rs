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

use std::future::IntoFuture;
use std::pin::Pin;

use arrow_schema_iceberg::{DataType as ArrowDateType, IntervalUnit};
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{parquet_to_arrow_schema, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::FileMetaData;
use risingwave_common::array::StreamChunk;
use risingwave_common::types::DataType as RwDataType;
use risingwave_common::util::tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use super::opendal_enumerator::OpendalEnumerator;
use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, EncodingProperties, ParquetParser, ParserConfig};
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::{nd_streaming, OpendalFsSplit};
use crate::source::{
    BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
    SplitReader,
};

const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
pub struct OpendalReader<Src: OpendalSource> {
    connector: OpendalEnumerator<Src>,
    splits: Vec<OpendalFsSplit<Src>>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
}
#[async_trait]
impl<Src: OpendalSource> SplitReader for OpendalReader<Src> {
    type Properties = Src::Properties;
    type Split = OpendalFsSplit<Src>;

    async fn new(
        properties: Src::Properties,
        splits: Vec<OpendalFsSplit<Src>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let connector = Src::new_enumerator(properties)?;
        let opendal_reader = OpendalReader {
            connector,
            splits,
            parser_config,
            source_ctx,
            columns,
        };
        Ok(opendal_reader)
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        self.into_stream_inner()
    }
}

impl<Src: OpendalSource> OpendalReader<Src> {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in self.splits {
            let source_ctx = self.source_ctx.clone();

            let object_name = split.name.clone();

            let msg_stream;

            if let EncodingProperties::Parquet = &self.parser_config.specific.encoding_config {
                // // If the format is "parquet", use `ParquetParser` to convert `record_batch` into stream chunk.
                let mut reader: tokio_util::compat::Compat<opendal::FuturesAsyncReader> = self
                    .connector
                    .op
                    .reader_with(&object_name)
                    .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
                    .await?
                    .into_futures_async_read(..)
                    .await?
                    .compat();
                let parquet_metadata = reader.get_metadata().await.map_err(anyhow::Error::from)?;

                let file_metadata = parquet_metadata.file_metadata();
                let column_indices =
                    extract_valid_column_indices(self.columns.clone(), file_metadata)?;
                let projection_mask =
                    ProjectionMask::leaves(file_metadata.schema_descr(), column_indices);
                // For the Parquet format, we directly convert from a record batch to a stream chunk.
                // Therefore, the offset of the Parquet file represents the current position in terms of the number of rows read from the file.
                let record_batch_stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await?
                    .with_batch_size(self.source_ctx.source_ctrl_opts.chunk_size)
                    .with_projection(projection_mask)
                    .with_offset(split.offset)
                    .build()?;

                let parquet_parser = ParquetParser::new(
                    self.parser_config.common.rw_columns.clone(),
                    object_name,
                    split.offset,
                )?;
                msg_stream = parquet_parser.into_stream(record_batch_stream);
            } else {
                let data_stream = Self::stream_read_object(
                    self.connector.op.clone(),
                    split,
                    self.source_ctx.clone(),
                    self.connector.compression_format.clone(),
                );

                let parser =
                    ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx)
                        .await?;
                msg_stream = if need_nd_streaming(&self.parser_config.specific.encoding_config) {
                    Box::pin(parser.into_stream(nd_streaming::split_stream(data_stream)))
                } else {
                    Box::pin(parser.into_stream(data_stream))
                };
            }
            #[for_await]
            for msg in msg_stream {
                let msg = msg?;
                yield msg;
            }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_object(
        op: Operator,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
        compression_format: CompressionFormat,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();
        let object_name = split.name.clone();
        let reader = op
            .read_with(&object_name)
            .range(split.offset as u64..)
            .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
            .await?;
        let stream_reader = StreamReader::new(
            reader.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        let buf_reader: Pin<Box<dyn AsyncRead + Send>> = match compression_format {
            CompressionFormat::Gzip => {
                let gzip_decoder = GzipDecoder::new(stream_reader);
                Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncRead + Send>>
            }
            CompressionFormat::None => {
                // todo: support automatic decompression of more compression types.
                if object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
                    let gzip_decoder = GzipDecoder::new(stream_reader);
                    Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncRead + Send>>
                } else {
                    Box::pin(BufReader::new(stream_reader)) as Pin<Box<dyn AsyncRead + Send>>
                }
            }
        };

        let mut offset: usize = split.offset;
        let mut batch_size: usize = 0;
        let mut batch = Vec::new();
        let partition_input_bytes_metrics = source_ctx
            .metrics
            .partition_input_bytes
            .with_guarded_label_values(&[
                &actor_id,
                &source_id,
                &split_id,
                &source_name,
                &fragment_id,
            ]);
        let stream = ReaderStream::with_capacity(buf_reader, STREAM_READER_CAPACITY);
        #[for_await]
        for read in stream {
            let bytes = read?;
            let len = bytes.len();
            let msg = SourceMessage {
                key: None,
                payload: Some(bytes.as_ref().to_vec()),
                offset: offset.to_string(),
                split_id: split.id(),
                meta: SourceMeta::Empty,
            };
            offset += len;
            batch_size += len;
            batch.push(msg);

            if batch.len() >= max_chunk_size {
                partition_input_bytes_metrics.inc_by(batch_size as u64);
                let yield_batch = std::mem::take(&mut batch);
                batch_size = 0;
                yield yield_batch;
            }
        }
        if !batch.is_empty() {
            partition_input_bytes_metrics.inc_by(batch_size as u64);
            yield batch;
        }
    }
}

/// Extracts valid column indices from a Parquet file schema based on the user's requested schema.
///
/// This function is used for column pruning of Parquet files. It calculates the intersection
/// between the columns in the currently read Parquet file and the schema provided by the user.
/// This is useful for reading a `RecordBatch` with the appropriate `ProjectionMask`, ensuring that
/// only the necessary columns are read.
///
/// # Parameters
/// - `columns`: A vector of `Column` representing the user's requested schema.
/// - `metadata`: A reference to `FileMetaData` containing the schema and metadata of the Parquet file.
///
/// # Returns
/// - A `ConnectorResult<Vec<usize>>`, which contains the indices of the valid columns in the
///   Parquet file schema that match the requested schema. If an error occurs during processing,
///   it returns an appropriate error.
pub fn extract_valid_column_indices(
    columns: Option<Vec<Column>>,
    metadata: &FileMetaData,
) -> ConnectorResult<Vec<usize>> {
    match columns {
        Some(rw_columns) => {
            let parquet_column_names = metadata
                .schema_descr()
                .columns()
                .iter()
                .map(|c| c.name())
                .collect_vec();

            let converted_arrow_schema =
                parquet_to_arrow_schema(metadata.schema_descr(), metadata.key_value_metadata())
                    .map_err(anyhow::Error::from)?;

            let valid_column_indices: Vec<usize> = rw_columns
                .iter()
                .filter_map(|column| {
                    parquet_column_names
                        .iter()
                        .position(|&name| name == column.name)
                        .and_then(|pos| {
                            let arrow_data_type: &arrow_schema_iceberg::DataType =
                                converted_arrow_schema.field(pos).data_type();
                            let rw_data_type: &risingwave_common::types::DataType =
                                &column.data_type;

                            if is_parquet_schema_match_source_schema(arrow_data_type, rw_data_type)
                            {
                                Some(pos)
                            } else {
                                None
                            }
                        })
                })
                .collect();
            Ok(valid_column_indices)
        }
        None => Ok(vec![]),
    }
}

/// This function checks whether the schema of a Parquet file matches the user defined schema.
/// It handles the following special cases:
/// - Arrow's `timestamp(_, None)` types (all four time units) match with RisingWave's `TimeStamp` type.
/// - Arrow's `timestamp(_, Some)` matches with RisingWave's `TimeStamptz` type.
/// - Since RisingWave does not have an `UInt` type:
///   - Arrow's `UInt8` matches with RisingWave's `Int16`.
///   - Arrow's `UInt16` matches with RisingWave's `Int32`.
///   - Arrow's `UInt32` matches with RisingWave's `Int64`.
///   - Arrow's `UInt64` matches with RisingWave's `Decimal`.
/// - Arrow's `Float16` matches with RisingWave's `Float32`.
fn is_parquet_schema_match_source_schema(
    arrow_data_type: &ArrowDateType,
    rw_data_type: &RwDataType,
) -> bool {
    matches!(
        (arrow_data_type, rw_data_type),
        (ArrowDateType::Boolean, RwDataType::Boolean)
            | (
                ArrowDateType::Int8 | ArrowDateType::Int16 | ArrowDateType::UInt8,
                RwDataType::Int16
            )
            | (
                ArrowDateType::Int32 | ArrowDateType::UInt16,
                RwDataType::Int32
            )
            | (
                ArrowDateType::Int64 | ArrowDateType::UInt32,
                RwDataType::Int64
            )
            | (
                ArrowDateType::UInt64 | ArrowDateType::Decimal128(_, _),
                RwDataType::Decimal
            )
            | (ArrowDateType::Decimal256(_, _), RwDataType::Int256)
            | (
                ArrowDateType::Float16 | ArrowDateType::Float32,
                RwDataType::Float32
            )
            | (ArrowDateType::Float64, RwDataType::Float64)
            | (ArrowDateType::Timestamp(_, None), RwDataType::Timestamp)
            | (
                ArrowDateType::Timestamp(_, Some(_)),
                RwDataType::Timestamptz
            )
            | (ArrowDateType::Date32, RwDataType::Date)
            | (
                ArrowDateType::Time32(_) | ArrowDateType::Time64(_),
                RwDataType::Time
            )
            | (
                ArrowDateType::Interval(IntervalUnit::MonthDayNano),
                RwDataType::Interval
            )
            | (
                ArrowDateType::Utf8 | ArrowDateType::LargeUtf8,
                RwDataType::Varchar
            )
            | (
                ArrowDateType::Binary | ArrowDateType::LargeBinary,
                RwDataType::Bytea
            )
            | (ArrowDateType::List(_), RwDataType::List(_))
            | (ArrowDateType::Struct(_), RwDataType::Struct(_))
            | (ArrowDateType::Map(_, _), RwDataType::Map(_))
    )
}
