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
use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Reader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, StreamChunk};
use risingwave_common::types::{Datum, ScalarImpl};
use tokio::io::BufReader;
use tokio_util::io::{ReaderStream, StreamReader};

use super::opendal_enumerator::OpendalEnumerator;
use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, EncodingProperties, ParserConfig};
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::{nd_streaming, OpendalFsSplit};
use crate::source::{
    BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
    SplitReader,
};

const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
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
        self.into_chunk_stream()
    }
}

impl<Src: OpendalSource> OpendalReader<Src> {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_chunk_stream(self) {
        let actor_id = self.source_ctx.actor_id.to_string();
        let fragment_id = self.source_ctx.fragment_id.to_string();
        let source_id = self.source_ctx.source_id.to_string();
        let source_name = self.source_ctx.source_name.to_string();

        for split in self.splits {
            let source_ctx = self.source_ctx.clone();
            let split_id = split.id();
            let file_reader = self
                .connector
                .op
                .reader_with(&split.name.clone())
                .range(split.offset as u64..)
                .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
                .await?;
            // // If the format is "parquet", there is no need to read it as bytes and parse it into a chunk using a parser.
            // // Instead, the Parquet file can be directly read as a `RecordBatch`` and converted into a chunk.
            // // For other formats, the corresponding parser will still be used for parsing.
            // if let EncodingProperties::Parquet = &self.parser_config.specific.encoding_config {
            //     let record_batch_stream: std::pin::Pin<Box<parquet::arrow::async_reader::ParquetRecordBatchStream<Reader>>> = Box::pin(
            //         ParquetRecordBatchStreamBuilder::new(file_reader)
            //             .await?
            //             .with_batch_size(self.source_ctx.source_ctrl_opts.chunk_size)
            //             .build()?,
            //     );

            //     #[for_await]
            //     for record_batch in record_batch_stream {
            //         let record_batch: RecordBatch = record_batch?;
            //         // Convert each record batch into a stream chunk according to user defined schema.
            //         let chunk: StreamChunk = convert_record_batch_to_stream_chunk(
            //             record_batch,
            //             self.columns.clone(),
            //             split.name.clone(),
            //         )?;

            //         self.source_ctx
            //             .metrics
            //             .partition_input_count
            //             .with_label_values(&[
            //                 &actor_id,
            //                 &source_id,
            //                 &split_id,
            //                 &source_name,
            //                 &fragment_id,
            //             ])
            //             .inc_by(chunk.cardinality() as u64);
            //         yield chunk;
            //     }
            // } else {
            let data_stream = Self::stream_read_object(file_reader, split, self.source_ctx.clone());

            let parser =
                ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx).await?;
            match parser{
                ByteStreamSourceParserImpl::Parquet(parquet_parser) => todo!(),
                _ => todo!(),
            }
                
            }
            let msg_stream = if need_nd_streaming(&self.parser_config.specific.encoding_config) {
                parser.into_stream(nd_streaming::split_stream(data_stream))
            } else {
                parser.into_stream(data_stream)
            };
            #[for_await]
            for msg in msg_stream {
                let msg = msg?;
                self.source_ctx
                    .metrics
                    .partition_input_count
                    .with_label_values(&[
                        &actor_id,
                        &source_id,
                        &split_id,
                        &source_name,
                        &fragment_id,
                    ])
                    .inc_by(msg.cardinality() as u64);
                yield msg;
            }
            // }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_object(
        file_reader: Reader,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();

        let stream_reader = StreamReader::new(
            file_reader.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );
        let buf_reader = BufReader::new(stream_reader);
        let stream = ReaderStream::with_capacity(buf_reader, STREAM_READER_CAPACITY);

        let mut offset: usize = split.offset;
        let mut batch_size: usize = 0;
        let mut batch = Vec::new();
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
                source_ctx
                    .metrics
                    .partition_input_bytes
                    .with_label_values(&[
                        &actor_id,
                        &source_id,
                        &split_id,
                        &source_name,
                        &fragment_id,
                    ])
                    .inc_by(batch_size as u64);
                let yield_batch = std::mem::take(&mut batch);
                batch_size = 0;
                yield yield_batch;
            }
        }
        if !batch.is_empty() {
            source_ctx
                .metrics
                .partition_input_bytes
                .with_label_values(&[&actor_id, &source_id, &split_id, &source_name, &fragment_id])
                .inc_by(batch_size as u64);
            yield batch;
        }
    }
}

/// The function `convert_record_batch_to_stream_chunk` is designed to transform the given `RecordBatch` into a `StreamChunk`.
///
/// For each column in the source column:
/// - If the column's schema matches a column in the `RecordBatch` (both the data type and column name are the same),
///   the corresponding records are converted into a column of the `StreamChunk`.
/// - If the column's schema does not match, null values are inserted.
/// - Hidden columns are handled separately by filling in the appropriate fields to ensure the data chunk maintains the correct format.
/// - If a column in the Parquet file does not exist in the source schema, it is skipped.
///
/// # Arguments
///
/// * `record_batch` - The `RecordBatch` to be converted into a `StreamChunk`.
/// * `source_columns` - User defined source schema.
///
/// # Returns
///
/// A `StreamChunk` containing the converted data from the `RecordBatch`.

// The hidden columns that must be included here are _rw_file and _rw_offset.
// Depending on whether the user specifies a primary key (pk), there may be an additional hidden column row_id.
// Therefore, the maximum number of hidden columns is three.
const MAX_HIDDEN_COLUMN_NUMS: usize = 3;
fn convert_record_batch_to_stream_chunk(
    record_batch: RecordBatch,
    source_columns: Option<Vec<Column>>,
    file_name: String,
) -> Result<StreamChunk, crate::error::ConnectorError> {
    match source_columns {
        Some(source_columns) => {
            let size = source_columns.len();
            let mut chunk_columns =
                Vec::with_capacity(source_columns.len() + MAX_HIDDEN_COLUMN_NUMS);
            for source_column in source_columns {
                if let Some(parquet_column) = record_batch.column_by_name(&source_column.name) {
                    let converted_arrow_data_type =
                        arrow_schema::DataType::try_from(&source_column.data_type)?;

                    if &converted_arrow_data_type == parquet_column.data_type() {
                        let column = Arc::new(parquet_column.try_into()?);
                        chunk_columns.push(column);
                    } else {
                        // data type mismatch, this column is set to null.
                        let mut array_builder =
                            ArrayBuilderImpl::with_type(size, source_column.data_type);

                        array_builder.append_n_null(record_batch.num_rows());
                        let res = array_builder.finish();
                        let column = Arc::new(res);
                        chunk_columns.push(column);
                    }
                } else if !source_column.is_visible {
                    // For hidden columns in the file source:
                    // - The `row_id` column, which will be filled by the row ID generator, is left as null.
                    // - The `_rw_file` column is filled with the object name.
                    // - The `_rw_offset` column is filled with the offset.
                    let mut array_builder =
                        ArrayBuilderImpl::with_type(size, source_column.data_type);
                    // Hidden column naming rules refer to https://github.com/risingwavelabs/risingwave/blob/3736982d6fe648ad32b50e1d3ce97119edcab1a6/src/connector/src/parser/additional_columns.rs#L289
                    let datum: Datum = if source_column.name.ends_with("file") {
                        Some(ScalarImpl::Utf8(file_name.clone().into()))
                    } else if source_column.name.ends_with("offset") {
                        // `_rw_offset` is not used.
                        Some(ScalarImpl::Utf8("0".into()))
                    } else {
                        // row_id column.
                        None
                    };

                    array_builder.append_n(record_batch.num_rows(), datum);
                    let res = array_builder.finish();
                    let column = Arc::new(res);
                    chunk_columns.push(column);
                } else {
                    // For columns defined in the source schema but not present in the Parquet file, null values are filled in.
                    let mut array_builder =
                        ArrayBuilderImpl::with_type(size, source_column.data_type);

                    array_builder.append_n_null(record_batch.num_rows());
                    let res = array_builder.finish();
                    let column = Arc::new(res);
                    chunk_columns.push(column);
                }
            }

            let data_chunk = DataChunk::new(chunk_columns.clone(), record_batch.num_rows());
            Ok(data_chunk.into())
        }
        None => {
            let mut chunk_columns = Vec::with_capacity(record_batch.num_columns());
            for array in record_batch.columns() {
                let column = Arc::new(array.try_into()?);
                chunk_columns.push(column);
            }
            Ok(DataChunk::new(chunk_columns, record_batch.num_rows()).into())
        }
    }
}
