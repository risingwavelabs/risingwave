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

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use risingwave_common::array::StreamChunk;
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
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let connector = Src::new_enumerator(properties)?;
        let opendal_reader = OpendalReader {
            connector,
            splits,
            parser_config,
            source_ctx,
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
                let reader: tokio_util::compat::Compat<opendal::FuturesAsyncReader> = self
                    .connector
                    .op
                    .reader_with(&object_name)
                    .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
                    .await?
                    .into_futures_async_read(..)
                    .await?
                    .compat();
                // For the Parquet format, we directly convert from a record batch to a stream chunk.
                // Therefore, the offset of the Parquet file represents the current position in terms of the number of rows read from the file.
                let record_batch_stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await?
                    .with_batch_size(self.source_ctx.source_ctrl_opts.chunk_size)
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
                source_ctx
                    .metrics
                    .partition_input_bytes
                    .with_guarded_label_values(&[
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
                .with_guarded_label_values(&[
                    &actor_id,
                    &source_id,
                    &split_id,
                    &source_name,
                    &fragment_id,
                ])
                .inc_by(batch_size as u64);
            yield batch;
        }
    }
}
