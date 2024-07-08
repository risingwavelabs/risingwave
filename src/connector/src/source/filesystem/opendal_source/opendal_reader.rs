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
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use thiserror_ext::AsReport;
use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::errors::ParquetError;
use parquet::file::footer::{decode_footer, decode_metadata};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::FOOTER_SIZE;
use risingwave_common::array::StreamChunk;
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
                // If the format is "parquet", use `ParquetParser` to convert `record_batch` into stream chunk.
                let file_reader = ParquetFileReader {
                    op: self.connector.op.clone(),
                    path: split.name.clone(),
                };

                let record_batch_stream = ParquetRecordBatchStreamBuilder::new(file_reader)
                    .await?
                    .with_batch_size(self.source_ctx.source_ctrl_opts.chunk_size)
                    .build()?;

                let parquet_parser =
                    ParquetParser::new(self.parser_config.common.rw_columns.clone())?;
                msg_stream = parquet_parser.into_stream(record_batch_stream, object_name);
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

/// Since the `Reader` does not implement `tokio::io::AsyncRead` after `OpenDAL` 0.47, we construct a struct `ParquetFileReader` that implements `AsyncFileReader`, which is used as a parameter of `ParquetRecordBatchStreamBuilder`.
/// The following code refers to <`https://github.com/icelake-io/icelake/blob/07d53893d7788b4e41fc11efad8a6be828405c31/icelake/src/io/scan.rs#L196`>, we can remove the following implementation after `ParquetFileReader` is changed to a public struct in icelake. 
pub struct ParquetFileReader {
    op: Operator,
    path: String,
}

impl AsyncFileReader for ParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        Box::pin(async move {
            self.op
                .read_with(&self.path)
                .range(range.start as u64..range.end as u64)
                .await
                .map(|data| data.to_bytes())
                .map_err(|e| ParquetError::General(format!("{}", e.as_report())))
        })
    }

    /// Get the metadata of the parquet file.
    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async {
            let file_size = self
                .op
                .stat(&self.path)
                .await
                .map_err(|e| ParquetError::General(format!("{}", e.as_report())))?
                .content_length();

            if file_size < (FOOTER_SIZE as u64) {
                return Err(ParquetError::General(
                    "Invalid Parquet file. Size is smaller than footer".to_string(),
                ));
            }

            let mut footer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
            {
                let footer_buffer = self
                    .op
                    .read_with(&self.path)
                    .range((file_size - (FOOTER_SIZE as u64))..file_size)
                    .await
                    .map_err(|e| ParquetError::General(format!("{}", e.as_report())))?
                    .to_bytes();

                assert_eq!(footer_buffer.len(), FOOTER_SIZE);
                footer.copy_from_slice(&footer_buffer);
            }

            let metadata_len = decode_footer(&footer)?;
            let footer_metadata_len = FOOTER_SIZE + metadata_len;

            if footer_metadata_len > file_size as usize {
                return Err(ParquetError::General(format!(
                    "Invalid Parquet file. Reported metadata length of {} + {} byte footer, but file is only {} bytes",
                    metadata_len,
                    FOOTER_SIZE,
                    file_size
                )));
            }

            let start = file_size - footer_metadata_len as u64;
            let metadata_bytes = self
                .op
                .read_with(&self.path)
                .range(start..(start + metadata_len as u64))
                .await
                .map_err(|e| ParquetError::General(format!("{}", e.as_report())))?
                .to_bytes();
            Ok(Arc::new(decode_metadata(&metadata_bytes)?))
        })
    }
}
