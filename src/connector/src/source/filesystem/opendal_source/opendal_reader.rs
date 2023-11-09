// Copyright 2023 RisingWave Labs
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

use std::pin::pin;

use anyhow::{Ok, Result};
use async_trait::async_trait;
use aws_smithy_http::byte_stream::ByteStream;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use risingwave_common::error::RwError;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use super::opendal_enumerator::OpendalConnector;
use super::GcsProperties;
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig};
use crate::source::filesystem::{nd_streaming, OpendalSplit};
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
    SplitReader, StreamChunkWithState,
};

const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const STREAM_READER_CAPACITY: usize = 4096;
pub struct OpendalReader {
    connector: OpendalConnector,
    splits: Vec<OpendalSplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}
#[async_trait]
impl SplitReader for OpendalReader {
    type Properties = GcsProperties;
    type Split = OpendalSplit;

    async fn new(
        properties: GcsProperties,
        splits: Vec<OpendalSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // match properties {
        //     OpenDALProperties::GcsProperties(gcs_properties) => {
        //         OpendalConnector::new_gcs_source(gcs_properties)
        //     }
        //     OpenDALProperties::S3Properties(s3_properties) => {
        //         OpendalConnector::new_s3_source(s3_properties)
        //     }
        // }
        let connector = OpendalConnector::new_gcs_source(properties)?;
        let opendal_reader = OpendalReader {
            connector,
            splits,
            parser_config,
            source_ctx,
        };
        Ok(opendal_reader)
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl OpendalReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    async fn into_chunk_stream(self) {
        for split in self.splits {
            let actor_id = self.source_ctx.source_info.actor_id.to_string();
            let source_id = self.source_ctx.source_info.source_id.to_string();
            let source_ctx = self.source_ctx.clone();

            let split_id = split.id();

            let data_stream =
                Self::stream_read_object(self.connector.op.clone(), split, self.source_ctx.clone());

            let parser =
                ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx).await?;
            let msg_stream = if matches!(
                parser,
                ByteStreamSourceParserImpl::Json(_) | ByteStreamSourceParserImpl::Csv(_)
            ) {
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
                    .with_label_values(&[&actor_id, &source_id, &split_id])
                    .inc_by(msg.chunk.cardinality() as u64);
                yield msg;
            }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn stream_read_object(
        op: Operator,
        split: OpendalSplit,
        source_ctx: SourceContextRef,
    ) {
        let actor_id = source_ctx.source_info.actor_id.to_string();
        let source_id = source_ctx.source_info.source_id.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();

        let object_name = split.name.clone();

        let byte_stream = Self::get_object(op, &object_name, split.offset).await?;

        let stream_reader = StreamReader::new(
            byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        let reader = pin!(BufReader::new(stream_reader));

        let stream = ReaderStream::with_capacity(reader, STREAM_READER_CAPACITY);

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
                    .with_label_values(&[&actor_id, &source_id, &split_id])
                    .inc_by(batch_size as u64);
                batch_size = 0;
                yield batch.clone();
                batch.clear();
            }
        }
        if !batch.is_empty() {
            source_ctx
                .metrics
                .partition_input_bytes
                .with_label_values(&[&actor_id, &source_id, &split_id])
                .inc_by(batch_size as u64);
            yield batch;
        }
    }

    pub async fn get_object(op: Operator, object_name: &str, start: usize) -> Result<ByteStream> {
        let mut reader = op.reader_with(object_name).range(start as u64..).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        let res = ByteStream::from(buffer);
        Ok(res)
    }
}
