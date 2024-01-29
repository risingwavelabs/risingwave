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

use anyhow::{Ok, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use risingwave_common::array::StreamChunk;
use risingwave_common::error::RwError;
use risingwave_pb::meta::table_fragments::fragment;
use tokio::io::BufReader;
use tokio_util::io::{ReaderStream, StreamReader};

use super::opendal_enumerator::OpendalEnumerator;
use super::OpendalSource;
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig};
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::{nd_streaming, OpendalFsSplit};
use crate::source::{
    self, BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
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
    ) -> Result<Self> {
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
        self.into_chunk_stream()
    }
}

impl<Src: OpendalSource> OpendalReader<Src> {
    #[try_stream(boxed, ok = StreamChunk, error = RwError)]
    async fn into_chunk_stream(self) {
        for split in self.splits {
            let actor_id = self.source_ctx.source_info.actor_id.to_string();
            let fragment_id = self.source_ctx.source_info.fragment_id.to_string();
            let source_id = self.source_ctx.source_info.source_id.to_string();
            let source_name = self.source_ctx.source_info.source_name.to_string();
            let source_ctx = self.source_ctx.clone();

            let split_id = split.id();

            let data_stream =
                Self::stream_read_object(self.connector.op.clone(), split, self.source_ctx.clone());

            let parser =
                ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx).await?;
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
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn stream_read_object(
        op: Operator,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
    ) {
        let actor_id = source_ctx.source_info.actor_id.to_string();
        let fragment_id = source_ctx.source_info.fragment_id.to_string();
        let source_id = source_ctx.source_info.source_id.to_string();
        let source_name = source_ctx.source_info.source_name.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();

        let object_name = split.name.clone();

        let reader = op
            .reader_with(&object_name)
            .range(split.offset as u64..)
            .await?;

        let stream_reader = StreamReader::new(
            reader.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
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
