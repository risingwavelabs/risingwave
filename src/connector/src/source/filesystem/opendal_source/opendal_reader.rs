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

use anyhow::{anyhow, Result, Ok};
use async_trait::async_trait;
use aws_smithy_http::byte_stream::ByteStream;
use futures_async_stream::try_stream;
use risingwave_common::error::RwError;
use tokio::io::AsyncReadExt;
use tokio_stream::Stream;
use tokio::io::{AsyncRead, AsyncSeek};
use super::opendal_enumerator::OpenDALConnector;
use super::{GCSProperties, OpenDALProperties};
use crate::{parser::ParserConfig, source::StreamChunkWithState};
use crate::source::filesystem::GcsSplit;
use crate::source::{
    BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef, SourceMessage,
    SplitReader,
};
#[async_trait]
impl SplitReader for OpenDALConnector {
    type Properties = GCSProperties;
    type Split = GcsSplit;

    async fn new(
        properties: GCSProperties,
        splits: Vec<GcsSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // match properties {
        //     OpenDALProperties::GCSProperties(gcs_properties) => {
        //         OpenDALConnector::new_gcs_source(gcs_properties)
        //     }
        //     OpenDALProperties::S3Properties(s3_properties) => {
        //         OpenDALConnector::new_s3_source(s3_properties)
        //     }
        // }
        OpenDALConnector::new_gcs_source(properties)
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        todo!()
    }
}

impl OpenDALConnector{
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    async fn into_chunk_stream(self) {
        
    }

    async fn streaming_read(
        &self,
        path: &str,
        start_pos: Option<usize>,
    ) -> Result<()>{
        let reader = match start_pos {
            Some(start_position) => {
                self.op
                    .reader_with(path)
                    .range(start_position as u64..)
                    .await?
            }
            None => self.op.reader(path).await?,
        };
        Ok(())
    }

    pub async fn get_object(
        &self,
        object_name: &str,
        start: usize,
    ) -> Result<ByteStream> {
        let mut reader = self.op
                    .reader_with(object_name)
                    .range(start as u64..)
                    .await?;
        // Seek to the beginning of the object
    reader.seek(0).await?;

    let mut buffer = vec![0u8; 1024];
    let mut chunk = Vec::new();
    let mut total_bytes_read = 0;

    // Read the object and convert it into chunks
    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        chunk.extend_from_slice(&buffer[..bytes_read]);
        total_bytes_read += bytes_read;

        if total_bytes_read >= 1024 {
            // Create a new StreamChunk and emit it
            let stream_chunk = StreamChunk::new(chunk);
            yield stream_chunk;

            // Reset the chunk and byte counter
            chunk = Vec::new();
            total_bytes_read = 0;
        }
    }

    // If there are remaining bytes in the last chunk, emit it
    if !chunk.is_empty() {
        let stream_chunk = StreamChunk::new(chunk);
        yield stream_chunk;
    }
    }
}