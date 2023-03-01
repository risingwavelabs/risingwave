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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_smithy_http::byte_stream::ByteStream;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use io::StreamReader;
use risingwave_common::error::RwError;
use tokio::io::BufReader;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use crate::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig};
use crate::source::base::{SplitMetaData, SplitReader, MAX_CHUNK_SIZE};
use crate::source::filesystem::file_common::FsSplit;
use crate::source::filesystem::s3::S3Properties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitImpl,
    StreamChunkWithState,
};
const MAX_CHANNEL_BUFFER_SIZE: usize = 2048;
const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug)]
pub struct S3FileReader {
    split_offset: HashMap<String, u64>,
    bucket_name: String,
    s3_client: s3_client::Client,
    splits: Vec<FsSplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl S3FileReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    async fn stream_read_object(
        client_for_s3: s3_client::Client,
        bucket_name: String,
        split: FsSplit,
        source_ctx: SourceContextRef,
    ) {
        let actor_id = source_ctx.source_info.actor_id.to_string();
        let source_id = source_ctx.source_info.source_id.to_string();
        let split_id = split.id();

        let object_name = split.name.clone();
        let byte_stream =
            S3FileReader::get_object(&client_for_s3, &bucket_name, &object_name, split.offset)
                .await?;
        let stream_reader = StreamReader::new(
            byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        let reader = Box::pin(BufReader::new(stream_reader));

        let stream = ReaderStream::with_capacity(reader, STREAM_READER_CAPACITY);

        let mut offset: usize = split.offset;
        let mut batch_size: usize = 0;
        let mut batch = Vec::new();
        #[for_await]
        for read in stream {
            let bytes = read?;
            let len = bytes.len();
            let msg = SourceMessage {
                payload: Some(bytes),
                offset: offset.to_string(),
                split_id: split.id(),
                meta: SourceMeta::Empty,
            };
            offset += len;
            batch_size += len;
            batch.push(msg);
            if batch.len() >= MAX_CHUNK_SIZE {
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

    async fn get_object(
        client_for_s3: &s3_client::Client,
        bucket_name: &str,
        object_name: &str,
        start: usize,
    ) -> anyhow::Result<ByteStream> {
        let range = if start == 0 {
            None
        } else {
            Some(format!("bytes={}-", start))
        };
        // TODO. set_range
        let obj = client_for_s3
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .set_range(range)
            .send()
            .await
            .map_err(|sdk_err| {
                anyhow!(
                    "S3 GetObject from {} error: {}",
                    bucket_name,
                    sdk_err.to_string()
                )
            })?
            .body;
        Ok(obj)
    }
}

#[async_trait]
impl SplitReader for S3FileReader {
    type Properties = S3Properties;

    async fn new(
        props: S3Properties,
        state: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let config = AwsConfigV2::from(HashMap::from(props.clone()));
        let sdk_config = config.load_config(None).await;

        let bucket_name = props.bucket_name;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));

        let splits = state
            .into_iter()
            .map(|split| split.into_fs().expect("not a fs split"))
            .collect();
        let s3_file_reader = S3FileReader {
            split_offset: HashMap::new(),
            bucket_name,
            s3_client,
            splits,
            parser_config,
            source_ctx,
        };

        Ok(s3_file_reader)
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl S3FileReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    async fn into_chunk_stream(self) {
        for split in self.splits {
            let actor_id = self.source_ctx.source_info.actor_id.to_string();
            let source_id = self.source_ctx.source_info.source_id.to_string();
            let source_ctx = self.source_ctx.clone();

            let split_id = split.id();

            let data_stream = Self::stream_read_object(
                self.s3_client.clone(),
                self.bucket_name.clone(),
                split,
                self.source_ctx.clone(),
            );

            let parser =
                ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx)?;
            let msg_stream = parser.into_stream(Box::pin(data_stream));
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
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::parser::{CommonParserConfig, CsvParserConfig, SpecificParserConfig};
    use crate::source::filesystem::{S3Properties, S3SplitEnumerator};
    use crate::source::{SourceColumnDesc, SplitEnumerator};

    #[tokio::test]
    #[ignore]
    async fn test_s3_split_reader() {
        let props = S3Properties {
            region_name: "ap-southeast-1".to_owned(),
            bucket_name: "mingchao-s3-source".to_owned(),
            match_pattern: None,
            access: None,
            secret: None,
            endpoint_url: None,
        };
        let mut enumerator = S3SplitEnumerator::new(props.clone()).await.unwrap();
        let splits = enumerator.list_splits().await.unwrap();
        println!("splits {:?}", splits);

        let splits = splits.into_iter().map(SplitImpl::S3).collect();

        let descs = vec![
            SourceColumnDesc::simple("id", DataType::Int64, 1.into()),
            SourceColumnDesc::simple("name", DataType::Varchar, 2.into()),
            SourceColumnDesc::simple("age", DataType::Int32, 3.into()),
        ];

        let csv_config = CsvParserConfig {
            delimiter: b',',
            has_header: true,
        };

        let config = ParserConfig {
            common: CommonParserConfig { rw_columns: descs },
            specific: SpecificParserConfig::Csv(csv_config),
        };

        let reader = S3FileReader::new(props, splits, config, Default::default(), None)
            .await
            .unwrap();

        let msg_stream = reader.into_chunk_stream();
        #[for_await]
        for msg in msg_stream {
            println!("msg {:?}", msg);
        }
    }
}
