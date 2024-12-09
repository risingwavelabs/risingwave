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

use std::collections::HashMap;
use std::pin::pin;

use anyhow::anyhow;
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_smithy_http::futures_stream_adapter::FuturesStreamCompatByteStream;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::ByteStream;
use futures_async_stream::try_stream;
use io::StreamReader;
use risingwave_common::array::StreamChunk;
use tokio::io::BufReader;
use tokio_util::io;
use tokio_util::io::ReaderStream;

use super::split_stream::split_stream;
use crate::aws_utils::{default_conn_config, s3_client};
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::base::{SplitMetaData, SplitReader};
use crate::source::filesystem::file_common::FsSplit;
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::s3::S3Properties;
use crate::source::{
    into_chunk_stream, BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SourceMeta,
};

const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug)]
pub struct S3FileReader {
    #[expect(dead_code)]
    split_offset: HashMap<String, u64>,
    bucket_name: String,
    s3_client: s3_client::Client,
    splits: Vec<FsSplit>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl S3FileReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_object(
        client_for_s3: s3_client::Client,
        bucket_name: String,
        split: FsSplit,
        source_ctx: SourceContextRef,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();

        let object_name = split.name.clone();

        let byte_stream = match S3FileReader::get_object(
            &client_for_s3,
            &bucket_name,
            &object_name,
            split.offset,
        )
        .await
        .map_err(|sdk_err| sdk_err.into_service_error())
        {
            Ok(s) => s,
            Err(GetObjectError::NoSuchKey(_)) => {
                tracing::warn!("S3 Object {} not found, ignoring", object_name);
                return Ok(());
            }
            Err(e) => {
                return Err(anyhow!(e)
                    .context(format!("S3 GetObject from {bucket_name} error"))
                    .into());
            }
        };

        // FYI: https://github.com/awslabs/smithy-rs/pull/2983
        let byte_stream = FuturesStreamCompatByteStream::new(byte_stream);

        let stream_reader = StreamReader::new(byte_stream);

        let reader = pin!(BufReader::new(stream_reader));

        let stream = ReaderStream::with_capacity(reader, STREAM_READER_CAPACITY);

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

    pub async fn get_object(
        client_for_s3: &s3_client::Client,
        bucket_name: &str,
        object_name: &str,
        start: usize,
    ) -> std::result::Result<
        ByteStream,
        SdkError<GetObjectError, aws_smithy_runtime_api::http::Response<SdkBody>>,
    > {
        let range = if start == 0 {
            None
        } else {
            Some(format!("bytes={}-", start))
        };
        // TODO. set_range

        client_for_s3
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .set_range(range)
            .send()
            .await
            .map(|r| r.body)
    }
}

#[async_trait]
impl SplitReader for S3FileReader {
    type Properties = S3Properties;
    type Split = FsSplit;

    async fn new(
        props: S3Properties,
        splits: Vec<FsSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let config = AwsAuthProps::from(&props);

        let sdk_config = config.build_config().await?;

        let bucket_name = props.common.bucket_name;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));

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

    fn into_stream(self) -> BoxChunkSourceStream {
        self.into_stream_inner()
    }
}

impl S3FileReader {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in self.splits {
            let data_stream = Self::stream_read_object(
                self.s3_client.clone(),
                self.bucket_name.clone(),
                split,
                self.source_ctx.clone(),
            );
            let data_stream = if need_nd_streaming(&self.parser_config.specific.encoding_config) {
                split_stream(data_stream)
            } else {
                data_stream
            };

            let msg_stream = into_chunk_stream(
                data_stream,
                self.parser_config.clone(),
                self.source_ctx.clone(),
            );
            #[for_await]
            for msg in msg_stream {
                let msg = msg?;
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
    use crate::parser::{
        CommonParserConfig, CsvProperties, EncodingProperties, ProtocolProperties,
        SpecificParserConfig,
    };
    use crate::source::filesystem::file_common::CompressionFormat;
    use crate::source::filesystem::s3::S3PropertiesCommon;
    use crate::source::filesystem::S3SplitEnumerator;
    use crate::source::{
        SourceColumnDesc, SourceContext, SourceEnumeratorContext, SplitEnumerator,
    };

    #[tokio::test]
    #[ignore]
    async fn test_s3_split_reader() {
        let props: S3Properties = S3PropertiesCommon {
            region_name: "ap-southeast-1".to_owned(),
            bucket_name: "mingchao-s3-source".to_owned(),
            match_pattern: None,
            access: None,
            secret: None,
            endpoint_url: None,
            compression_format: CompressionFormat::None,
        }
        .into();
        let mut enumerator =
            S3SplitEnumerator::new(props.clone(), SourceEnumeratorContext::dummy().into())
                .await
                .unwrap();
        let splits = enumerator.list_splits().await.unwrap();
        println!("splits {:?}", splits);

        let descs = vec![
            SourceColumnDesc::simple("id", DataType::Int64, 1.into()),
            SourceColumnDesc::simple("name", DataType::Varchar, 2.into()),
            SourceColumnDesc::simple("age", DataType::Int32, 3.into()),
        ];

        let csv_config = CsvProperties {
            delimiter: b',',
            has_header: true,
        };

        let config = ParserConfig {
            common: CommonParserConfig { rw_columns: descs },
            specific: SpecificParserConfig {
                encoding_config: EncodingProperties::Csv(csv_config),
                protocol_config: ProtocolProperties::Plain,
            },
        };

        let reader = S3FileReader::new(props, splits, config, SourceContext::dummy().into(), None)
            .await
            .unwrap();

        let msg_stream = reader.into_stream_inner();
        #[for_await]
        for msg in msg_stream {
            println!("msg {:?}", msg);
        }
    }
}
