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

use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use futures_async_stream::try_stream;
use risingwave_common::error::RwError;

use crate::aws_auth::AwsAuthProps;
use crate::aws_utils::{default_conn_config, s3_client};
use crate::parser::{ByteStreamSourceParserImpl, ParserConfig};
use crate::source::base::SplitMetaData;
use crate::source::filesystem::{nd_streaming, FsSplit, S3FileReader, S3Properties};
use crate::source::{
    BoxSourceWithStateStream, Column, FsFileReader, SourceContextRef, StreamChunkWithState,
};

pub struct S3SourceReader {
    bucket_name: String,
    s3_client: s3_client::Client,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl S3SourceReader {
    #[try_stream(boxed, ok = StreamChunkWithState, error = RwError)]
    async fn build_read_stream_inner(
        client_for_s3: s3_client::Client,
        bucket_name: String,
        source_ctx: SourceContextRef,
        parser_config: ParserConfig,
        split: FsSplit,
    ) {
        let split_id = split.id();
        let data_stream =
            S3FileReader::stream_read_object(client_for_s3, bucket_name, split, source_ctx.clone());
        let parser =
            ByteStreamSourceParserImpl::create(parser_config.clone(), source_ctx.clone()).await?;
        let msg_stream = if matches!(
            parser,
            ByteStreamSourceParserImpl::Json(_) | ByteStreamSourceParserImpl::Csv(_)
        ) {
            parser.into_stream(nd_streaming::split_stream(data_stream))
        } else {
            parser.into_stream(data_stream)
        };

        let actor_id = source_ctx.source_info.actor_id.to_string();
        let source_id = source_ctx.source_info.source_id.to_string();
        #[for_await]
        for msg in msg_stream {
            let msg = msg?;
            source_ctx
                .metrics
                .partition_input_count
                .with_label_values(&[&actor_id, &source_id, &split_id])
                .inc_by(msg.chunk.cardinality() as u64);
            yield msg;
        }
    }
}

#[async_trait]
impl FsFileReader for S3SourceReader {
    type Properties = S3Properties;

    async fn new(
        properties: Self::Properties,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let config = AwsAuthProps::from(&properties);

        let sdk_config = config.build_config().await?;

        let bucket_name = properties.bucket_name;
        let s3_client = s3_client(&sdk_config, Some(default_conn_config()));

        Ok(S3SourceReader {
            bucket_name,
            s3_client,
            parser_config,
            source_ctx,
        })
    }

    fn build_read_stream(&mut self, split: FsSplit) -> BoxSourceWithStateStream {
        Self::build_read_stream_inner(
            self.s3_client.clone(),
            self.bucket_name.clone(),
            self.source_ctx.clone(),
            self.parser_config.clone(),
            split,
        )
    }
}
