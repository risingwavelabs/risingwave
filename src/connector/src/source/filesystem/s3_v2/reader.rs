use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_s3::client as s3_client;
use futures_async_stream::try_stream;
use risingwave_common::error::RwError;

use crate::aws_auth::AwsAuthProps;
use crate::aws_utils::{s3_client, default_conn_config};
use crate::parser::{ParserConfig, ByteStreamSourceParserImpl};
use crate::source::{BoxSourceWithStateStream, StreamChunkWithState};
use crate::source::base::SplitMetaData;
use crate::source::filesystem::{FsSplit, S3FileReader, nd_streaming};
use crate::source::{SourceReader, filesystem::S3Properties, SourceContextRef, Column};

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
        let data_stream = S3FileReader::stream_read_object(client_for_s3, bucket_name, split, source_ctx.clone());
        let parser = ByteStreamSourceParserImpl::create(parser_config.clone(), source_ctx.clone()).await?;
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
impl SourceReader for S3SourceReader {
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
            split
        )
    }
}