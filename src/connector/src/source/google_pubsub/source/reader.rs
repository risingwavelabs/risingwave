// Copyright 2025 RisingWave Labs
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

use async_trait::async_trait;
use futures_async_stream::try_stream;
use google_cloud_pubsub::subscription::Subscription;
use risingwave_common::{bail, ensure};
use tonic::Code;

use super::TaggedReceivedMessage;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::google_pubsub::{PubsubProperties, PubsubSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

const PUBSUB_MAX_FETCH_MESSAGES: usize = 1024;

pub struct PubsubSplitReader {
    subscription: Subscription,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl PubsubSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        loop {
            let pull_result = self
                .subscription
                .pull(PUBSUB_MAX_FETCH_MESSAGES as i32, None)
                .await;

            let raw_chunk = match pull_result {
                Ok(chunk) => chunk,
                Err(e) => match e.code() {
                    Code::NotFound => bail!("subscription not found"),
                    Code::PermissionDenied => bail!("not authorized to access subscription"),
                    _ => continue,
                },
            };

            // Sleep if we get an empty batch -- this should generally not happen
            // since subscription.pull claims to block until at least a single message is available.
            // But pull seems to time out at some point a return with no messages, so we need to see
            // ? if that's somehow adjustable or we can skip sleeping and hand it off to pull again
            if raw_chunk.is_empty() {
                continue;
            }

            let mut chunk: Vec<SourceMessage> = Vec::with_capacity(raw_chunk.len());

            for message in raw_chunk {
                chunk.push(SourceMessage::from(TaggedReceivedMessage(
                    self.split_id.clone(),
                    message,
                )));
            }

            yield chunk;
        }
    }
}

#[async_trait]
impl SplitReader for PubsubSplitReader {
    type Properties = PubsubProperties;
    type Split = PubsubSplit;

    async fn new(
        properties: PubsubProperties,
        splits: Vec<PubsubSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(
            splits.len() == 1,
            "the pubsub reader only supports a single split"
        );
        let split = splits.into_iter().next().unwrap();

        let subscription = properties.subscription_client().await?;

        Ok(Self {
            subscription,
            split_id: split.id(),
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}
