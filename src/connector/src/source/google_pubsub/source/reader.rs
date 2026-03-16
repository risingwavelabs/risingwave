// Copyright 2022 RisingWave Labs
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
use futures::FutureExt;
use futures::StreamExt;
use futures_async_stream::try_stream;
use google_cloud_pubsub::subscriber::SubscriberConfig;
use google_cloud_pubsub::subscription::{SubscribeConfig, Subscription};
use risingwave_common::{bail, ensure};

use super::TaggedReceivedMessage;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::google_pubsub::{PubsubProperties, PubsubSplit};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

const DEFAULT_ACK_DEADLINE_SECONDS: i32 = 60;
const MAX_BATCH_SIZE: usize = 1024;

pub struct PubsubSplitReader {
    subscription: Subscription,
    ack_deadline_seconds: i32,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl PubsubSplitReader {
    fn build_subscribe_config(&self) -> SubscribeConfig {
        let subscriber_config = SubscriberConfig {
            stream_ack_deadline_seconds: self.ack_deadline_seconds,
            ..Default::default()
        };
        SubscribeConfig::default().with_subscriber_config(subscriber_config)
    }

    #[try_stream(ok = Vec<SourceMessage>, error = ConnectorError)]
    async fn into_data_stream(self) {
        loop {
            let subscribe_config = self.build_subscribe_config();
            let mut stream = self
                .subscription
                .subscribe(Some(subscribe_config))
                .await
                .map_err(|e| {
                    ConnectorError::from(anyhow::anyhow!("failed to subscribe: {}", e))
                })?;

            while let Some(first) = stream.next().await {
                let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
                batch.push(SourceMessage::from(TaggedReceivedMessage(
                    self.split_id.clone(),
                    first,
                )));

                // Drain remaining ready messages without blocking
                while batch.len() < MAX_BATCH_SIZE {
                    match stream.next().now_or_never() {
                        Some(Some(msg)) => {
                            batch.push(SourceMessage::from(TaggedReceivedMessage(
                                self.split_id.clone(),
                                msg,
                            )));
                        }
                        _ => break,
                    }
                }

                yield batch;
            }

            // Stream ended (all subscribers stopped). Reconnect.
            tracing::warn!("pubsub streaming pull ended, reconnecting...");
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

        let ack_deadline_seconds = properties
            .ack_deadline_seconds
            .unwrap_or(DEFAULT_ACK_DEADLINE_SECONDS);

        if !(10..=600).contains(&ack_deadline_seconds) {
            bail!("pubsub.ack_deadline_seconds must be between 10 and 600");
        }

        let subscription = properties.subscription_client().await?;

        Ok(Self {
            subscription,
            ack_deadline_seconds,
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
