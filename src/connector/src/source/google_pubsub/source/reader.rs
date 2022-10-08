// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Ok, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures_async_stream::try_stream;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::{SeekTo, Subscription};
use risingwave_common::try_match_expand;

use super::TaggedReceivedMessage;
use crate::source::google_pubsub::PubsubProperties;
use crate::source::{
    BoxSourceStream, Column, ConnectorState, SourceMessage, SplitId, SplitImpl, SplitReader,
};

const PUBSUB_MAX_FETCH_MESSAGES: usize = 1024;

pub struct PubsubSplitReader {
    subscription: Subscription,
    split_id: SplitId,
}

impl PubsubSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(self) {
        loop {
            let raw_chunk = self
                .subscription
                .pull(PUBSUB_MAX_FETCH_MESSAGES as i32, None, None)
                .await?;

            // TODO: handle errors conditionally

            // Sleep if we get an empty batch -- this should generally not happen
            // since subscription.pull claims to block until at least a single message is available.
            // But pull seems to time out at some point a return with no messages, so we need to see
            // ? if that's somehow adjustable or we can skip sleeping and hand it off to pull again
            if raw_chunk.is_empty() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }

            let mut chunk: Vec<SourceMessage> = Vec::with_capacity(raw_chunk.len());
            let mut ack_ids: Vec<String> = Vec::with_capacity(raw_chunk.len());

            for message in raw_chunk {
                ack_ids.push(message.ack_id().into());
                chunk.push(SourceMessage::from(TaggedReceivedMessage(
                    self.split_id.clone(),
                    message,
                )));
            }

            self.subscription
                .ack(ack_ids)
                .await
                .map_err(|e| anyhow!(e))
                .context("failed to ack pubsub messages")?;

            yield chunk;
        }
    }
}

#[async_trait]
impl SplitReader for PubsubSplitReader {
    type Properties = PubsubProperties;

    async fn new(
        properties: PubsubProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let splits = state.ok_or_else(|| anyhow!("no default state for reader"))?;
        ensure!(
            splits.len() == 1,
            "the pubsub reader only supports a single split"
        );
        let split = try_match_expand!(splits.into_iter().next().unwrap(), SplitImpl::GooglePubsub)
            .map_err(|e| anyhow!(e))?;

        // Set environment variables consumed by `google_cloud_pubsub`
        properties.initialize_env();

        let client = Client::default().await.map_err(|e| anyhow!(e))?;
        let subscription = client.subscription(&properties.subscription);

        if let Some(offset) = split.start_offset {
            let timestamp = offset
                .as_str()
                .parse::<i64>()
                .map(|nanos| Utc.timestamp_nanos(nanos))
                .map_err(|e| anyhow!("error parsing offset: {:?}", e))?;

            subscription
                .seek(SeekTo::Timestamp(timestamp.into()), None, None)
                .await
                .map_err(|e| anyhow!("error seeking to pubsub offset: {:?}", e))?;
        }

        Ok(Self {
            subscription,
            split_id: split.index.to_string().into(),
        })
    }

    fn into_stream(self) -> BoxSourceStream {
        self.into_stream()
    }

    // async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
    //     let next_batch = self
    //         .subscription
    //         .pull(PUBSUB_MAX_FETCH_MESSAGES as i32, None, None)
    //         .await
    //         .map_err(|e| anyhow!(e))
    //         .context("failed to pull messages from pubsub")?;

    //     // TODO: remove check -- irrelevant since next_batch will always have 1+ message
    //     // (or does it have a timeout? needs to be verified)
    //     if next_batch.is_empty() {
    //         return Ok(None);
    //     }

    //     let ack_ids: Vec<String> = next_batch.iter().map(|m| m.ack_id().into()).collect();

    //     // ? is this the right way to handle an ack failure
    //     self.subscription
    //         .ack(ack_ids)
    //         .await
    //         .map_err(|e| anyhow!(e))
    //         .context("failed to ack messages to pubsub")?;

    //     let source_message_batch: Vec<SourceMessage> = next_batch
    //         .into_iter()
    //         .map(|rm| TaggedReceivedMessage(self.split_id.to_string(), rm).into())
    //         .collect();

    //     Ok(Some(source_message_batch))
    // }
}
