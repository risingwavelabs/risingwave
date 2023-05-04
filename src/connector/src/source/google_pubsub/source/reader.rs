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

use anyhow::{anyhow, ensure, Context, Result};
use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::{SeekTo, Subscription};
use risingwave_common::bail;
use tonic::Code;

use super::TaggedReceivedMessage;
use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::google_pubsub::PubsubProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitId, SplitImpl,
    SplitMetaData, SplitReader,
};

const PUBSUB_MAX_FETCH_MESSAGES: usize = 1024;

impl_common_split_reader_logic!(PubsubSplitReader, PubsubProperties);

pub struct PubsubSplitReader {
    subscription: Subscription,
    stop_offset: Option<NaiveDateTime>,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

impl PubsubSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
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

            let latest_offset: NaiveDateTime = raw_chunk
                .last()
                .map(|m| m.message.publish_time.clone().unwrap_or_default())
                .map(|t| {
                    let mut t = t;
                    t.normalize();
                    NaiveDateTime::from_timestamp_opt(t.seconds, t.nanos as u32).unwrap_or_default()
                })
                .unwrap_or_default();

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

            // Stop if we've approached the stop_offset
            if let Some(stop_offset) = self.stop_offset && latest_offset >= stop_offset {
                return Ok(());
            }
        }
    }
}

#[async_trait]
impl SplitReader for PubsubSplitReader {
    type Properties = PubsubProperties;

    async fn new(
        properties: PubsubProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        ensure!(
            splits.len() == 1,
            "the pubsub reader only supports a single split"
        );
        let split = splits
            .into_iter()
            .next()
            .unwrap()
            .into_google_pubsub()
            .unwrap();

        // Set environment variables consumed by `google_cloud_pubsub`
        properties.initialize_env();

        let client = Client::new(Default::default())
            .await
            .map_err(|e| anyhow!(e))?;
        let subscription = client.subscription(&properties.subscription);

        if let Some(ref offset) = split.start_offset {
            let timestamp = offset
                .as_str()
                .parse::<i64>()
                .map(|nanos| Utc.timestamp_nanos(nanos))
                .map_err(|e| anyhow!("error parsing offset: {:?}", e))?;

            subscription
                .seek(SeekTo::Timestamp(timestamp.into()), None)
                .await
                .map_err(|e| anyhow!("error seeking to pubsub offset: {:?}", e))?;
        }

        let stop_offset = if let Some(ref offset) = split.stop_offset {
            Some(
                offset
                    .as_str()
                    .parse::<i64>()
                    .map_err(|e| anyhow!(e))
                    .map(|nanos| NaiveDateTime::from_timestamp_opt(nanos, 0).unwrap_or_default())?,
            )
        } else {
            None
        };

        Ok(Self {
            subscription,
            split_id: split.id(),
            stop_offset,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}
