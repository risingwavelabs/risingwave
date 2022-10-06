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

use std::error::Error;

use anyhow::{anyhow, Ok, Result};
use async_trait::async_trait;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::Subscription;

use super::TaggedReceivedMessage;
use crate::source::google_pubsub::PubsubProperties;
use crate::source::{Column, ConnectorState, SourceMessage, SplitReader};

const PUBSUB_MAX_FETCH_MESSAGES: usize = 1024;

pub struct PubsubSplitReader {
    subscription: Subscription,
    split_id: u32,
}

impl PubsubSplitReader {}

#[async_trait]
impl SplitReader for PubsubSplitReader {
    type Properties = PubsubProperties;

    async fn new(
        properties: PubsubProperties,
        _state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        // set emulator host
        if let Some(emulator_host) = properties.emulator_host {
            std::env::set_var("PUBSUB_EMULATOR_HOST", emulator_host);
        }

        // TODO: Set credentials
        // Per changes in the `google-cloud-rust` crate, for authentication credentials are set
        // in the GOOGLE_CLOUD_CREDENTIALS_JSON environment variable.

        let client = Client::default().await.map_err(|e| anyhow!(e))?;
        let subscription = client.subscription(&properties.subscription);

        // TODO: tag with split_id from ConnectorState
        Ok(Self {
            subscription,
            split_id: 0 as u32,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let next_batch = self
            .subscription
            .pull(PUBSUB_MAX_FETCH_MESSAGES as i32, None, None)
            .await
            .map_err(to_anyhow)?;

        // TODO: remove check -- irrelevant since next_batch will always have 1+ message
        // (or does it have a timeout? needs to be verified)
        if next_batch.is_empty() {
            return Ok(None);
        }

        let ack_ids: Vec<String> = next_batch.iter().map(|m| m.ack_id().into()).collect();

        // ? is this the right way to handle an ack failure
        self.subscription.ack(ack_ids).await.map_err(to_anyhow)?;

        let source_message_batch: Vec<SourceMessage> = next_batch
            .into_iter()
            .map(|rm| TaggedReceivedMessage(self.split_id.to_string(), rm).into())
            .collect();

        Ok(Some(source_message_batch))
    }
}

fn to_anyhow<T>(e: T) -> anyhow::Error
where
    T: Error,
    T: Into<anyhow::Error>,
    T: std::marker::Send,
    T: std::marker::Sync,
{
    anyhow!(e)
}
