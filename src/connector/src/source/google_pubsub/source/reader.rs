use std::error::Error;

use anyhow::{anyhow, Ok, Result};
use async_trait::async_trait;
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::{Subscription, SubscriptionConfig};

use crate::source::google_pubsub::PubsubProperties;
use crate::source::{Column, ConnectorState, SourceMessage, SplitReader};

const PUBSUB_MAX_FETCH_MESSAGES: usize = 1024;

pub struct PubsubSplitReader {
    subscription: Subscription,
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
        let client = Client::default().await.map_err(|e| anyhow!(e))?;


        let client = Client::default().await.map_err(|e| anyhow!(e))?;
        let subscription = client.subscription(&properties.subscription);

        Ok(Self { subscription })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let next_batch = self
            .subscription
            .pull(PUBSUB_MAX_FETCH_MESSAGES as i32, None, None)
            .await
            .map_err(to_anyhow)?;

        if next_batch.is_empty() {
            return Ok(None);
        }

        let ack_ids: Vec<String> = next_batch.iter().map(|m| m.ack_id().into()).collect();

        // ? is this the right way to handle an ack failure
        self.subscription.ack(ack_ids).await.map_err(to_anyhow)?;

        let source_message_batch: Vec<SourceMessage> =
            next_batch.into_iter().map(|rm| rm.into()).collect();

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
