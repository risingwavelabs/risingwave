use std::collections::HashMap;

use async_trait::async_trait;

use super::super::PubsubProperties as GooglePubsubProperties;
use crate::source::base::SplitEnumerator;
use crate::source::google_pubsub::split::PubsubSplit;

pub struct GooglePubsubSplitEnumerator {
    topic: String,

    // subscription to pull things in from, but shouldn't this be also be autogenerateable?
    subscription: String,

    // Has to be static at first -- then we expose something to change the split degree (perhaps
    // only upwards)
    split_count: u32,

    // To use a pubsub emulator as the source
    emulator_host: Option<String>,

    // Optionally filter messages by attributes -- again not sure if this belongs here.
    attributes: Option<HashMap<String, String>>,
}

impl GooglePubsubSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for GooglePubsubSplitEnumerator {
    type Properties = GooglePubsubProperties;
    type Split = PubsubSplit;

    async fn new(properties: Self::Properties) -> anyhow::Result<GooglePubsubSplitEnumerator> {
        let topic = properties.topic;
        let split_count = properties.split_count;
        let subscription = properties.subscription;
        let attributes = properties.attributes;
        let emulator_host = properties.emulator_host;

        Ok(Self {
            topic,
            split_count,
            subscription,
            attributes,
            emulator_host,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<PubsubSplit>> {
        let splits: Vec<PubsubSplit> = (0..self.split_count)
            .map(|i| PubsubSplit {
                index: i,
                topic: self.topic.clone(),
                subscription: self.subscription.clone(),
            })
            .collect();

        Ok(splits)
    }
}
