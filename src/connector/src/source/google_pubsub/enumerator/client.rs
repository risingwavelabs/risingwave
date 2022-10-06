use async_trait::async_trait;

use super::super::PubsubProperties as GooglePubsubProperties;
use crate::source::base::SplitEnumerator;
use crate::source::google_pubsub::split::PubsubSplit;

pub struct GooglePubsubSplitEnumerator {
    // subscription to pull things in from, but shouldn't this be also be autogenerateable?
    subscription: String,

    // Has to be static at first -- then we expose something to change the split degree (perhaps
    // only upwards)
    split_count: u32,

    // To use a pubsub emulator as the source
    emulator_host: Option<String>,
}

impl GooglePubsubSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for GooglePubsubSplitEnumerator {
    type Properties = GooglePubsubProperties;
    type Split = PubsubSplit;

    async fn new(properties: Self::Properties) -> anyhow::Result<GooglePubsubSplitEnumerator> {
        let split_count = properties.split_count;
        let subscription = properties.subscription;
        let emulator_host = properties.emulator_host;

        Ok(Self {
            split_count,
            subscription,
            emulator_host,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<PubsubSplit>> {
        let splits: Vec<PubsubSplit> = (0..self.split_count)
            .map(|i| PubsubSplit {
                index: i,
                subscription: self.subscription.clone(),
            })
            .collect();

        Ok(splits)
    }
}
