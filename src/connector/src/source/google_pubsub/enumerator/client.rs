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

use async_trait::async_trait;

use super::super::PubsubProperties as GooglePubsubProperties;
use crate::source::base::SplitEnumerator;
use crate::source::google_pubsub::split::PubsubSplit;

pub struct PubsubSplitEnumerator {
    // subscription to pull things in from, but shouldn't this be also be autogenerateable?
    subscription: String,

    // Has to be static at first -- then we expose something to change the split degree (perhaps
    // only upwards)
    split_count: u32,

    // To use a pubsub emulator as the source
    emulator_host: Option<String>,
}

impl PubsubSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for PubsubSplitEnumerator {
    type Properties = GooglePubsubProperties;
    type Split = PubsubSplit;

    async fn new(properties: Self::Properties) -> anyhow::Result<PubsubSplitEnumerator> {
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
            .map(|i| PubsubSplit { index: i })
            .collect();

        Ok(splits)
    }
}
