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

use anyhow::anyhow;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use google_cloud_pubsub::client::Client;
use google_cloud_pubsub::subscription::SeekTo;
use serde::{Deserialize, Serialize};

use super::PubsubProperties;
use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PubsubSplit {
    pub(crate) index: u32,
    pub(crate) subscription: String,
    pub(crate) properties: PubsubProperties,
}

impl PubsubSplit {
    // ! impl_split expects a non-async Self return, but we need async here to really seek back
    pub async fn copy_with_offset(&self, start_offset: String) -> Self {
        // TODO: tracing, no panic?
        self.properties.initialize_env();
        let client = Client::default().await.unwrap();
        let subscription = client.subscription(self.properties.subscription.as_str());

        let nanos = i64::from_str_radix(start_offset.as_str(), 10).unwrap();
        let timestamp = Utc.timestamp_nanos(nanos);

        subscription
            .seek(SeekTo::Timestamp(timestamp.into()), None, None)
            .await
            .unwrap();
        self.clone()
    }
}

impl SplitMetaData for PubsubSplit {
    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }

    fn id(&self) -> SplitId {
        self.index.to_string().into()
    }
}
