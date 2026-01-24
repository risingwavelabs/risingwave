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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PubsubSplit {
    // XXX: `index` and `subscription` seems also not useful. It's only for `SplitMetaData::id`.
    // Is the split id useful?
    pub(crate) index: u32,
    pub(crate) subscription: String,

    #[serde(rename = "start_offset")]
    #[serde(skip_serializing)]
    pub(crate) __deprecated_start_offset: Option<String>,

    #[serde(rename = "stop_offset")]
    #[serde(skip_serializing)]
    pub(crate) __deprecated_stop_offset: Option<String>,
}

impl SplitMetaData for PubsubSplit {
    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn id(&self) -> SplitId {
        format!("{}-{}", self.subscription, self.index).into()
    }

    /// No-op. Actually `PubsubSplit` doesn't maintain any state. It's fully managed by Pubsub.
    /// One subscription is like one Kafka consumer group.
    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // forcefully set previously persisted start_offset to None
        self.__deprecated_start_offset = None;
        Ok(())
    }
}
