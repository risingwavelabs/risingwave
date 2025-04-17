// Copyright 2025 RisingWave Labs
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
use crate::source::pulsar::PulsarEnumeratorOffset;
use crate::source::pulsar::topic::Topic;
use crate::source::{SplitId, SplitMetaData};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct PulsarSplit {
    pub(crate) topic: Topic,
    pub(crate) start_offset: PulsarEnumeratorOffset,
}

impl SplitMetaData for PulsarSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        self.topic.to_string().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        let start_offset = if last_seen_offset.is_empty() {
            PulsarEnumeratorOffset::Earliest
        } else {
            PulsarEnumeratorOffset::MessageId(last_seen_offset)
        };

        self.start_offset = start_offset;
        Ok(())
    }
}
