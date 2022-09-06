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
use serde::{Deserialize, Serialize};

use crate::source::pulsar::topic::Topic;
use crate::source::pulsar::PulsarEnumeratorOffset;
use crate::source::{SplitId, SplitMetaData};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
pub struct PulsarSplit {
    pub(crate) topic: Topic,
    pub(crate) start_offset: PulsarEnumeratorOffset,
}

impl PulsarSplit {
    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        let start_offset = if start_offset.is_empty() {
            PulsarEnumeratorOffset::Earliest
        } else {
            PulsarEnumeratorOffset::MessageId(start_offset)
        };
        Self {
            topic: self.topic.clone(),
            start_offset,
        }
    }
}

impl SplitMetaData for PulsarSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        self.topic.to_string().into()
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}
