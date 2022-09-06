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

use crate::source::{SplitId, SplitMetaData};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum KinesisOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(i64),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub struct KinesisSplit {
    pub(crate) shard_id: SplitId,
    pub(crate) start_position: KinesisOffset,
    pub(crate) end_position: KinesisOffset,
}

impl SplitMetaData for KinesisSplit {
    fn id(&self) -> SplitId {
        self.shard_id.clone()
    }

    fn encode_to_bytes(&self) -> Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

impl KinesisSplit {
    pub fn new(
        shard_id: SplitId,
        start_position: KinesisOffset,
        end_position: KinesisOffset,
    ) -> KinesisSplit {
        KinesisSplit {
            shard_id,
            start_position,
            end_position,
        }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        let start_offset = if start_offset.is_empty() {
            KinesisOffset::Earliest
        } else {
            KinesisOffset::SequenceNumber(start_offset)
        };
        Self::new(
            self.shard_id.clone(),
            start_offset,
            self.end_position.clone(),
        )
    }
}
