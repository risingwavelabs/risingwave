// Copyright 2024 RisingWave Labs
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

/// See <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_StartingPosition.html> for more details.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum KinesisOffset {
    /// Corresponds to `TRIM_HORIZON`. Points the oldest record in the shard.
    Earliest,
    /// Corresponds to `LATEST`. Points to the (still-nonexisting) record just after the most recent one in the shard.
    Latest,
    /// Corresponds to `AFTER_SEQUENCE_NUMBER`. Points the record just after the one with the given sequence number.
    #[serde(alias = "SequenceNumber")] // for backward compatibility
    AfterSequenceNumber(String),
    /// Corresponds to `AT_TIMESTAMP`. Points to the (first) record right at or after the given timestamp.
    Timestamp(i64),

    None,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Hash)]
pub struct KinesisSplit {
    pub(crate) shard_id: SplitId,

    #[serde(alias = "start_position")] // for backward compatibility
    pub(crate) next_offset: KinesisOffset,
    #[serde(alias = "end_position")] // for backward compatibility
    pub(crate) end_offset: KinesisOffset,
}

impl SplitMetaData for KinesisSplit {
    fn id(&self) -> SplitId {
        self.shard_id.clone()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.next_offset = KinesisOffset::AfterSequenceNumber(last_seen_offset);
        Ok(())
    }
}

impl KinesisSplit {
    pub fn new(
        shard_id: SplitId,
        next_offset: KinesisOffset,
        end_offset: KinesisOffset,
    ) -> KinesisSplit {
        KinesisSplit {
            shard_id,
            next_offset,
            end_offset,
        }
    }
}
