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
use serde::{Deserialize, Serialize};

use crate::base::SourceSplit;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum KinesisOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(i64),
    None,
}

pub const KINESIS_SPLIT_TYPE: &str= "kinesis";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisSplit {
    pub(crate) shard_id: String,
    pub(crate) start_position: KinesisOffset,
    pub(crate) end_position: KinesisOffset,
}

impl SourceSplit for KinesisSplit {
    fn id(&self) -> String {
        self.shard_id.to_string()
    }

    fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }

    fn get_type(&self) -> String {
        KINESIS_SPLIT_TYPE.to_string()
    }
}

impl KinesisSplit {
    pub fn new(
        shard_id: String,
        start_position: KinesisOffset,
        end_position: KinesisOffset,
    ) -> KinesisSplit {
        KinesisSplit {
            shard_id,
            start_position,
            end_position,
        }
    }
}
