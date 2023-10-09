// Copyright 2023 RisingWave Labs
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

use anyhow::{anyhow, Ok};
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::source::{SplitId, SplitMetaData};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum NatsOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(i128),
    None,
}

/// The states of a NATS split, which will be persisted to checkpoint.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct NatsSplit {
    pub(crate) subject: String,
    // TODO: to simplify the logic, return 1 split for first version. May use parallelism in
    // future.
    pub(crate) split_id: SplitId,
    pub(crate) start_sequence: NatsOffset,
}

impl SplitMetaData for NatsSplit {
    fn id(&self) -> SplitId {
        // TODO: should avoid constructing a string every time
        format!("{}", self.split_id).into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, start_sequence: String) -> anyhow::Result<()> {
        let start_sequence = if start_sequence.is_empty() {
            NatsOffset::Earliest
        } else {
            NatsOffset::SequenceNumber(start_sequence)
        };
        self.start_sequence = start_sequence;
        Ok(())
    }
}

impl NatsSplit {
    pub fn new(subject: String, split_id: SplitId, start_sequence: NatsOffset) -> Self {
        Self {
            subject,
            split_id,
            start_sequence,
        }
    }
}
