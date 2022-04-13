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

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PulsarOffset {
    MessageID(u64),
    Timestamp(u64),
    None,
}

pub const PULSAR_SPLIT_TYPE: &str = "pulsar";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarSplit {
    pub(crate) sub_topic: String,
    pub(crate) start_offset: PulsarOffset,
    pub(crate) stop_offset: PulsarOffset,
}

impl PulsarSplit {
    pub fn new(sub_topic: String, start_offset: PulsarOffset, stop_offset: PulsarOffset) -> Self {
        Self {
            sub_topic,
            start_offset,
            stop_offset,
        }
    }
}

impl SourceSplit for PulsarSplit {
    fn id(&self) -> String {
        self.sub_topic.clone()
    }

    fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }

    fn get_type(&self) -> String {
        PULSAR_SPLIT_TYPE.to_string()
    }
}
