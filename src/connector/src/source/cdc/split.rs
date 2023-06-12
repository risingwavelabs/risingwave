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

use std::collections::HashMap;

use anyhow::anyhow;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::source::{SplitId, SplitMetaData};

/// The states of a CDC split, which will be persisted to checkpoint.
/// CDC source only has single split, so we use the `source_id` to identify the split.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct CdcSplit {
    pub split_id: u32,
    // the hostname and port of a node that holding shard tables
    pub server_addr: Option<String>,
    pub start_offset: Option<String>,

    pub snapshot_done: bool,
}

impl SplitMetaData for CdcSplit {
    fn id(&self) -> SplitId {
        format!("{}", self.split_id).into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }
}

// {
//     "sourcePartition":
//     {
//         "server": "RW_CDC_public.te"
//     },
//     "sourceOffset":
//     {
//         "last_snapshot_record": false,
//         "lsn": 29973552,
//         "txId": 1046,
//         "ts_usec": 1670826189008456,
//         "snapshot": true
//     }
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DebeziumOffset {
    #[serde(rename = "sourceOffset")]
    source_offset: DebeziumSourceOffset,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DebeziumSourceOffset {
    last_snapshot_record: bool,
    lsn: u64,
    tx_id: u64,
    ts_usec: u64,
    snapshot: bool,
}

impl CdcSplit {
    pub fn new(split_id: u32, start_offset: String) -> CdcSplit {
        Self {
            split_id,
            server_addr: None,
            start_offset: Some(start_offset),
            snapshot_done: false,
        }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        // deserialize the start_offset
        let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset)
            .expect(&format!("invalid cdc offset: {}", start_offset));

        Self {
            split_id: self.split_id,
            server_addr: self.server_addr.clone(),
            start_offset: Some(start_offset),
            snapshot_done: !dbz_offset.source_offset.snapshot,
        }
    }
}
