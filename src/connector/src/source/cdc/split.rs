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

use anyhow::anyhow;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::source::{SplitId, SplitMetaData};

/// The base states of a CDC split, which will be persisted to checkpoint.
/// CDC source only has single split, so we use the `source_id` to identify the split.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct CdcSplitBase {
    pub split_id: u32,
    pub start_offset: Option<String>,
    pub snapshot_done: bool,
}

impl CdcSplitBase {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        Self {
            split_id,
            start_offset,
            snapshot_done: false,
        }
    }
}

// Example debezium offset JSON:
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
    // postgres field
    last_snapshot_record: Option<bool>,
    // mysql field
    snapshot: Option<bool>,
    lsn: u64,
    tx_id: u64,
    ts_usec: u64,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MySqlCdcSplit {
    pub inner: CdcSplitBase,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PostgresCdcSplit {
    pub inner: CdcSplitBase,
    // the hostname and port of a node that holding shard tables (for Citus)
    pub server_addr: Option<String>,
}

impl MySqlCdcSplit {
    pub fn new(split_id: u32, start_offset: String) -> MySqlCdcSplit {
        let split = CdcSplitBase {
            split_id,
            start_offset: Some(start_offset),
            snapshot_done: false,
        };
        Self { inner: split }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        // deserialize the start_offset
        let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset)
            .unwrap_or_else(|_| panic!("invalid cdc offset: {}", start_offset));

        info!("dbz_offset: {:?}", dbz_offset);

        let snapshot_done = match dbz_offset.source_offset.snapshot {
            Some(val) => !val,
            None => true,
        };

        let split = CdcSplitBase {
            split_id: self.inner.split_id,
            start_offset: Some(start_offset),
            snapshot_done,
        };
        Self { inner: split }
    }
}

impl PostgresCdcSplit {
    pub fn new(split_id: u32, start_offset: String) -> PostgresCdcSplit {
        let split = CdcSplitBase {
            split_id,
            start_offset: Some(start_offset),
            snapshot_done: false,
        };
        Self {
            inner: split,
            server_addr: None,
        }
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        // deserialize the start_offset
        let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset)
            .unwrap_or_else(|_| panic!("invalid cdc offset: {}", start_offset));

        info!("dbz_offset: {:?}", dbz_offset);
        let snapshot_done = dbz_offset
            .source_offset
            .last_snapshot_record
            .unwrap_or(false);

        let split = CdcSplitBase {
            split_id: self.inner.split_id,
            start_offset: Some(start_offset),
            snapshot_done,
        };

        let server_addr = self.server_addr.clone();
        Self {
            inner: split,
            server_addr,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct DebeziumCdcSplit {
    pub mysql_split: Option<MySqlCdcSplit>,
    pub pg_split: Option<PostgresCdcSplit>,
}

impl SplitMetaData for DebeziumCdcSplit {
    fn id(&self) -> SplitId {
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &self.mysql_split {
            return format!("{}", split.inner.split_id).into();
        }
        if let Some(split) = &self.pg_split {
            return format!("{}", split.inner.split_id).into();
        }
        unreachable!("invalid split")
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }
}

impl DebeziumCdcSplit {
    pub fn new(mysql_split: Option<MySqlCdcSplit>, pg_split: Option<PostgresCdcSplit>) -> Self {
        Self {
            mysql_split,
            pg_split,
        }
    }

    pub fn split_id(&self) -> u32 {
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &self.mysql_split {
            return split.inner.split_id;
        }
        if let Some(split) = &self.pg_split {
            return split.inner.split_id;
        }

        unreachable!("invalid split")
    }

    pub fn start_offset(&self) -> &Option<String> {
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &self.mysql_split {
            return &split.inner.start_offset;
        }
        if let Some(split) = &self.pg_split {
            return &split.inner.start_offset;
        }
        unreachable!("invalid split")
    }

    pub fn server_addr(&self) -> &Option<String> {
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &self.pg_split {
            return &split.server_addr;
        }
        unreachable!("invalid split")
    }

    pub fn copy_with_offset(&self, start_offset: String) -> Self {
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &self.mysql_split {
            let mysql_split = Some(split.copy_with_offset(start_offset));
            return Self {
                mysql_split,
                pg_split: None,
            };
        }
        if let Some(split) = &self.pg_split {
            let pg_split = Some(split.copy_with_offset(start_offset));
            return Self {
                mysql_split: None,
                pg_split,
            };
        }
        unreachable!("invalid split")
    }
}

// impl SplitMetaData for MySqlCdcSplit {
//     fn id(&self) -> SplitId {
//         format!("{}", self.inner.split_id).into()
//     }
//
//     fn encode_to_json(&self) -> JsonbVal {
//         serde_json::to_value(self.clone()).unwrap().into()
//     }
//
//     fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
//         serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
//     }
// }
//
// impl SplitMetaData for PostgresCdcSplit {
//     fn id(&self) -> SplitId {
//         format!("{}", self.inner.split_id).into()
//     }
//
//     fn encode_to_json(&self) -> JsonbVal {
//         serde_json::to_value(self.clone()).unwrap().into()
//     }
//
//     fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
//         serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
//     }
// }

// impl SplitMetaData for CdcSplit {
//     fn id(&self) -> SplitId {
//         format!("{}", self.split_id).into()
//     }
//
//     fn encode_to_json(&self) -> JsonbVal {
//         serde_json::to_value(self.clone()).unwrap().into()
//     }
//
//     fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
//         serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
//     }
// }

// impl CdcSplit {
//     pub fn new(split_id: u32, start_offset: String) -> CdcSplit {
//         Self {
//             split_id,
//             start_offset: Some(start_offset),
//             snapshot_done: false,
//         }
//     }
// }
