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

use std::marker::PhantomData;

use anyhow::anyhow;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::source::cdc::CdcSourceTypeTrait;
use crate::source::external::DebeziumOffset;
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

    pub fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let mut snapshot_done = self.inner.snapshot_done;
        if !snapshot_done {
            let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset).map_err(|e| {
                anyhow!(
                    "invalid mysql offset: {}, error: {}, split: {}",
                    start_offset,
                    e,
                    self.inner.split_id
                )
            })?;

            // heartbeat event should not update the `snapshot_done` flag
            if !dbz_offset.is_heartbeat {
                snapshot_done = match dbz_offset.source_offset.snapshot {
                    Some(val) => !val,
                    None => true,
                };
            }
        }
        self.inner.start_offset = Some(start_offset);
        // if snapshot_done is already true, it won't be updated
        self.inner.snapshot_done = snapshot_done;
        Ok(())
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

    pub fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let mut snapshot_done = self.inner.snapshot_done;
        if !snapshot_done {
            let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset).map_err(|e| {
                anyhow!(
                    "invalid postgres offset: {}, error: {}, split: {}",
                    start_offset,
                    e,
                    self.inner.split_id
                )
            })?;

            // heartbeat event should not update the `snapshot_done` flag
            if !dbz_offset.is_heartbeat {
                snapshot_done = dbz_offset
                    .source_offset
                    .last_snapshot_record
                    .unwrap_or(false);
            }
        }
        self.inner.start_offset = Some(start_offset);
        // if snapshot_done is already true, it won't be updated
        self.inner.snapshot_done = snapshot_done;
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct DebeziumCdcSplit<T: CdcSourceTypeTrait> {
    pub mysql_split: Option<MySqlCdcSplit>,
    pub pg_split: Option<PostgresCdcSplit>,

    #[serde(skip)]
    pub _phantom: PhantomData<T>,
}

impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
    fn id(&self) -> SplitId {
        // TODO: may check T to get the specific cdc type
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

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        // TODO: may check T to get the specific cdc type
        assert!(self.mysql_split.is_some() || self.pg_split.is_some());
        if let Some(split) = &mut self.mysql_split {
            split.update_with_offset(start_offset)?
        } else if let Some(split) = &mut self.pg_split {
            split.update_with_offset(start_offset)?
        }
        Ok(())
    }
}

impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
    pub fn new(mysql_split: Option<MySqlCdcSplit>, pg_split: Option<PostgresCdcSplit>) -> Self {
        Self {
            mysql_split,
            pg_split,
            _phantom: PhantomData,
        }
    }

    pub fn split_id(&self) -> u32 {
        if let Some(split) = &self.mysql_split {
            return split.inner.split_id;
        }
        if let Some(split) = &self.pg_split {
            return split.inner.split_id;
        }
        unreachable!("invalid debezium split")
    }

    pub fn start_offset(&self) -> &Option<String> {
        if let Some(split) = &self.mysql_split {
            return &split.inner.start_offset;
        }
        if let Some(split) = &self.pg_split {
            return &split.inner.start_offset;
        }
        unreachable!("invalid debezium split")
    }

    pub fn snapshot_done(&self) -> bool {
        if let Some(split) = &self.mysql_split {
            return split.inner.snapshot_done;
        }
        if let Some(split) = &self.pg_split {
            return split.inner.snapshot_done;
        }
        unreachable!("invalid debezium split")
    }

    pub fn server_addr(&self) -> Option<String> {
        if let Some(split) = &self.pg_split {
            split.server_addr.clone()
        } else {
            None
        }
    }
}
