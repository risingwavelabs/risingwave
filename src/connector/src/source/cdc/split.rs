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

use std::marker::PhantomData;

use anyhow::{anyhow, Context};
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::source::cdc::external::DebeziumOffset;
use crate::source::cdc::{CdcSourceType, CdcSourceTypeTrait};
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

trait CdcSplitTrait: Send + Sync {
    fn split_id(&self) -> u32;
    fn start_offset(&self) -> &Option<String>;
    fn is_snapshot_done(&self) -> bool;
    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()>;
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MongoDbCdcSplit {
    pub inner: CdcSplitBase,
}

impl MySqlCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }
}

impl CdcSplitTrait for MySqlCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let mut snapshot_done = self.inner.snapshot_done;
        if !snapshot_done {
            let dbz_offset: DebeziumOffset =
                serde_json::from_str(&start_offset).with_context(|| {
                    format!(
                        "invalid mysql offset: {}, split: {}",
                        start_offset, self.inner.split_id
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
    pub fn new(split_id: u32, start_offset: Option<String>, server_addr: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self {
            inner: split,
            server_addr,
        }
    }
}

impl CdcSplitTrait for PostgresCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let mut snapshot_done = self.inner.snapshot_done;
        if !snapshot_done {
            let dbz_offset: DebeziumOffset =
                serde_json::from_str(&start_offset).with_context(|| {
                    format!(
                        "invalid postgres offset: {}, split: {}",
                        start_offset, self.inner.split_id
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

impl MongoDbCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }
}

impl CdcSplitTrait for MongoDbCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let mut snapshot_done = self.inner.snapshot_done;
        // extract snapshot state from debezium offset
        if !snapshot_done {
            let dbz_offset: DebeziumOffset = serde_json::from_str(&start_offset).map_err(|e| {
                anyhow!(
                    "invalid mongodb offset: {}, error: {}, split: {}",
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
        // if snapshot_done is already true, it will remain true
        self.inner.snapshot_done = snapshot_done;
        Ok(())
    }
}

/// We use this struct to wrap the specific split, which act as an interface to other modules
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct DebeziumCdcSplit<T: CdcSourceTypeTrait> {
    pub mysql_split: Option<MySqlCdcSplit>,

    #[serde(rename = "pg_split")] // backward compatibility
    pub postgres_split: Option<PostgresCdcSplit>,
    pub citus_split: Option<PostgresCdcSplit>,
    pub mongodb_split: Option<MongoDbCdcSplit>,

    #[serde(skip)]
    pub _phantom: PhantomData<T>,
}

macro_rules! dispatch_cdc_split {
    ($dbz_split:expr, $as_type:tt, {$($cdc_source_type:tt),*}, $body:expr) => {
        match T::source_type() {
            $(
                CdcSourceType::$cdc_source_type => {
                    $crate::paste! {
                        $dbz_split.[<$cdc_source_type:lower _split>]
                            .[<as_ $as_type>]()
                            .expect(concat!(stringify!([<$cdc_source_type:lower>]), " split must exist"))
                            .$body
                    }
                }
            )*
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split");
            }
        }
    }
}

impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
    fn id(&self) -> SplitId {
        format!("{}", self.split_id()).into()
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        self.update_with_offset(start_offset)
    }
}

impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
    pub fn new(split_id: u32, start_offset: Option<String>, server_addr: Option<String>) -> Self {
        let mut ret = Self {
            mysql_split: None,
            postgres_split: None,
            citus_split: None,
            mongodb_split: None,
            _phantom: PhantomData,
        };
        match T::source_type() {
            CdcSourceType::Mysql => {
                let split = MySqlCdcSplit::new(split_id, start_offset);
                ret.mysql_split = Some(split);
            }
            CdcSourceType::Postgres => {
                let split = PostgresCdcSplit::new(split_id, start_offset, None);
                ret.postgres_split = Some(split);
            }
            CdcSourceType::Citus => {
                let split = PostgresCdcSplit::new(split_id, start_offset, server_addr);
                ret.citus_split = Some(split);
            }
            CdcSourceType::Mongodb => {
                let split = MongoDbCdcSplit::new(split_id, start_offset);
                ret.mongodb_split = Some(split);
            }
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split")
            }
        }
        ret
    }

    pub fn split_id(&self) -> u32 {
        dispatch_cdc_split!(self, ref, {
            Mysql,
            Postgres,
            Citus,
            Mongodb
        }, split_id())
    }

    pub fn start_offset(&self) -> &Option<String> {
        dispatch_cdc_split!(self, ref, {
            Mysql,
            Postgres,
            Citus,
            Mongodb
        }, start_offset())
    }

    pub fn snapshot_done(&self) -> bool {
        dispatch_cdc_split!(self, ref, {
            Mysql,
            Postgres,
            Citus,
            Mongodb
        }, is_snapshot_done())
    }

    pub fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        dispatch_cdc_split!(self, mut, {
            Mysql,
            Postgres,
            Citus,
            Mongodb
        }, update_with_offset(start_offset)?);
        Ok(())
    }
}
