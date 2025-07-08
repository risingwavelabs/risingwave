// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
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
    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()>;

    // MySQL and MongoDB shares the same logic to extract the snapshot flag
    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
            format!(
                "invalid cdc offset: {}, split: {}",
                start_offset,
                self.split_id()
            )
        })?;

        // heartbeat event should not update the `snapshot_done` flag
        if !dbz_offset.is_heartbeat {
            snapshot_done = match dbz_offset.source_offset.snapshot {
                Some(val) => !val,
                None => true,
            };
        }
        Ok(snapshot_done)
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

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct MongoDbCdcSplit {
    pub inner: CdcSplitBase,
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct SqlServerCdcSplit {
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

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it won't be updated
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
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

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }

    fn extract_snapshot_flag(&self, start_offset: &str) -> ConnectorResult<bool> {
        // if snapshot_done is already true, it won't be changed
        let mut snapshot_done = self.is_snapshot_done();
        if snapshot_done {
            return Ok(snapshot_done);
        }

        let dbz_offset: DebeziumOffset = serde_json::from_str(start_offset).with_context(|| {
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
        Ok(snapshot_done)
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

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it will remain true
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
        Ok(())
    }
}

impl SqlServerCdcSplit {
    pub fn new(split_id: u32, start_offset: Option<String>) -> Self {
        let split = CdcSplitBase {
            split_id,
            start_offset,
            snapshot_done: false,
        };
        Self { inner: split }
    }
}

impl CdcSplitTrait for SqlServerCdcSplit {
    fn split_id(&self) -> u32 {
        self.inner.split_id
    }

    fn start_offset(&self) -> &Option<String> {
        &self.inner.start_offset
    }

    fn is_snapshot_done(&self) -> bool {
        self.inner.snapshot_done
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        // if snapshot_done is already true, it will remain true
        self.inner.snapshot_done = self.extract_snapshot_flag(last_seen_offset.as_str())?;
        self.inner.start_offset = Some(last_seen_offset);
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
    pub sql_server_split: Option<SqlServerCdcSplit>,

    #[serde(skip)]
    pub _phantom: PhantomData<T>,
}

macro_rules! dispatch_cdc_split_inner {
    ($dbz_split:expr, $as_type:tt, {$({$cdc_source_type:tt, $cdc_source_split:tt}),*}, $body:expr) => {
        match T::source_type() {
            $(
                CdcSourceType::$cdc_source_type => {
                    $crate::paste! {
                        $dbz_split.[<$cdc_source_split>]
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

// call corresponding split method of the specific cdc source type
macro_rules! dispatch_cdc_split {
    ($dbz_split:expr, $as_type:tt, $body:expr) => {
        dispatch_cdc_split_inner!($dbz_split, $as_type, {
            {Mysql, mysql_split},
            {Postgres, postgres_split},
            {Citus, citus_split},
            {Mongodb, mongodb_split},
            {SqlServer, sql_server_split}
        }, $body)
    }
}

impl<T: CdcSourceTypeTrait> SplitMetaData for DebeziumCdcSplit<T> {
    fn id(&self) -> SplitId {
        format!("{}", self.split_id()).into()
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn update_offset(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        self.update_offset_inner(last_seen_offset)
    }
}

impl<T: CdcSourceTypeTrait> DebeziumCdcSplit<T> {
    pub fn new(split_id: u32, start_offset: Option<String>, server_addr: Option<String>) -> Self {
        let mut ret = Self {
            mysql_split: None,
            postgres_split: None,
            citus_split: None,
            mongodb_split: None,
            sql_server_split: None,
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
            CdcSourceType::SqlServer => {
                let split = SqlServerCdcSplit::new(split_id, start_offset);
                ret.sql_server_split = Some(split);
            }
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split")
            }
        }
        ret
    }

    pub fn split_id(&self) -> u32 {
        dispatch_cdc_split!(self, ref, split_id())
    }

    pub fn start_offset(&self) -> &Option<String> {
        dispatch_cdc_split!(self, ref, start_offset())
    }

    pub fn snapshot_done(&self) -> bool {
        dispatch_cdc_split!(self, ref, is_snapshot_done())
    }

    pub fn update_offset_inner(&mut self, last_seen_offset: String) -> ConnectorResult<()> {
        dispatch_cdc_split!(self, mut, update_offset(last_seen_offset)?);
        Ok(())
    }

    /// Reset the CDC split state by clearing the offset and snapshot status
    /// This will cause the CDC source to restart from the latest position
    pub fn reset_to_latest(&mut self) {
        match T::source_type() {
            CdcSourceType::Mysql => {
                if let Some(ref mut split) = self.mysql_split {
                    split.inner.start_offset = None;
                    split.inner.snapshot_done = false;
                }
            }
            CdcSourceType::Postgres => {
                if let Some(ref mut split) = self.postgres_split {
                    split.inner.start_offset = None;
                    split.inner.snapshot_done = false;
                }
            }
            CdcSourceType::Citus => {
                if let Some(ref mut split) = self.citus_split {
                    split.inner.start_offset = None;
                    split.inner.snapshot_done = false;
                }
            }
            CdcSourceType::Mongodb => {
                if let Some(ref mut split) = self.mongodb_split {
                    split.inner.start_offset = None;
                    split.inner.snapshot_done = false;
                }
            }
            CdcSourceType::SqlServer => {
                if let Some(ref mut split) = self.sql_server_split {
                    split.inner.start_offset = None;
                    split.inner.snapshot_done = false;
                }
            }
            CdcSourceType::Unspecified => {
                unreachable!("invalid debezium split")
            }
        }
    }
}
