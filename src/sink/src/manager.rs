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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

/// The local sink manager on the compute node.
#[async_trait]
pub trait SinkManager: Debug + Sync + Send {
    async fn create_sink(&self, table_id: &TableId) -> Result<()>;

    fn get_sink(&self, sink_id: &TableId) -> Result<SinkDesc>;
    fn drop_sink(&self, sink_id: &TableId) -> Result<()>;

    /// Clear sinks, this is used when failover happens.
    fn clear_sinks(&self) -> Result<()>;
}

/// `SinkColumnDesc` is used to describe a column in the Sink and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SinkColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub skip_parse: bool,
}

impl From<&ColumnDesc> for SinkColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            name: c.name.clone(),
            data_type: c.data_type.clone(),
            column_id: c.column_id,
            skip_parse: false,
        }
    }
}

/// `SinkDesc` is used to describe a `Sink`
#[derive(Clone, Debug)]
pub struct SinkDesc {
    pub columns: Vec<SinkColumnDesc>,
}

pub type SinkManagerRef = Arc<dyn SinkManager>;

#[derive(Debug, Default)]
pub struct MemSinkManager {
    sinks: Mutex<HashMap<TableId, SinkDesc>>,
    /// Located worker id.
    worker_id: u32,
}

#[async_trait]
impl SinkManager for MemSinkManager {
    async fn create_sink(&self, sink_id: &TableId) -> Result<()> {
        todo!();
    }

    fn get_sink(&self, table_id: &TableId) -> Result<SinkDesc> {
        todo!();
    }

    fn drop_sink(&self, table_id: &TableId) -> Result<()> {
        todo!();
    }

    fn clear_sinks(&self) -> Result<()> {
        todo!();
    }
}

impl MemSinkManager {
    pub fn new(worker_id: u32) -> Self {
        MemSinkManager {
            sinks: Mutex::new(HashMap::new()),
            worker_id,
        }
    }

    fn get_sinks(&self) -> Result<MutexGuard<HashMap<TableId, SinkDesc>>> {
        Ok(self.sinks.lock())
    }
}
