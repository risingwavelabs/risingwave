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

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;

/// The local sink manager on the compute node.
#[async_trait]
pub trait SinkManager: Debug + Sync + Send {
    async fn create_sink(&self, table_id: &TableId) -> Result<()>;

    fn drop_sink(&self, sink_id: &TableId) -> Result<()>;

    /// Clear sinks, this is used when failover happens.
    fn clear_sinks(&self) -> Result<()>;
}

pub type SinkManagerRef = Arc<dyn SinkManager>;

#[derive(Debug, Default)]
pub struct MemSinkManager {
    // sinks: Mutex<HashMap<TableId, SinkDesc>>,
    /// Located worker id.
    worker_id: u32,
}

#[async_trait]
impl SinkManager for MemSinkManager {
    async fn create_sink(&self, sink_id: &TableId) -> Result<()> {
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
            // sinks: Mutex::new(HashMap::new()),
            worker_id,
        }
    }
}
