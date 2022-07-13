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

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::catalog::TableId;
use risingwave_common::ensure;
use risingwave_common::error::Result;

use crate::monitor::SinkMetrics;

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
    sinks: Mutex<HashSet<TableId>>,
    /// Located worker id.
    _worker_id: u32,
}

#[async_trait]
impl SinkManager for MemSinkManager {
    async fn create_sink(&self, sink_id: &TableId) -> Result<()> {
        // TODO(nanderstabel): Actually implement create_sink.

        let mut sinks = self.get_sinks()?;
        ensure!(
            !sinks.contains(sink_id),
            "Sink id already exists: {:?}",
            sink_id
        );
        sinks.insert(*sink_id);

        Ok(())
    }

    fn drop_sink(&self, table_id: &TableId) -> Result<()> {
        let mut sinks = self.get_sinks()?;
        ensure!(
            sinks.contains(table_id),
            "Sink does not exist: {:?}",
            table_id
        );
        sinks.remove(table_id);
        Ok(())
    }

    fn clear_sinks(&self) -> Result<()> {
        let mut sinks = self.get_sinks()?;
        sinks.clear();
        Ok(())
    }
}

impl MemSinkManager {
    pub fn new(_worker_id: u32, _metrics: Arc<SinkMetrics>) -> Self {
        MemSinkManager {
            sinks: Mutex::new(HashSet::new()),
            _worker_id,
        }
    }

    fn get_sinks(&self) -> Result<MutexGuard<HashSet<TableId>>> {
        Ok(self.sinks.lock())
    }
}
