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

use std::fmt::{Display, Formatter};
use std::sync::Arc;

use prometheus::core::Atomic;
use risingwave_batch::error::Result;
use risingwave_batch::monitor::BatchMetrics;
use risingwave_batch::task::{BatchTaskContext, TaskOutput, TaskOutputId};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeManagerRef;
use risingwave_common::catalog::SysCatalogReaderRef;
use risingwave_common::config::BatchConfig;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::TrAdderAtomic;
use risingwave_common::util::addr::{HostAddr, is_local_address};
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_pb::task_service::PbTaskStats;
use risingwave_rpc_client::ComputeClientPoolRef;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::session::SessionImpl;

/// Batch task execution context in frontend.
#[derive(Clone)]
pub struct FrontendBatchTaskContext {
    session: Arc<SessionImpl>,

    mem_context: MemoryContext,
}

impl FrontendBatchTaskContext {
    pub fn create(session: Arc<SessionImpl>) -> Arc<dyn BatchTaskContext> {
        let mem_context =
            MemoryContext::new(Some(session.env().mem_context()), TrAdderAtomic::new(0));
        Arc::new(Self {
            session,
            mem_context,
        })
    }
}

impl BatchTaskContext for FrontendBatchTaskContext {
    fn get_task_output(&self, _task_output_id: TaskOutputId) -> Result<TaskOutput> {
        unimplemented!("not supported in local mode")
    }

    fn catalog_reader(&self) -> SysCatalogReaderRef {
        Arc::new(SysCatalogReaderImpl::new(
            self.session.env().catalog_reader().clone(),
            self.session.env().user_info_reader().clone(),
            self.session.env().meta_client_ref(),
            self.session.auth_context(),
            self.session.shared_config(),
            self.session.env().system_params_manager().get_params(),
        ))
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        is_local_address(self.session.env().server_address(), peer_addr)
    }

    fn state_store(&self) -> risingwave_storage::store_impl::StateStoreImpl {
        unimplemented!("not supported in local mode")
    }

    fn batch_metrics(&self) -> Option<BatchMetrics> {
        None
    }

    fn client_pool(&self) -> ComputeClientPoolRef {
        self.session.env().client_pool()
    }

    fn get_config(&self) -> &BatchConfig {
        self.session.env().batch_config()
    }

    fn dml_manager(&self) -> risingwave_dml::dml_manager::DmlManagerRef {
        unimplemented!("not supported in local mode")
    }

    fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.session.env().source_metrics()
    }

    fn spill_metrics(&self) -> Arc<risingwave_batch::monitor::BatchSpillMetrics> {
        self.session.env().spill_metrics()
    }

    fn create_executor_mem_context(&self, _executor_id: &str) -> MemoryContext {
        MemoryContext::new(Some(self.mem_context.clone()), TrAdderAtomic::new(0))
    }

    fn worker_node_manager(&self) -> Option<WorkerNodeManagerRef> {
        Some(self.session.env().worker_node_manager_ref())
    }

    fn task_stats(&self) -> Option<risingwave_batch::task::task_stats::TaskStatsRef> {
        None
    }
}

pub type StageStats = QueryStats;

#[derive(Debug)]
pub struct QueryStats {
    pub row_scan_count: u64,
}

impl QueryStats {
    pub fn new() -> Self {
        Self { row_scan_count: 0 }
    }

    pub fn add_task_stats(&mut self, task_stats: &PbTaskStats) {
        self.row_scan_count += task_stats.row_scan_count;
    }

    pub fn add_stage_stats(&mut self, stage_stats: &StageStats) {
        self.row_scan_count += stage_stats.row_scan_count;
    }
}

impl Display for QueryStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "row_scan_count={}", self.row_scan_count)
    }
}
