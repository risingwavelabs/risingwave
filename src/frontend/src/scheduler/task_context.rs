use std::sync::Arc;

use risingwave_batch::executor2::BatchMetrics;
use risingwave_batch::task::{BatchTaskContext, TaskId, TaskOutput, TaskOutputId};
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::HostAddr;
use risingwave_source::SourceManagerRef;

/// Batch task execution context in frontend.
#[derive(Clone, Default)]
pub struct FrontendBatchTaskContext {}

impl BatchTaskContext for FrontendBatchTaskContext {
    fn get_task_output(
        &self,
        task_output_id: TaskOutputId,
    ) -> risingwave_common::error::Result<TaskOutput<Self>> {
        todo!()
    }

    fn is_local_addr(&self, peer_addr: &HostAddr) -> bool {
        todo!()
    }

    fn get_task_error(&self, task_id: TaskId) -> Result<Option<RwError>> {
        todo!()
    }

    fn source_manager_ref(&self) -> Option<SourceManagerRef> {
        todo!()
    }

    fn state_store(&self) -> Option<risingwave_storage::store_impl::StateStoreImpl> {
        todo!()
    }

    fn stats(&self) -> Arc<BatchMetrics> {
        todo!()
    }

    fn try_get_error(&self, task_id: &TaskId) -> risingwave_common::error::Result<Option<RwError>> {
        todo!()
    }
}
