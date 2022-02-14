use std::net::SocketAddr;

use risingwave_common::worker_id::WorkerIdRef;
use risingwave_source::{SourceManager, SourceManagerRef};
use risingwave_storage::table::{TableManager, TableManagerRef};

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct StreamTaskEnv {
    table_manager: TableManagerRef,
    server_addr: SocketAddr,
    source_manager: SourceManagerRef,
    worker_id_ref: WorkerIdRef,
}

impl StreamTaskEnv {
    pub fn new(
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        server_addr: SocketAddr,
        worker_id_ref: WorkerIdRef,
    ) -> Self {
        StreamTaskEnv {
            table_manager,
            server_addr,
            source_manager,
            worker_id_ref,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::sync::Arc;

        use risingwave_source::MemSourceManager;
        use risingwave_storage::table::SimpleTableManager;

        StreamTaskEnv {
            table_manager: Arc::new(SimpleTableManager::with_in_memory_store()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: Arc::new(MemSourceManager::new()),
            worker_id_ref: WorkerIdRef::for_test(),
        }
    }

    pub fn table_manager(&self) -> &dyn TableManager {
        &*self.table_manager
    }

    pub fn table_manager_ref(&self) -> TableManagerRef {
        self.table_manager.clone()
    }

    pub fn server_address(&self) -> &SocketAddr {
        &self.server_addr
    }

    pub fn source_manager(&self) -> &dyn SourceManager {
        &*self.source_manager
    }

    pub fn source_manager_ref(&self) -> SourceManagerRef {
        self.source_manager.clone()
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id_ref.get()
    }
}
