use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_common::config::BatchConfig;
use risingwave_common::worker_id::WorkerIdRef;
use risingwave_source::{SourceManager, SourceManagerRef};
use risingwave_storage::table::{TableManager, TableManagerRef};

use crate::task::BatchManager;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct BatchEnvironment {
    /// The table manager.
    table_manager: TableManagerRef,

    /// Endpoint the batch task manager listens on.
    server_addr: SocketAddr,

    /// Reference to the task manager.
    task_manager: Arc<BatchManager>,

    /// Reference to the source manager. This is used to query the sources.
    source_manager: SourceManagerRef,

    /// Batch related configurations.
    config: Arc<BatchConfig>,

    /// Reference to the worker node id.
    worker_id_ref: WorkerIdRef,
}

impl BatchEnvironment {
    pub fn new(
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        task_manager: Arc<BatchManager>,
        server_addr: SocketAddr,
        config: Arc<BatchConfig>,
        worker_id_ref: WorkerIdRef,
    ) -> Self {
        BatchEnvironment {
            table_manager,
            server_addr,
            task_manager,
            source_manager,
            config,
            worker_id_ref,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_source::MemSourceManager;
        use risingwave_storage::table::SimpleTableManager;

        BatchEnvironment {
            table_manager: Arc::new(SimpleTableManager::with_in_memory_store()),
            task_manager: Arc::new(BatchManager::new()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: std::sync::Arc::new(MemSourceManager::new()),
            config: Arc::new(BatchConfig::default()),
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

    pub fn task_manager(&self) -> Arc<BatchManager> {
        self.task_manager.clone()
    }

    pub fn source_manager(&self) -> &dyn SourceManager {
        &*self.source_manager
    }

    pub fn source_manager_ref(&self) -> SourceManagerRef {
        self.source_manager.clone()
    }

    pub fn config(&self) -> &BatchConfig {
        self.config.as_ref()
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id_ref.get()
    }
}
