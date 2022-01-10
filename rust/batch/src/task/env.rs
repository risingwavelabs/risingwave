use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_source::{SourceManager, SourceManagerRef};
use risingwave_storage::table::{TableManager, TableManagerRef};

use crate::task::TaskManager;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct BatchTaskEnv {
    table_manager: TableManagerRef,
    server_addr: SocketAddr,
    task_manager: Arc<TaskManager>,
    source_manager: SourceManagerRef,
}

impl BatchTaskEnv {
    pub fn new(
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        task_manager: Arc<TaskManager>,
        server_addr: SocketAddr,
    ) -> Self {
        BatchTaskEnv {
            table_manager,
            server_addr,
            task_manager,
            source_manager,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_source::MemSourceManager;
        use risingwave_storage::table::SimpleTableManager;

        BatchTaskEnv {
            table_manager: Arc::new(SimpleTableManager::with_in_memory_store()),
            task_manager: Arc::new(TaskManager::new()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: std::sync::Arc::new(MemSourceManager::new()),
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

    pub fn task_manager(&self) -> Arc<TaskManager> {
        self.task_manager.clone()
    }

    pub fn source_manager(&self) -> &dyn SourceManager {
        &*self.source_manager
    }

    pub fn source_manager_ref(&self) -> SourceManagerRef {
        self.source_manager.clone()
    }
}
