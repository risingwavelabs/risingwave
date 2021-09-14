use crate::storage::{StorageManager, StorageManagerRef};
use crate::task::TaskManager;
use std::net::SocketAddr;
use std::sync::Arc;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct GlobalTaskEnv {
    storage_manager: StorageManagerRef,
    server_addr: SocketAddr,
    task_manager: Arc<TaskManager>,
}

impl GlobalTaskEnv {
    pub fn new(
        storage_manager: StorageManagerRef,
        task_manager: Arc<TaskManager>,
        server_addr: SocketAddr,
    ) -> Self {
        GlobalTaskEnv {
            storage_manager,
            task_manager,
            server_addr,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use crate::storage::MemStorageManager;
        GlobalTaskEnv {
            storage_manager: Arc::new(MemStorageManager::new()),
            task_manager: Arc::new(TaskManager::new()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
        }
    }

    pub fn storage_manager(&self) -> &dyn StorageManager {
        &*self.storage_manager
    }

    pub fn storage_manager_ref(&self) -> StorageManagerRef {
        self.storage_manager.clone()
    }

    pub fn server_address(&self) -> &SocketAddr {
        &self.server_addr
    }

    pub fn task_manager(&self) -> Arc<TaskManager> {
        self.task_manager.clone()
    }
}
