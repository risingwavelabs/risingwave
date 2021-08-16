use crate::storage::{StorageManager, StorageManagerRef};

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub(crate) struct GlobalTaskEnv {
    storage_manager: StorageManagerRef,
}

impl GlobalTaskEnv {
    pub(crate) fn new(storage_manager: StorageManagerRef) -> Self {
        GlobalTaskEnv { storage_manager }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        use crate::storage::MemStorageManager;
        GlobalTaskEnv {
            storage_manager: std::sync::Arc::new(MemStorageManager::new()),
        }
    }

    pub(crate) fn storage_manager(&self) -> &dyn StorageManager {
        &*self.storage_manager
    }

    pub(crate) fn storage_manager_ref(&self) -> StorageManagerRef {
        self.storage_manager.clone()
    }
}
