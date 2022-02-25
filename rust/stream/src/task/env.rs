use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_common::config::StreamingConfig;
use risingwave_source::{SourceManager, SourceManagerRef};
use risingwave_storage::table::{TableManager, TableManagerRef};

pub(crate) type WorkerNodeId = u32;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct StreamEnvironment {
    /// The table manager.
    table_manager: TableManagerRef,

    /// Endpoint the stream manager listens on.
    server_addr: SocketAddr,

    /// Reference to the source manager.
    source_manager: SourceManagerRef,

    /// Streaming related configurations.
    config: Arc<StreamingConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,
}

impl StreamEnvironment {
    pub fn new(
        table_manager: TableManagerRef,
        source_manager: SourceManagerRef,
        server_addr: SocketAddr,
        config: Arc<StreamingConfig>,
        worker_id: WorkerNodeId,
    ) -> Self {
        StreamEnvironment {
            table_manager,
            server_addr,
            source_manager,
            config,
            worker_id,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_source::MemSourceManager;
        use risingwave_storage::table::SimpleTableManager;

        StreamEnvironment {
            table_manager: Arc::new(SimpleTableManager::with_in_memory_store()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: Arc::new(MemSourceManager::new()),
            config: Arc::new(StreamingConfig::default()),
            worker_id: WorkerNodeId::default(),
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

    pub fn config(&self) -> &StreamingConfig {
        self.config.as_ref()
    }

    pub fn worker_id(&self) -> WorkerNodeId {
        self.worker_id
    }
}
