use std::net::SocketAddr;

use risingwave_source::{SourceManager, SourceManagerRef};

use crate::stream::{StreamTableManager, StreamTableManagerRef};

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone)]
pub struct StreamTaskEnv {
    table_manager: StreamTableManagerRef,
    server_addr: SocketAddr,
    source_manager: SourceManagerRef,
}

impl StreamTaskEnv {
    pub fn new(
        table_manager: StreamTableManagerRef,
        source_manager: SourceManagerRef,
        server_addr: SocketAddr,
    ) -> Self {
        StreamTaskEnv {
            table_manager,
            server_addr,
            source_manager,
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use std::sync::Arc;

        use risingwave_source::MemSourceManager;

        use crate::stream::SimpleTableManager;
        StreamTaskEnv {
            table_manager: Arc::new(SimpleTableManager::new()),
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: std::sync::Arc::new(MemSourceManager::new()),
        }
    }

    pub fn table_manager(&self) -> &dyn StreamTableManager {
        &*self.table_manager
    }

    pub fn table_manager_ref(&self) -> StreamTableManagerRef {
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
}
