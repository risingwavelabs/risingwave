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
//
use std::net::SocketAddr;
use std::sync::Arc;

use risingwave_common::config::StreamingConfig;
use risingwave_source::{SourceManager, SourceManagerRef};
use risingwave_storage::StateStoreImpl;
use tokio::sync::mpsc::UnboundedSender;

pub(crate) type WorkerNodeId = u32;

/// The global environment for task execution.
/// The instance will be shared by every task.
#[derive(Clone, Debug)]
pub struct StreamEnvironment {
    /// Endpoint the stream manager listens on.
    server_addr: SocketAddr,

    /// Reference to the source manager.
    source_manager: SourceManagerRef,

    /// Streaming related configurations.
    config: Arc<StreamingConfig>,

    /// Current worker node id.
    worker_id: WorkerNodeId,

    /// State store for table scanning.
    state_store: StateStoreImpl,

    /// Shutdown sender for current stream service.
    shutdown_sender: Option<UnboundedSender<()>>,
}

impl StreamEnvironment {
    pub fn new(
        source_manager: SourceManagerRef,
        server_addr: SocketAddr,
        config: Arc<StreamingConfig>,
        worker_id: WorkerNodeId,
        state_store: StateStoreImpl,
        shutdown_sender: UnboundedSender<()>,
    ) -> Self {
        StreamEnvironment {
            server_addr,
            source_manager,
            config,
            worker_id,
            state_store,
            shutdown_sender: Some(shutdown_sender),
        }
    }

    // Create an instance for testing purpose.
    #[cfg(test)]
    pub fn for_test() -> Self {
        use risingwave_source::MemSourceManager;
        use risingwave_storage::monitor::StateStoreMetrics;

        StreamEnvironment {
            server_addr: SocketAddr::V4("127.0.0.1:5688".parse().unwrap()),
            source_manager: Arc::new(MemSourceManager::new()),
            config: Arc::new(StreamingConfig::default()),
            worker_id: WorkerNodeId::default(),
            state_store: StateStoreImpl::shared_in_memory_store(Arc::new(
                StateStoreMetrics::unused(),
            )),
            shutdown_sender: None,
        }
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

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }

    pub fn shutdown(&self) {
        self.shutdown_sender.as_ref().unwrap().send(()).unwrap();
    }
}
