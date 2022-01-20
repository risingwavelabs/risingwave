use std::sync::Arc;

use super::{StreamClients, StreamClientsRef};
#[cfg(test)]
use crate::manager::MemEpochGenerator;
use crate::manager::{
    Config, EpochGenerator, EpochGeneratorRef, IdGeneratorManager, IdGeneratorManagerRef,
};
#[cfg(test)]
use crate::storage::MemStore;
use crate::storage::{MetaStore, MetaStoreRef};

/// [`MetaSrcEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv {
    /// global configuration.
    config: Arc<Config>,
    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef,
    /// meta store.
    meta_store: MetaStoreRef,
    /// epoch generator.
    epoch_generator: EpochGeneratorRef,
    /// stream clients memoization.
    stream_clients: StreamClientsRef,
}

impl MetaSrvEnv {
    pub async fn new(
        config: Arc<Config>,
        meta_store: MetaStoreRef,
        epoch_generator: EpochGeneratorRef,
    ) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            config,
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }

    // Instance for test.
    #[cfg(test)]
    pub async fn for_test() -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::new());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            config: Default::default(),
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }

    // Instance for test using sled as backend
    #[cfg(test)]
    pub async fn for_test_with_sled(sled_root: impl AsRef<std::path::Path>) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(crate::storage::SledMetaStore::new(sled_root.as_ref()).unwrap());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            config: Default::default(),
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }

    pub fn config(&self) -> Arc<Config> {
        self.config.clone()
    }

    pub fn meta_store(&self) -> &dyn MetaStore {
        &*self.meta_store
    }

    pub fn meta_store_ref(&self) -> MetaStoreRef {
        self.meta_store.clone()
    }

    pub fn id_gen_manager_ref(&self) -> IdGeneratorManagerRef {
        self.id_gen_manager.clone()
    }

    pub fn epoch_generator(&self) -> &dyn EpochGenerator {
        &*self.epoch_generator
    }

    pub fn epoch_generator_ref(&self) -> EpochGeneratorRef {
        self.epoch_generator.clone()
    }

    pub fn stream_clients_ref(&self) -> StreamClientsRef {
        self.stream_clients.clone()
    }
}
