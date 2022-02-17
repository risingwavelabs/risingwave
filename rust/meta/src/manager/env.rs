use std::sync::Arc;

use super::{StreamClients, StreamClientsRef};
#[cfg(test)]
use crate::manager::MemEpochGenerator;
use crate::manager::{EpochGeneratorRef, IdGeneratorManager, IdGeneratorManagerRef};
use crate::storage::MetaStore;
#[cfg(test)]
use crate::storage::{MemStore, SledMetaStore};

/// [`MetaSrcEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv<S>
where
    S: MetaStore,
{
    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef<S>,
    /// meta store.
    meta_store: Arc<S>,
    /// epoch generator.
    epoch_generator: EpochGeneratorRef,
    /// stream clients memoization.
    stream_clients: StreamClientsRef,
}

impl<S> MetaSrvEnv<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>, epoch_generator: EpochGeneratorRef) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }

    pub fn meta_store_ref(&self) -> Arc<S> {
        self.meta_store.clone()
    }

    pub fn id_gen_manager_ref(&self) -> IdGeneratorManagerRef<S> {
        self.id_gen_manager.clone()
    }

    pub fn epoch_generator_ref(&self) -> EpochGeneratorRef {
        self.epoch_generator.clone()
    }

    pub fn stream_clients_ref(&self) -> StreamClientsRef {
        self.stream_clients.clone()
    }
}

#[cfg(test)]
impl MetaSrvEnv<MemStore> {
    // Instance for test.
    pub async fn for_test() -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::new());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }
}

#[cfg(test)]
impl MetaSrvEnv<SledMetaStore> {
    // Instance for test using sled as backend
    #[cfg(test)]
    pub async fn for_test_with_sled() -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(SledMetaStore::new(None).unwrap());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
        }
    }
}
