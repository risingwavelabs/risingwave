use std::sync::Arc;

use risingwave_common::error::Result;

use super::{StreamClients, StreamClientsRef};
#[cfg(test)]
use crate::manager::MemEpochGenerator;
use crate::manager::{
    EpochGenerator, EpochGeneratorRef, IdGeneratorManager, IdGeneratorManagerRef,
    StoredCatalogManager, StoredCatalogManagerRef,
};
#[cfg(test)]
use crate::storage::MemStore;
use crate::storage::{MetaStore, MetaStoreRef};

/// [`MetaSrcEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv {
    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef,
    /// meta store.
    meta_store: MetaStoreRef,
    /// epoch generator.
    epoch_generator: EpochGeneratorRef,
    /// stream clients memoization.
    stream_clients: StreamClientsRef,
    // catalog manager.
    stored_catalog_manager: StoredCatalogManagerRef,
}

impl MetaSrvEnv {
    pub async fn new(meta_store: MetaStoreRef, epoch_generator: EpochGeneratorRef) -> Result<Self> {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let stream_clients = Arc::new(StreamClients::default());
        let stored_catalog_manager = Arc::new(StoredCatalogManager::new(meta_store.clone()).await?);

        Ok(Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
            stored_catalog_manager,
        })
    }

    // Instance for test.
    #[cfg(test)]
    pub async fn for_test() -> Result<Self> {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::new());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());
        let stored_catalog_manager = Arc::new(StoredCatalogManager::new(meta_store.clone()).await?);

        Ok(Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
            stored_catalog_manager,
        })
    }

    // Instance for test using sled as backend
    #[cfg(test)]
    pub async fn for_test_with_sled(sled_root: impl AsRef<std::path::Path>) -> Result<Self> {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store =
            Arc::new(crate::storage::SledMetaStore::new(Some(sled_root.as_ref())).unwrap());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let stream_clients = Arc::new(StreamClients::default());
        let stored_catalog_manager = Arc::new(StoredCatalogManager::new(meta_store.clone()).await?);

        Ok(Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            stream_clients,
            stored_catalog_manager,
        })
    }

    #[allow(dead_code)]
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

    pub fn stored_catalog_manager_ref(&self) -> StoredCatalogManagerRef {
        self.stored_catalog_manager.clone()
    }
}
