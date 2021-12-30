use std::sync::Arc;

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
}

impl MetaSrvEnv {
    pub async fn new(
        config: Arc<Config>,
        meta_store: MetaStoreRef,
        epoch_generator: EpochGeneratorRef,
    ) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);

        Self {
            config,
            id_gen_manager,
            meta_store,
            epoch_generator,
        }
    }

    // Instance for test.
    #[cfg(test)]
    pub async fn for_test() -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::new());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());

        Self {
            config: Default::default(),
            id_gen_manager,
            meta_store,
            epoch_generator,
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
}
