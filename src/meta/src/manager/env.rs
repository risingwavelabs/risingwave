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

use std::ops::Deref;
use std::sync::Arc;

use super::{StreamClients, StreamClientsRef};
#[cfg(any(test, feature = "test"))]
use crate::manager::MemEpochGenerator;
use crate::manager::{
    EpochGenerator, EpochGeneratorRef, IdGeneratorManager, IdGeneratorManagerRef,
    NotificationManager, NotificationManagerRef,
};
#[cfg(any(test, feature = "test"))]
use crate::storage::MemStore;
use crate::storage::MetaStore;

/// [`MetaSrvEnv`] is the global environment in Meta service. The instance will be shared by all
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

    /// notification manager.
    notification_manager: NotificationManagerRef,

    /// stream clients memorization.
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
        let notification_manager = Arc::new(NotificationManager::new(epoch_generator.clone()));

        Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            notification_manager,
            stream_clients,
        }
    }

    pub fn meta_store_ref(&self) -> Arc<S> {
        self.meta_store.clone()
    }

    pub fn meta_store(&self) -> &S {
        self.meta_store.deref()
    }

    pub fn id_gen_manager_ref(&self) -> IdGeneratorManagerRef<S> {
        self.id_gen_manager.clone()
    }

    pub fn id_gen_manager(&self) -> &IdGeneratorManager<S> {
        self.id_gen_manager.deref()
    }

    pub fn epoch_generator_ref(&self) -> EpochGeneratorRef {
        self.epoch_generator.clone()
    }

    pub fn epoch_generator(&self) -> &dyn EpochGenerator {
        self.epoch_generator.deref()
    }

    pub fn notification_manager_ref(&self) -> NotificationManagerRef {
        self.notification_manager.clone()
    }

    pub fn notification_manager(&self) -> &NotificationManager {
        self.notification_manager.deref()
    }

    pub fn stream_clients_ref(&self) -> StreamClientsRef {
        self.stream_clients.clone()
    }

    pub fn stream_clients(&self) -> &StreamClients {
        self.stream_clients.deref()
    }
}

#[cfg(any(test, feature = "test"))]
impl MetaSrvEnv<MemStore> {
    // Instance for test.
    pub async fn for_test() -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::default());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let epoch_generator = Arc::new(MemEpochGenerator::new());
        let notification_manager = Arc::new(NotificationManager::new(epoch_generator.clone()));
        let stream_clients = Arc::new(StreamClients::default());

        Self {
            id_gen_manager,
            meta_store,
            epoch_generator,
            notification_manager,
            stream_clients,
        }
    }
}
