use std::collections::HashMap;
use std::ops::Add;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prost::Message;
use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::hummock::HummockContext;
use risingwave_pb::meta::get_id_request::IdCategory;
use tokio::sync::RwLock;

use crate::manager;
use crate::manager::{IdGeneratorManagerRef, SINGLE_VERSION_EPOCH};
use crate::storage::MetaStoreRef;

#[derive(Clone)]
pub struct Config {
    // millisecond
    pub context_ttl: u64,
    // millisecond
    pub context_check_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            context_ttl: 10000,
            context_check_interval: 1000,
        }
    }
}

#[async_trait]
pub trait HummockManager: Sync + Send + 'static {
    async fn create_hummock_context(&self, group: &str) -> Result<HummockContext>;
    async fn invalidate_hummock_context(&self, context_id: i32) -> Result<()>;
    async fn refresh_hummock_context(&self, context_id: i32) -> Result<u64>;
}

pub struct DefaultHummockManager {
    meta_store_ref: MetaStoreRef,
    id_generator_manager_ref: IdGeneratorManagerRef,
    // lock order: meta_store_lock -> context_expires_at
    context_expires_at: RwLock<HashMap<i32, Instant>>,
    meta_store_lock: RwLock<()>,
    manager_config: manager::Config,
    hummock_config: Config,
}

impl DefaultHummockManager {
    pub async fn new(
        meta_store_ref: MetaStoreRef,
        id_generator_manager_ref: IdGeneratorManagerRef,
        manager_config: manager::Config,
        hummock_config: Config,
    ) -> Arc<Self> {
        let instance = Arc::new(Self {
            meta_store_ref,
            id_generator_manager_ref,
            context_expires_at: RwLock::new(HashMap::new()),
            meta_store_lock: RwLock::new(()),
            manager_config,
            hummock_config,
        });
        tokio::spawn(Self::start_hummock_context_tracker(Arc::downgrade(
            &instance,
        )));
        instance.clone().restore_hummock_context().await;
        instance
    }

    async fn restore_hummock_context(&self) {
        self.meta_store_lock.read().await;
        let hummock_context_list = self
            .meta_store_ref
            .list_cf(self.manager_config.get_hummock_context_cf())
            .await
            .unwrap();

        let mut guard = self.context_expires_at.write().await;
        hummock_context_list.iter().for_each(|v| {
            let hummock_context: HummockContext = HummockContext::decode(v.as_slice()).unwrap();
            guard.insert(
                hummock_context.identifier,
                Instant::now().add(Duration::from_millis(self.hummock_config.context_ttl)),
            );
        });
    }

    async fn start_hummock_context_tracker(weak_self: Weak<Self>) {
        loop {
            let hummock_manager_ref = weak_self.upgrade();
            if hummock_manager_ref.is_none() {
                break;
            }
            let hummock_manager = hummock_manager_ref.unwrap();
            let mut interval = tokio::time::interval(Duration::from_millis(
                hummock_manager.hummock_config.context_check_interval,
            ));
            interval.tick().await;
            interval.tick().await;
            let context_to_invalidate: Vec<i32>;
            {
                let guard = hummock_manager.context_expires_at.read().await;
                context_to_invalidate = guard
                    .iter()
                    .filter(|kv| Instant::now().saturating_duration_since(*kv.1) > Duration::ZERO)
                    .map(|kv| *kv.0)
                    .collect();
            }
            for context_id in context_to_invalidate {
                hummock_manager
                    .invalidate_hummock_context(context_id)
                    .await
                    .unwrap();
            }
        }
    }
}

#[async_trait]
impl HummockManager for DefaultHummockManager {
    /// [`DefaultHummockManager`] manages hummock related meta data.
    /// cf(hummock_context): `identifier` -> `HummockContext`
    async fn create_hummock_context(&self, group: &str) -> Result<HummockContext> {
        let context_id = self
            .id_generator_manager_ref
            .generate(IdCategory::HummockContext)
            .await?;
        let new_context = HummockContext {
            identifier: context_id,
            group: group.to_owned(),
            ttl: self.hummock_config.context_ttl,
        };
        let result;
        {
            self.meta_store_lock.write().await;
            let hummock_context_list = self
                .meta_store_ref
                .list_cf(self.manager_config.get_hummock_context_cf())
                .await;
            match hummock_context_list {
                Ok(value) => {
                    let is_group_used = value.iter().any(|v| -> bool {
                        let old_context: HummockContext =
                            HummockContext::decode(v.as_slice()).unwrap();
                        old_context.group == group
                    });
                    if is_group_used {
                        return Err(RwError::from(ErrorCode::HummockContextGroupInUse(
                            group.to_owned(),
                        )));
                    }
                }
                Err(_err) => {}
            };
            result = self
                .meta_store_ref
                .put_cf(
                    self.manager_config.get_hummock_context_cf(),
                    &new_context.identifier.to_be_bytes(),
                    &new_context.encode_to_vec(),
                    SINGLE_VERSION_EPOCH,
                )
                .await;
        }
        match result {
            Ok(()) => {
                let mut guard = self.context_expires_at.write().await;
                let expires_at = Instant::now().add(Duration::from_millis(new_context.ttl));
                guard.insert(new_context.identifier, expires_at);
                Ok(new_context)
            }
            Err(err) => Err(err),
        }
    }
    async fn invalidate_hummock_context(&self, context_id: i32) -> Result<()> {
        {
            let mut guard = self.context_expires_at.write().await;
            if guard.remove(&context_id) == None {
                return Err(RwError::from(ErrorCode::HummockContextNotFound(context_id)));
            }
        }

        self.meta_store_lock.write().await;
        self.meta_store_ref
            .delete_cf(
                self.manager_config.get_hummock_context_cf(),
                &context_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
    }

    async fn refresh_hummock_context(&self, context_id: i32) -> Result<u64> {
        let mut guard = self.context_expires_at.write().await;
        match guard.get_mut(&context_id) {
            Some(_) => {
                let new_ttl = self.hummock_config.context_ttl;
                guard.insert(
                    context_id,
                    Instant::now().add(Duration::from_millis(new_ttl)),
                );
                Ok(new_ttl)
            }
            None => Err(RwError::from(ErrorCode::HummockContextNotFound(context_id))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;

    use super::*;
    use crate::hummock;
    use crate::manager::{Config, IdGeneratorManager};
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_hummock_context_management() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let hummock_config = hummock::Config {
            context_ttl: 1000,
            context_check_interval: 300,
        };
        let hummock_manager = DefaultHummockManager::new(
            meta_store_ref.clone(),
            Arc::new(IdGeneratorManager::new(meta_store_ref.clone()).await).clone(),
            Config::default(),
            hummock_config.clone(),
        )
        .await;
        let group = "operator#1";
        let context = hummock_manager.create_hummock_context(group).await?;
        assert_eq!(context.group, group);
        let context2 = hummock_manager.create_hummock_context(group).await;
        assert!(context2.is_err());
        assert_matches!(
            context2.unwrap_err().inner(),
            ErrorCode::HummockContextGroupInUse(_)
        );
        let invalidate = hummock_manager
            .invalidate_hummock_context(context.identifier)
            .await;
        assert!(invalidate.is_ok());
        let context2 = hummock_manager.create_hummock_context(group).await?;
        assert_eq!(context2.group, group);

        tokio::time::sleep(Duration::from_millis(num_integer::Integer::div_ceil(
            &(hummock_config.context_ttl),
            &2,
        )))
        .await;

        let context2_refreshed = hummock_manager
            .refresh_hummock_context(context2.identifier)
            .await;
        assert!(context2_refreshed.is_ok());

        tokio::time::sleep(Duration::from_millis(
            hummock_config.context_ttl + hummock_config.context_check_interval,
        ))
        .await;

        let context2_refreshed = hummock_manager
            .refresh_hummock_context(context2.identifier)
            .await;
        assert!(context2_refreshed.is_err());
        Ok(())
    }
}
