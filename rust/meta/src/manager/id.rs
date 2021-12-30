use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::get_id_request::IdCategory;
use tokio::sync::RwLock;

use crate::manager::SINGLE_VERSION_EPOCH;
use crate::storage::MetaStoreRef;

pub const ID_PREALLOCATE_INTERVAL: i32 = 1000;

type Id = i32;

#[async_trait::async_trait]
pub trait IdGenerator: Sync + Send {
    /// Generate a batch of identities.
    /// The valid id range will be [result_id, result_id + interval)
    async fn generate_interval(&self, interval: i32) -> Result<Id>;

    /// Generate an identity.
    async fn generate(&self) -> Result<Id> {
        self.generate_interval(1).await
    }
}

pub type IdGeneratorRef = Box<dyn IdGenerator>;

/// [`StoredIdGenerator`] implements id generator using metastore.
pub struct StoredIdGenerator {
    meta_store_ref: MetaStoreRef,
    category_gen_key: String,
    current_id: AtomicI32,
    next_allocate_id: RwLock<Id>,
}

impl StoredIdGenerator {
    pub async fn new(meta_store_ref: MetaStoreRef, category: &str, start: Option<Id>) -> Self {
        let category_gen_key = format!("{}_id_next_generator", category);
        let res = meta_store_ref
            .get(category_gen_key.as_bytes(), SINGLE_VERSION_EPOCH)
            .await;
        let current_id = match res {
            Ok(value) => i32::from_be_bytes(value.as_slice().try_into().unwrap()),
            Err(err) => {
                if !matches!(err.inner(), ErrorCode::ItemNotFound(_)) {
                    panic!("{}", err)
                }
                start.unwrap_or(0)
            }
        };

        let next_allocate_id = current_id + ID_PREALLOCATE_INTERVAL;
        if let Err(err) = meta_store_ref
            .put(
                category_gen_key.as_bytes(),
                &next_allocate_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
        {
            panic!("{}", err)
        }

        StoredIdGenerator {
            meta_store_ref,
            category_gen_key,
            current_id: AtomicI32::new(current_id),
            next_allocate_id: RwLock::new(next_allocate_id),
        }
    }
}

#[async_trait::async_trait]
impl IdGenerator for StoredIdGenerator {
    async fn generate_interval(&self, interval: i32) -> Result<Id> {
        let id = self.current_id.fetch_add(interval, Ordering::Relaxed);
        let next_allocate_id = { *self.next_allocate_id.read().await };
        if id + interval > next_allocate_id {
            let mut next = self.next_allocate_id.write().await;
            if id + interval > *next {
                let weight = num_integer::Integer::div_ceil(
                    &(id + interval - *next),
                    &ID_PREALLOCATE_INTERVAL,
                );
                let next_allocate_id = *next + ID_PREALLOCATE_INTERVAL * weight;
                self.meta_store_ref
                    .put(
                        self.category_gen_key.as_bytes(),
                        &next_allocate_id.to_be_bytes(),
                        SINGLE_VERSION_EPOCH,
                    )
                    .await?;
                *next = next_allocate_id;
            }
        }

        Ok(id)
    }
}

pub type IdGeneratorManagerRef = Arc<IdGeneratorManager>;

/// [`IdGeneratorManager`] manages id generators in all categories,
/// which defined as [`IdCategory`] in [`meta.proto`].
pub struct IdGeneratorManager {
    inner: HashMap<IdCategory, IdGeneratorRef>,
}

impl IdGeneratorManager {
    pub async fn new(meta_store_ref: MetaStoreRef) -> Self {
        let mut inner = HashMap::new();
        for (category, name, start) in [
            (IdCategory::Default, "default", None),
            (IdCategory::Database, "database", None),
            (IdCategory::Schema, "schema", None),
            (IdCategory::Table, "table", None),
            (IdCategory::Worker, "worker", None),
            (IdCategory::Fragment, "fragment", Some(1)),
            (IdCategory::HummockContext, "hummock_context", Some(1)),
            (IdCategory::HummockSnapshot, "hummock_snapshot", Some(1)),
        ] {
            inner.insert(
                category,
                Box::new(StoredIdGenerator::new(meta_store_ref.clone(), name, start).await)
                    as IdGeneratorRef,
            );
        }

        // Return the manager.
        IdGeneratorManager { inner }
    }

    /// [`generate`] function generates id as `current_id`.
    pub async fn generate(&self, category: IdCategory) -> Result<Id> {
        self.inner.get(&category).unwrap().generate().await
    }

    /// [`generate_interval`] function generates ids as [`current_id`, `current_id` + interval), the
    /// next id will be `current_id` + interval.
    pub async fn generate_interval(&self, category: IdCategory, interval: i32) -> Result<Id> {
        self.inner
            .get(&category)
            .unwrap()
            .generate_interval(interval)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::future;

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_id_generator() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let id_generator = StoredIdGenerator::new(meta_store_ref.clone(), "default", None).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let id_generator_two =
            StoredIdGenerator::new(meta_store_ref.clone(), "default", None).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator_two;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (10000..20000).collect::<Vec<_>>());

        let id_generator_three =
            StoredIdGenerator::new(meta_store_ref.clone(), "table", None).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator_three;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let fragment_id_generator =
            StoredIdGenerator::new(meta_store_ref.clone(), "fragment", Some(1)).await;
        let ids = future::join_all((0..100).map(|_i| {
            let id_generator = &fragment_id_generator;
            async move { id_generator.generate_interval(100).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let vec_expect = (0..100).map(|e| e * 100 + 1).collect::<Vec<_>>();
        assert_eq!(ids, vec_expect);

        let fragment_id_generator_two =
            StoredIdGenerator::new(meta_store_ref, "fragment", None).await;
        let ids = future::join_all((0..100).map(|_i| {
            let id_generator = &fragment_id_generator_two;
            async move { id_generator.generate_interval(10).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let vec_expect = (0..100).map(|e| 10001 + e * 10).collect::<Vec<_>>();
        assert_eq!(ids, vec_expect);

        Ok(())
    }

    #[tokio::test]
    async fn test_id_generator_manager() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let manager = IdGeneratorManager::new(meta_store_ref.clone()).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let manager = &manager;
            async move { manager.generate(IdCategory::Default).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let ids = future::join_all((0..10000).map(|_i| {
            let manager = &manager;
            async move { manager.generate(IdCategory::Table).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let ids = future::join_all((0..100).map(|_i| {
            let manager = &manager;
            async move { manager.generate_interval(IdCategory::Fragment, 9999).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        let vec_expect = (0..100).map(|e| e * 9999 + 1).collect::<Vec<_>>();
        assert_eq!(ids, vec_expect);

        let manager = IdGeneratorManager::new(meta_store_ref).await;
        let id = manager.generate_interval(IdCategory::Fragment, 10).await?;
        assert_eq!(id, 1000001);

        Ok(())
    }
}
