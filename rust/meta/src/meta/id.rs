use crate::meta::{MetaStoreRef, SINGLE_VERSION_EPOCH};
use risingwave_common::error::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use risingwave_pb::meta::get_id_request::IdCategory;
use tokio::sync::RwLock;

pub const ID_PREALLOCATE_INTERVAL: i32 = 1000;

type Id = i32;

#[async_trait::async_trait]
pub trait IdGenerator: Sync + Send {
    async fn generate(&self) -> Result<Id>;
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
    pub async fn new(meta_store_ref: MetaStoreRef, category: &str) -> Self {
        let category_gen_key = format!("{}_id_next_generator", category);
        let res = meta_store_ref
            .get(category_gen_key.as_bytes(), SINGLE_VERSION_EPOCH)
            .await;
        let current_id = match res {
            Ok(value) => i32::from_be_bytes(value.as_slice().try_into().unwrap()),
            Err(err) => {
                if err.to_grpc_status().code() != tonic::Code::NotFound {
                    panic!("{}", err)
                }
                0
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
    async fn generate(&self) -> Result<Id> {
        let id = self.current_id.fetch_add(1, Ordering::Relaxed);
        let next_allocate_id = { *self.next_allocate_id.read().await };
        if id >= next_allocate_id {
            let mut next = self.next_allocate_id.write().await;
            if id >= *next {
                let next_allocate_id = *next + ID_PREALLOCATE_INTERVAL;
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

/// [`IdGeneratorManager`] manages id generators in all categories,
/// which defined as [`IdCategory`] in [`meta.proto`].
pub struct IdGeneratorManager {
    inner: HashMap<IdCategory, IdGeneratorRef>,
}

impl IdGeneratorManager {
    pub async fn new(meta_store_ref: MetaStoreRef) -> Self {
        let mut inner = HashMap::new();
        for (category, name) in [
            (IdCategory::Default, "default"),
            (IdCategory::Database, "database"),
            (IdCategory::Schema, "schema"),
            (IdCategory::Table, "table"),
            (IdCategory::Fragment, "fragment"),
        ] {
            inner.insert(
                category,
                Box::new(StoredIdGenerator::new(meta_store_ref.clone(), name).await)
                    as IdGeneratorRef,
            );
        }

        IdGeneratorManager { inner }
    }

    pub async fn generate(&self, category: IdCategory) -> Result<Id> {
        self.inner.get(&category).unwrap().generate().await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::meta::MemStore;
    use futures::future;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_id_generator() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let id_generator = StoredIdGenerator::new(meta_store_ref.clone(), "default").await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let id_generator_two = StoredIdGenerator::new(meta_store_ref.clone(), "default").await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator_two;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (10000..20000).collect::<Vec<_>>());

        let id_generator_three = StoredIdGenerator::new(meta_store_ref, "table").await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator_three;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        Ok(())
    }

    #[tokio::test]
    async fn test_id_generator_manager() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let manager = IdGeneratorManager::new(meta_store_ref).await;
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

        Ok(())
    }
}
