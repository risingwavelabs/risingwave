use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use risingwave_common::error::{ErrorCode, Result};
use tokio::sync::RwLock;

use crate::manager::SINGLE_VERSION_EPOCH;
use crate::storage::MetaStoreRef;

pub const ID_PREALLOCATE_INTERVAL: i32 = 1000;

pub type Id = i32;

#[async_trait::async_trait]
pub trait IdGenerator: Sync + Send + 'static {
    /// Generate a batch of identities.
    /// The valid id range will be [result_id, result_id + interval)
    async fn generate_interval(&self, interval: i32) -> Result<Id>;

    /// Generate an identity.
    async fn generate(&self) -> Result<Id> {
        self.generate_interval(1).await
    }
}

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

type IdCategoryType = u8;

// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/44580) get completed.
#[allow(non_snake_case, non_upper_case_globals)]
pub mod IdCategory {
    use super::IdCategoryType;

    #[cfg(test)]
    pub const Test: IdCategoryType = 0;
    pub const Database: IdCategoryType = 1;
    pub const Schema: IdCategoryType = 2;
    pub const Table: IdCategoryType = 3;
    pub const Worker: IdCategoryType = 4;
    pub const Actor: IdCategoryType = 5;
    pub const HummockSnapshot: IdCategoryType = 7;
    pub const HummockSSTableId: IdCategoryType = 8;
}

pub type IdGeneratorManagerRef = Arc<IdGeneratorManager>;

/// [`IdGeneratorManager`] manages id generators in all categories,
/// which defined as [`IdCategory`] in [`meta.proto`].
pub struct IdGeneratorManager {
    #[cfg(test)]
    test: Arc<StoredIdGenerator>,
    database: Arc<StoredIdGenerator>,
    schema: Arc<StoredIdGenerator>,
    table: Arc<StoredIdGenerator>,
    worker: Arc<StoredIdGenerator>,
    actor: Arc<StoredIdGenerator>,
    hummock_snapshot: Arc<StoredIdGenerator>,
    hummock_ss_table_id: Arc<StoredIdGenerator>,
}

impl IdGeneratorManager {
    pub async fn new(meta_store_ref: MetaStoreRef) -> Self {
        Self {
            #[cfg(test)]
            test: Arc::new(StoredIdGenerator::new(meta_store_ref.clone(), "test", None).await),
            database: Arc::new(
                StoredIdGenerator::new(meta_store_ref.clone(), "database", None).await,
            ),
            schema: Arc::new(StoredIdGenerator::new(meta_store_ref.clone(), "schema", None).await),
            table: Arc::new(StoredIdGenerator::new(meta_store_ref.clone(), "table", None).await),
            worker: Arc::new(StoredIdGenerator::new(meta_store_ref.clone(), "worker", None).await),
            actor: Arc::new(StoredIdGenerator::new(meta_store_ref.clone(), "actor", Some(1)).await),
            hummock_snapshot: Arc::new(
                StoredIdGenerator::new(meta_store_ref.clone(), "hummock_snapshot", Some(1)).await,
            ),
            hummock_ss_table_id: Arc::new(
                StoredIdGenerator::new(meta_store_ref.clone(), "hummock_ss_table_id", Some(1))
                    .await,
            ),
        }
    }

    const fn get<const C: IdCategoryType>(&self) -> &Arc<StoredIdGenerator> {
        match C {
            #[cfg(test)]
            IdCategory::Test => &self.test,
            IdCategory::Database => &self.database,
            IdCategory::Schema => &self.schema,
            IdCategory::Table => &self.table,
            IdCategory::Actor => &self.actor,
            IdCategory::HummockSnapshot => &self.hummock_snapshot,
            IdCategory::Worker => &self.worker,
            IdCategory::HummockSSTableId => &self.hummock_ss_table_id,
            _ => unreachable!(),
        }
    }

    /// [`generate`] function generates id as `current_id`.
    pub async fn generate<const C: IdCategoryType>(&self) -> Result<Id> {
        self.get::<C>().generate().await
    }

    /// [`generate_interval`] function generates ids as [`current_id`, `current_id` + interval), the
    /// next id will be `current_id` + interval.
    pub async fn generate_interval<const C: IdCategoryType>(&self, interval: i32) -> Result<Id> {
        self.get::<C>().generate_interval(interval).await
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

        let actor_id_generator =
            StoredIdGenerator::new(meta_store_ref.clone(), "actor", Some(1)).await;
        let ids = future::join_all((0..100).map(|_i| {
            let id_generator = &actor_id_generator;
            async move { id_generator.generate_interval(100).await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

        let vec_expect = (0..100).map(|e| e * 100 + 1).collect::<Vec<_>>();
        assert_eq!(ids, vec_expect);

        let actor_id_generator_two = StoredIdGenerator::new(meta_store_ref, "actor", None).await;
        let ids = future::join_all((0..100).map(|_i| {
            let id_generator = &actor_id_generator_two;
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
            async move { manager.generate::<{ IdCategory::Test }>().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let ids = future::join_all((0..10000).map(|_i| {
            let manager = &manager;
            async move { manager.generate::<{ IdCategory::Table }>().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let ids = future::join_all((0..100).map(|_i| {
            let manager = &manager;
            async move {
                manager
                    .generate_interval::<{ IdCategory::Actor }>(9999)
                    .await
            }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        let vec_expect = (0..100).map(|e| e * 9999 + 1).collect::<Vec<_>>();
        assert_eq!(ids, vec_expect);

        let manager = IdGeneratorManager::new(meta_store_ref).await;
        let id = manager
            .generate_interval::<{ IdCategory::Actor }>(10)
            .await?;
        assert_eq!(id, 1000001);

        Ok(())
    }
}
