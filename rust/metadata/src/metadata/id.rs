use crate::metadata::{MetaStoreRef, SINGLE_VERSION_EPOCH};
use risingwave_common::error::Result;
use std::sync::atomic::{AtomicI32, Ordering};

use tokio::sync::RwLock;

const IDGEN_NEXT_START_KEY: &[u8] = "key_for_id_generator".as_bytes();

pub const ID_PREALLOCATE_INTERVAL: i32 = 1000;

type Id = i32;

#[async_trait::async_trait]
pub trait IdGenerator: Sync + Send {
    async fn generate(&self) -> Result<Id>;
}

pub type IdGeneratorRef = Box<dyn IdGenerator>;

/// `StoredIdGenerator` implements id generator using metastore.
pub struct StoredIdGenerator {
    meta_store_ref: MetaStoreRef,
    current_id: AtomicI32,
    next_allocate_id: RwLock<Id>,
}

impl StoredIdGenerator {
    pub async fn new(meta_store_ref: MetaStoreRef) -> Self {
        let res = meta_store_ref
            .get(IDGEN_NEXT_START_KEY, SINGLE_VERSION_EPOCH)
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
                IDGEN_NEXT_START_KEY,
                &next_allocate_id.to_be_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await
        {
            panic!("{}", err)
        }

        StoredIdGenerator {
            meta_store_ref,
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
                        IDGEN_NEXT_START_KEY,
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::metadata::MemStore;
    use futures::future;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_id_generator() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::new());
        let id_generator = StoredIdGenerator::new(meta_store_ref.clone()).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (0..10000).collect::<Vec<_>>());

        let id_generator_new = StoredIdGenerator::new(meta_store_ref).await;
        let ids = future::join_all((0..10000).map(|_i| {
            let id_generator = &id_generator_new;
            async move { id_generator.generate().await }
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(ids, (10000..20000).collect::<Vec<_>>());

        Ok(())
    }
}
