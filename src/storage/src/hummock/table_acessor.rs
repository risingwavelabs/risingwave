use risingwave_hummock_sdk::HummockSSTableId;

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

#[async_trait::async_trait]
pub trait TableAcessor: Clone + Sync + Send {
    async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder>;
}

#[derive(Clone)]
pub struct StorageTableAcessor {
    store: SstableStoreRef,
}

impl StorageTableAcessor {
    pub fn new(store: SstableStoreRef) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl TableAcessor for StorageTableAcessor {
    async fn sstable(
        &self,
        sst_id: HummockSSTableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.store.load_table(sst_id, stats, true).await
    }
}
