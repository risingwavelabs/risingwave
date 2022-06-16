use risingwave_hummock_sdk::HummockSSTableId;

use crate::hummock::sstable_store::TableHolder;
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
