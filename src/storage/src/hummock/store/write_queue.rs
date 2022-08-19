use std::sync::Arc;

use tokio::sync::mpsc;

use super::memtable::Memtable;
use super::version::OrderIdx;
use super::SyncFutureTrait;

#[allow(unused)]
pub struct HummockWriteQueueItem<M>
where
    M: Memtable,
{
    /// Immutable memtable.
    imm_mem: Arc<M>,
    /// Idx to identify immutable memtable in state store.
    idx: OrderIdx,
    /// table_id to identify table configuration for writes.
    table_id: u64,
    // TODO: may add more
}

#[allow(unused)]
pub struct HummockWriteQueue<M>
where
    M: Memtable,
{
    receiver: mpsc::Receiver<HummockWriteQueueItem<M>>,
    // TODO: may add more
}

impl<M> HummockWriteQueue<M>
where
    M: Memtable,
{
    // Question: do we need to provide an epoch or an epoch range?
    /// Forces a flush to persistent storage.
    pub fn sync(&self) -> impl SyncFutureTrait<'_> {
        async move { unimplemented!() }
    }
}
