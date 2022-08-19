use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use tokio::sync::mpsc;

use super::memtable::Memtable;
use super::version::{HummockReadVersion, OrderIdx, StagingData, VersionUpdate};
use super::write_queue::HummockWriteQueueItem;
use super::{
    EmptyFutureTrait, FlushFutureTrait, GetFutureTrait, IterFutureTrait, ReadOptions, StateStore,
};
use crate::define_local_state_store_associated_type;
use crate::hummock::{HummockResult, HummockStateStoreIter};

#[allow(unused)]
pub struct HummockStorageCore<M>
where
    M: Memtable,
{
    /// Mutable memtable.
    memtable: M,

    /// Read handle.
    read_version: HummockReadVersion<M>,

    /// Write handle.
    queue_writer: mpsc::Sender<HummockWriteQueueItem<M>>,
}

#[allow(unused)]
#[derive(Clone)]
pub struct HummockStorage<M>
where
    M: Memtable,
{
    core: Arc<Mutex<HummockStorageCore<M>>>,
}

#[allow(unused)]
impl<M> HummockStorage<M>
where
    M: Memtable,
{
    /// See `HummockReadVersion::update_committed` for more details.
    pub fn update_committed(&mut self, info: VersionUpdate) -> HummockResult<()> {
        unimplemented!()
    }

    /// See `HummockReadVersion::update_staging` for more details.
    pub fn update_staging(&mut self, info: StagingData<M>, idx: OrderIdx) -> HummockResult<()> {
        unimplemented!()
    }
}

#[allow(unused)]
impl<M> StateStore for HummockStorage<M>
where
    M: Memtable,
{
    type Iter = HummockStateStoreIter;

    define_local_state_store_associated_type!();

    fn insert(&self, key: Bytes, val: Bytes, epoch: u64) -> Self::InsertFuture<'_> {
        async move { unimplemented!() }
    }

    fn delete(&self, key: Bytes, epoch: u64) -> Self::DeleteFuture<'_> {
        async move { unimplemented!() }
    }

    fn get(&self, key: &[u8], epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(
        &self,
        key_range: R,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn flush(&self) -> Self::FlushFuture<'_> {
        async move { unimplemented!() }
    }
}
