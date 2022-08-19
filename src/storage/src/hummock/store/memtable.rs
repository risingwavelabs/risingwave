use std::ops::RangeBounds;

use bytes::Bytes;

pub trait Memtable: Send + Sync + 'static + Clone {
    type Iter;

    /// Inserts a key-value entry associated with a given `epoch` into memtable.
    fn insert(&mut self, key: Bytes, val: Bytes, epoch: u64);

    /// Deletes a key-value entry from memtable. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&mut self, key: Bytes, epoch: u64);

    /// Point gets a value from memtable.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get(&self, key: &u8, epoch: u64) -> Option<Bytes>;

    /// Opens and returns an iterator for a given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter<'a, R, B>(&self, key_range: R) -> Self::Iter
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;
}
