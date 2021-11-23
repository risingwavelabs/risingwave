use bytes::Bytes;

use super::HummockResult;

mod concat;
pub use concat::*;
mod merge;
pub use merge::*;
#[cfg(test)]
mod tests;

use async_trait::async_trait;

/// `HummockIterator` defines the interface of all iterators, including `TableIterator`,
/// `MergeIterator` and `ConcatIterator`.
#[async_trait]
pub trait HummockIterator {
    /// Get the next key/value pair in the table. If `None` is returned, the iterator must be
    /// rewinded to retrive new values.
    async fn next(&mut self) -> HummockResult<Option<(Bytes, Bytes)>>;

    /// Reset the position of the iterator
    async fn rewind(&mut self);

    /// Seek to a key
    async fn seek(&mut self, key: &[u8]) -> HummockResult<()>;
}

pub enum HummockIteratorImpl {
    // The basic unit of iterators is the TableIterator.
    // Table(TableIterator),
    Concat(ConcatIterator),
    Merge(MergeIterator),
    #[cfg(test)]
    Test(tests::TestIterator),
}
