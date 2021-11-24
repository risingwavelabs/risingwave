use super::{HummockResult, HummockValue};

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
    async fn next(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>>;

    /// Reset the position of the iterator
    async fn rewind(&mut self) -> HummockResult<()>;

    /// Seek to a key
    async fn seek(&mut self, key: &[u8]) -> HummockResult<()>;
}

pub enum HummockIteratorImpl {
    // Will support later
    // Table(TableIterator),
    Concat(ConcatIterator),
    Merge(MergeIterator),
    #[cfg(test)]
    Test(tests::TestIterator),
}
