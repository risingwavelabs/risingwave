use std::cmp::Ordering::{Equal, Greater, Less};
use std::sync::Arc;

use async_trait::async_trait;

use super::variants::*;
use crate::hummock::iterator::HummockIterator;
use crate::hummock::value::HummockValue;
use crate::hummock::version_cmp::VersionedComparator;
use crate::hummock::{HummockResult, SSTable, SSTableIteratorType};

/// Serves as the concrete implementation of `ConcatIterator` and `ReverseConcatIterator`.
pub struct ConcatIteratorInner<TI: SSTableIteratorType> {
    /// The iterator of the current table.
    sstable_iter: Option<TI::SSTableIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    tables: Vec<Arc<SSTable>>,
}

impl<TI: SSTableIteratorType> ConcatIteratorInner<TI> {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a reverse iterator.
    pub fn new(tables: Vec<Arc<SSTable>>) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            tables,
        }
    }

    /// Seek to a table, and then seek to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.tables.len() {
            self.sstable_iter = None;
        } else {
            let mut sstable_iter = TI::new(self.tables[idx].clone());
            if let Some(key) = seek_key {
                sstable_iter.seek(key).await?;
            } else {
                sstable_iter.rewind().await?;
            }

            self.sstable_iter = Some(sstable_iter);
            self.cur_idx = idx;
        }
        Ok(())
    }
}

#[async_trait]
impl<TI: SSTableIteratorType> HummockIterator for ConcatIteratorInner<TI> {
    async fn next(&mut self) -> HummockResult<()> {
        let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");
        sstable_iter.next().await?;

        if sstable_iter.is_valid() {
            Ok(())
        } else {
            // seek to next table
            self.seek_idx(self.cur_idx + 1, None).await
        }
    }

    fn key(&self) -> &[u8] {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let table_idx = self
            .tables
            .partition_point(|table| match TI::DIRECTION {
                FORWARD => {
                    let ord = VersionedComparator::compare_key(&table.meta.smallest_key, key);
                    ord == Less || ord == Equal
                }
                BACKWARD => {
                    let ord = VersionedComparator::compare_key(&table.meta.largest_key, key);
                    ord == Greater || ord == Equal
                }
                _ => unreachable!(),
            })
            .saturating_sub(1); // considering the boundary of 0

        self.seek_idx(table_idx, Some(key)).await?;
        if !self.is_valid() {
            // seek to next block
            self.seek_idx(table_idx + 1, None).await?;
        }
        Ok(())
    }
}
