use std::cmp::Ordering::{Equal, Less};
use std::sync::Arc;

use async_trait::async_trait;

use super::super::{HummockResult, HummockValue};
use super::{BlockIterator, SSTable, SeekPos};
use crate::hummock::iterator::variants::FORWARD;
use crate::hummock::iterator::HummockIterator;
use crate::hummock::version_cmp::VersionedComparator;

pub trait SSTableIteratorBase: HummockIterator {}

pub trait SSTableIteratorType {
    type SSTableIterator: SSTableIteratorBase;
    const DIRECTION: usize;

    fn new(table: Arc<SSTable>) -> Self::SSTableIterator;
}

/// Iterates on a table.
pub struct SSTableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the table
    pub table: Arc<SSTable>,
}

impl SSTableIterator {
    pub fn new(table: Arc<SSTable>) -> Self {
        Self {
            block_iter: None,
            cur_idx: 0,
            table,
        }
    }

    /// Seek to a block, and then seek to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        tracing::trace!(
            target: "events::storage::sstable::block_seek",
            "table iterator seek: table_id = {}, block_id = {}",
            self.table.id,
            idx,
        );
        if idx >= self.table.block_count() {
            self.block_iter = None;
        } else {
            let mut block_iter = BlockIterator::new(self.table.block(idx).await?);
            if let Some(key) = seek_key {
                block_iter.seek(key, SeekPos::Origin);
            } else {
                block_iter.seek_to_first();
            }

            self.block_iter = Some(block_iter);
            self.cur_idx = idx;
        }

        Ok(())
    }
}

#[async_trait]
impl HummockIterator for SSTableIterator {
    async fn next(&mut self) -> HummockResult<()> {
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();

        if block_iter.is_valid() {
            Ok(())
        } else {
            // seek to next block
            self.seek_idx(self.cur_idx + 1, None).await
        }
    }

    fn key(&self) -> &[u8] {
        self.block_iter
            .as_ref()
            .expect("no block iter")
            .key()
            .expect("invalid iter")
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self
            .block_iter
            .as_ref()
            .expect("no block iter")
            .value()
            .expect("invalid iter");

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let block_idx = self
            .table
            .meta
            .block_metas
            .partition_point(|block_meta| {
                // compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `block`, thus the
                // partition point should be `prev(<=)` instead of `<`.
                let ord = VersionedComparator::compare_key(block_meta.smallest_key.as_slice(), key);
                ord == Less || ord == Equal
            })
            .saturating_sub(1); // considering the boundary of 0

        self.seek_idx(block_idx, Some(key)).await?;
        if !self.is_valid() {
            // seek to next block
            self.seek_idx(block_idx + 1, None).await?;
        }

        Ok(())
    }
}

impl SSTableIteratorBase for SSTableIterator {}

impl SSTableIteratorType for SSTableIterator {
    type SSTableIterator = SSTableIterator;
    const DIRECTION: usize = FORWARD;

    fn new(table: Arc<SSTable>) -> Self::SSTableIterator {
        SSTableIterator::new(table)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::*;

    use super::super::builder::tests::*;
    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::key::key_with_epoch;
    use crate::hummock::sstable::builder::tests::gen_test_sstable;

    #[tokio::test]
    async fn test_table_iterator() {
        // build remote table
        let table = gen_test_sstable(default_builder_opt_for_test()).await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.block_metas.len() > 10);

        let mut sstable_iter = SSTableIterator::new(Arc::new(table));
        let mut cnt = 0;
        sstable_iter.rewind().await.unwrap();

        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_bytes_eq!(key, builder_test_key_of(cnt));
            assert_bytes_eq!(value.into_put_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }

        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_table_seek() {
        let table = gen_test_sstable(default_builder_opt_for_test()).await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.block_metas.len() > 10);
        let table = Arc::new(table);
        let mut sstable_iter = SSTableIterator::new(table.clone());
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            sstable_iter.seek(&builder_test_key_of(i)).await.unwrap();
            // sstable_iter.next().await.unwrap();
            let key = sstable_iter.key();
            assert_bytes_eq!(key, builder_test_key_of(i));
        }

        // Seek to key #500 and start iterating.
        sstable_iter.seek(&builder_test_key_of(500)).await.unwrap();
        for i in 500..TEST_KEYS_COUNT {
            let key = sstable_iter.key();
            assert_eq!(key, builder_test_key_of(i));
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());

        // Seek to < first key

        let smallest_key = key_with_epoch(format!("key_aaaa_{:05}", 0).as_bytes().to_vec(), 233);
        sstable_iter.seek(smallest_key.as_slice()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, builder_test_key_of(0));

        // Seek to > last key
        let largest_key = key_with_epoch(format!("key_zzzz_{:05}", 0).as_bytes().to_vec(), 233);
        sstable_iter.seek(largest_key.as_slice()).await.unwrap();
        assert!(!sstable_iter.is_valid());

        // Seek to non-existing key
        for idx in 1..TEST_KEYS_COUNT {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            sstable_iter
                .seek(
                    key_with_epoch(
                        format!("key_test_{:05}", idx * 2 - 1).as_bytes().to_vec(),
                        0,
                    )
                    .as_slice(),
                )
                .await
                .unwrap();

            let key = sstable_iter.key();
            assert_eq!(key, builder_test_key_of(idx));
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());
    }
}
