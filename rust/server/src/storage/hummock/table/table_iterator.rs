use std::sync::Arc;

use super::super::{HummockIterator, HummockResult, HummockValue};
use super::{BlockIterator, SeekPos, Table};
use async_trait::async_trait;

/// Iterates on a table
pub struct TableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Whether this is the first operation after seek. If so, we need some special
    /// handling.
    first_op_after_seek: bool,

    /// Reference to the table
    table: Arc<Table>,

    /// The next block to fetch when the current block has been finished (`block_iter` becomes
    /// `None`).
    next_blk: usize,
}

impl TableIterator {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            table,
            block_iter: None,
            next_blk: 0,
            first_op_after_seek: false,
        }
    }

    /// Seek to a block and return the first value of the data.
    async fn seek_and_get(&mut self, idx: usize) -> HummockResult<(&[u8], &[u8])> {
        let mut block_iter = BlockIterator::new(self.table.block(idx).await?);
        block_iter.seek_to_first();
        self.block_iter = Some(block_iter);
        Ok(self.block_iter.as_ref().unwrap().data().unwrap())
    }

    async fn next_inner(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>> {
        let data_available = match &mut self.block_iter {
            Some(block_iter) => {
                if self.first_op_after_seek {
                    // If this is the first operation after any seek operation, we do not need to
                    // advance the block iterator. The data should be already
                    // available.
                    self.first_op_after_seek = false;
                    // It is possible that we seek to a position that a key is not available.
                    self.block_iter.as_ref().unwrap().is_valid()
                } else {
                    // Try to get the next value for the block iterator. The function will return
                    // false when we reach the end of a block.
                    block_iter.next()
                }
            }
            _ => false,
        };
        let (key, raw_value) = if data_available {
            self.block_iter.as_ref().unwrap().data().unwrap()
        } else {
            if self.next_blk >= self.table.block_count() {
                // We reached the end of the table
                return Ok(None);
            }
            let this_blk = self.next_blk;
            self.next_blk += 1;
            // Move to the next block and get the first entry in the block
            self.seek_and_get(this_blk).await?
        };
        Ok(Some((key, HummockValue::from_slice(raw_value)?)))
    }

    async fn rewind_inner(&mut self) -> HummockResult<()> {
        self.next_blk = 0;
        self.block_iter = None;
        Ok(())
    }

    async fn seek_inner(&mut self, key: &[u8]) -> HummockResult<()> {
        let block_idx = self.table.meta.offsets.partition_point(|offset| {
            use std::cmp::Ordering::Less;
            // Find the first block that seek key >= its first key, so that the seek key is within
            // the range of the previous block.
            offset.key.as_slice().partial_cmp(key) == Some(Less)
        });
        let block_idx = if block_idx > 0 { block_idx - 1 } else { 0 };

        let mut block_iter = BlockIterator::new(self.table.block(block_idx).await?);
        block_iter.seek(key, SeekPos::Origin);
        self.block_iter = Some(block_iter);
        self.first_op_after_seek = true;
        self.next_blk = block_idx + 1;

        Ok(())
    }
}

#[async_trait]
impl HummockIterator for TableIterator {
    async fn next(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>> {
        self.next_inner().await
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.rewind_inner().await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        self.seek_inner(key).await
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::*;

    use super::super::builder::tests::*;
    use super::*;
    use crate::assert_bytes_eq;
    use crate::storage::hummock::table::builder::tests::gen_test_table;

    #[tokio::test]
    async fn test_table_iterator() {
        // build remote table
        let table = gen_test_table(default_builder_opt_for_test()).await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.offsets.len() > 10);

        let mut table_iter = TableIterator::new(Arc::new(table));
        let mut cnt = 0;
        while let Some((key, value)) = table_iter.next().await.unwrap() {
            assert_bytes_eq!(key, test_key_of(cnt));
            assert_bytes_eq!(value.into_put_value().unwrap(), test_value_of(cnt));
            cnt += 1;
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_table_seek() {
        let table = gen_test_table(default_builder_opt_for_test()).await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.offsets.len() > 10);
        let table = Arc::new(table);
        let mut table_iter = TableIterator::new(table.clone());
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            table_iter.seek(&test_key_of(i)).await.unwrap();
            let (key, _) = table_iter.next().await.unwrap().unwrap();
            assert_bytes_eq!(key, test_key_of(i));
        }

        // Seek to key #500 and start iterating.
        table_iter.seek(&test_key_of(500)).await.unwrap();
        for i in 500..TEST_KEYS_COUNT {
            let (key, _) = table_iter.next().await.unwrap().unwrap();
            assert_eq!(key, test_key_of(i));
        }
        assert!(matches!(table_iter.next().await.unwrap(), None));

        // Seek to < first key
        table_iter.seek(b"key_test").await.unwrap();
        let (key, _) = table_iter.next().await.unwrap().unwrap();
        assert_eq!(key, test_key_of(0));

        // Seek to > last key
        table_iter.seek(b"key_zzzzz").await.unwrap();
        assert!(matches!(table_iter.next().await.unwrap(), None));

        // Seek to non-existing key
        table_iter.seek(&test_key_of(500)).await.unwrap();
        for idx in 1..TEST_KEYS_COUNT {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            table_iter
                .seek(format!("key_test_{:05}", idx * 2 - 1).as_bytes())
                .await
                .unwrap();

            let (key, _) = table_iter.next().await.unwrap().unwrap();
            assert_eq!(key, test_key_of(idx));
        }
        assert!(matches!(table_iter.next().await.unwrap(), None));
    }
}
