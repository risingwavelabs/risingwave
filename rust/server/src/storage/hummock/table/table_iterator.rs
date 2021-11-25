use std::sync::Arc;

use super::super::{HummockIterator, HummockResult, HummockValue};
use super::{BlockIterator, Table};
use async_trait::async_trait;

/// Iterates on a table
pub struct TableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

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
                // Try to get the next value for the block iterator. The function will return false
                // when we reach the end of a block.
                block_iter.next()
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

    async fn seek_inner(&mut self, _key: &[u8]) -> HummockResult<()> {
        todo!()
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
    use crate::storage::hummock::table::builder::tests::gen_test_table;

    use super::super::builder::tests::*;
    use super::*;

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
            assert_eq!(key, test_key_of(cnt));
            assert_eq!(value.into_put_value().unwrap(), test_value_of(cnt));
            cnt += 1;
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
    }
}
