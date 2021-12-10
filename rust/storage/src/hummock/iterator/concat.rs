use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::key_range::VersionComparator;
use crate::hummock::table::{BlockIterator, Table, TableIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;

pub struct ConcatIterator {
    tables: Vec<Arc<Table>>,
    table_count: usize,
    cur_iter: Option<TableIterator>,
    cur_table_idx: usize,
}

impl ConcatIterator {
    pub fn new(tables: Vec<Arc<Table>>) -> Self {
        let table_count = tables.len();
        Self {
            tables,
            table_count,
            cur_iter: None,
            cur_table_idx: 0,
        }
    }
    async fn seek_inner(&mut self, key: &[u8]) -> HummockResult<()> {
        let first_keys = futures::future::join_all(self.tables.iter().map(|x| x.block(0))).await;
        let first_keys = first_keys.into_iter().map(|e| e.unwrap()).collect_vec();
        let mut nth_table = first_keys.partition_point(|block| {
            use std::cmp::Ordering::Less;
            let mut block_iter = BlockIterator::new(block.clone());
            block_iter.seek_to_first();
            let (block_key, _) = block_iter.data().unwrap();

            // compare by version comparator
            VersionComparator::compare_key(block_key, key) == Less
        });
        if nth_table > 0 {
            nth_table -= 1
        }
        self.set_iter(nth_table).await?;
        self.cur_iter.as_mut().unwrap().seek(key).await?;
        Ok(())
    }

    async fn set_iter(&mut self, table_idx: usize) -> HummockResult<()> {
        let mut iter = TableIterator::new(self.tables[table_idx].clone());
        iter.rewind().await?;
        self.cur_iter = Some(iter);
        self.cur_table_idx = table_idx;
        Ok(())
    }
    async fn rewind_inner(&mut self) -> HummockResult<()> {
        self.set_iter(0).await
    }

    async fn next_inner(&mut self) -> HummockResult<Option<(&[u8], HummockValue<&[u8]>)>> {
        if self.cur_iter.is_none() {
            // call the iterator without rewinding
            self.rewind_inner().await?;
        }
        let valid = self.cur_iter.as_mut().unwrap().is_valid();
        if valid {
            return self.cur_iter.as_mut().unwrap().next().await;
        }

        if self.cur_table_idx + 1 < self.table_count {
            // if still remains tables to iterate.
            self.set_iter(self.cur_table_idx + 1).await?;
            self.cur_iter.as_mut().unwrap().next().await
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl HummockIterator for ConcatIterator {
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
    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_test_table, iterator_test_key_of, test_value_of,
        TEST_KEYS_COUNT,
    };

    #[tokio::test]
    async fn hello() {
        let table0 = gen_test_table(0, default_builder_opt_for_test()).await;
        let table1 = gen_test_table(1, default_builder_opt_for_test()).await;
        let table2 = gen_test_table(2, default_builder_opt_for_test()).await;
        let mut iter =
            ConcatIterator::new(vec![Arc::new(table0), Arc::new(table1), Arc::new(table2)]);
        let mut i = 0;
        iter.rewind().await.unwrap();
        loop {
            let table_idx = i / TEST_KEYS_COUNT;
            let (key, val) = iter.next().await.unwrap().unwrap();
            assert_eq!(
                key,
                iterator_test_key_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            assert_eq!(
                val.into_put_value().unwrap(),
                test_value_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            i += 1;
            if i == TEST_KEYS_COUNT * 3 {
                assert!(iter.next().await.unwrap().is_none());
                break;
            }
        }

        iter.rewind().await.unwrap();
        let (k, v) = iter.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 0).as_slice());
    }

    #[tokio::test]
    async fn seek_test() {
        let table0 = gen_test_table(0, default_builder_opt_for_test()).await;
        let table1 = gen_test_table(1, default_builder_opt_for_test()).await;
        let table2 = gen_test_table(2, default_builder_opt_for_test()).await;
        let mut iter =
            ConcatIterator::new(vec![Arc::new(table0), Arc::new(table1), Arc::new(table2)]);

        // Middle normal case
        iter.seek(iterator_test_key_of(1, 1).as_slice())
            .await
            .unwrap();
        let (k, v) = iter.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(1, 1).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(1, 1).as_slice());

        // Left edge case
        iter.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        let (k, v) = iter.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(v.into_put_value().unwrap(), test_value_of(0, 0).as_slice());

        // Right edge case
        iter.seek(iterator_test_key_of(2, TEST_KEYS_COUNT - 1).as_slice())
            .await
            .unwrap();

        let (k, v) = iter.next().await.unwrap().unwrap();
        assert_eq!(k, iterator_test_key_of(2, TEST_KEYS_COUNT - 1).as_slice());
        assert_eq!(
            v.into_put_value().unwrap(),
            test_value_of(2, TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(4, 10).as_slice())
            .await
            .unwrap();
        let res = iter.next().await.unwrap();
        assert!(res.is_none());
    }
}
