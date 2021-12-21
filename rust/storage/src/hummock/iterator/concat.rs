use std::cmp::Ordering::{Equal, Less};
use std::sync::Arc;

use async_trait::async_trait;

use crate::hummock::iterator::HummockIterator;
use crate::hummock::table::{Table, TableIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::version_cmp::VersionedComparator;
use crate::hummock::HummockResult;

/// Iterates on multiple non-overlapping tables.
pub struct ConcatIterator {
    // The iterator of the current table.
    table_iter: Option<TableIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    tables: Vec<Arc<Table>>,
}

impl ConcatIterator {
    /// Caller should make sure that `tables` are ordered and non-overlapping.
    pub fn new(tables: Vec<Arc<Table>>) -> Self {
        Self {
            table_iter: None,
            cur_idx: 0,
            tables,
        }
    }

    /// Seek to a table, and then seek to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.tables.len() {
            self.table_iter = None;
        } else {
            let mut table_iter = TableIterator::new(self.tables[idx].clone());
            if let Some(key) = seek_key {
                table_iter.seek(key).await?;
            } else {
                table_iter.rewind().await?;
            }

            self.table_iter = Some(table_iter);
            self.cur_idx = idx;
        }

        Ok(())
    }
}

#[async_trait]
impl HummockIterator for ConcatIterator {
    async fn next(&mut self) -> HummockResult<()> {
        let table_iter = self.table_iter.as_mut().expect("no table iter");
        table_iter.next().await?;

        if table_iter.is_valid() {
            Ok(())
        } else {
            // seek to next table
            self.seek_idx(self.cur_idx + 1, None).await
        }
    }

    fn key(&self) -> &[u8] {
        self.table_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.table_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.table_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let table_idx = self
            .tables
            .partition_point(|table| {
                // compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `table`, thus the
                // partition point should be `prev(<=)` instead of `<`.
                let ord = VersionedComparator::compare_key(&table.meta.smallest_key, key);
                ord == Less || ord == Equal
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_test_table, gen_test_table_base, iterator_test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };

    #[tokio::test]
    async fn test_concat_iterator() {
        let table0 = gen_test_table(0, default_builder_opt_for_test()).await;
        let table1 = gen_test_table(1, default_builder_opt_for_test()).await;
        let table2 = gen_test_table(2, default_builder_opt_for_test()).await;

        let mut iter =
            ConcatIterator::new(vec![Arc::new(table0), Arc::new(table1), Arc::new(table2)]);
        let mut i = 0;
        iter.rewind().await.unwrap();

        while iter.is_valid() {
            let table_idx = (i / TEST_KEYS_COUNT) as u64;
            let key = iter.key();
            let val = iter.value();
            assert_eq!(
                key,
                iterator_test_key_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            assert_eq!(
                val.into_put_value().unwrap(),
                test_value_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            i += 1;

            iter.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!iter.is_valid());
                break;
            }
        }

        iter.rewind().await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(0, 0).as_slice()
        );
    }

    #[tokio::test]
    async fn test_concat_seek() {
        let table0 = gen_test_table(0, default_builder_opt_for_test()).await;
        let table1 = gen_test_table(1, default_builder_opt_for_test()).await;
        let table2 = gen_test_table(2, default_builder_opt_for_test()).await;
        let mut iter =
            ConcatIterator::new(vec![Arc::new(table0), Arc::new(table1), Arc::new(table2)]);

        iter.seek(iterator_test_key_of(1, 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(1, 1).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(1, 1).as_slice()
        );

        // Left edge case
        iter.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(0, 0).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(0, 0).as_slice()
        );

        // Right edge case
        iter.seek(iterator_test_key_of(2, TEST_KEYS_COUNT - 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(2, TEST_KEYS_COUNT - 1).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(2, TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(4, 10).as_slice())
            .await
            .unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_concat_seek_not_exists() {
        let table0 = gen_test_table_base(0, default_builder_opt_for_test(), |x| x * 2).await;
        let table1 = gen_test_table_base(1, default_builder_opt_for_test(), |x| x * 2).await;
        let table2 = gen_test_table_base(2, default_builder_opt_for_test(), |x| x * 2).await;
        let mut iter =
            ConcatIterator::new(vec![Arc::new(table0), Arc::new(table1), Arc::new(table2)]);

        iter.seek(iterator_test_key_of(1, 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(1, 2).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(1, 2).as_slice()
        );

        iter.seek(iterator_test_key_of(1, TEST_KEYS_COUNT * 114514).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(2, 0).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            test_value_of(2, 0).as_slice()
        );
    }
}
