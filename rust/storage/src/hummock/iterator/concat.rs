use crate::hummock::iterator::concat_inner::ConcatIteratorInner;
use crate::hummock::TableIterator;

/// Iterates on multiple non-overlapping tables.
pub type ConcatIterator = ConcatIteratorInner<TableIterator>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_test_table, gen_test_table_base, iterator_test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::HummockIterator;

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
