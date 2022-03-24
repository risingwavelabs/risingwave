// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::hummock::iterator::concat_inner::ConcatIteratorInner;
use crate::hummock::ReverseSSTableIterator;

/// Reversely iterates on multiple non-overlapping tables.
#[allow(dead_code)]
pub type ReverseConcatIterator = ConcatIteratorInner<ReverseSSTableIterator>;

/// Mirror the tests used for `SSTableIterator`
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable, gen_iterator_test_sstable_base,
        iterator_test_key_of, iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::HummockIterator;

    #[tokio::test]
    async fn test_reverse_concat_iterator() {
        let sstable_store = mock_sstable_store();
        let table0 =
            gen_iterator_test_sstable(0, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let table1 =
            gen_iterator_test_sstable(1, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let table2 =
            gen_iterator_test_sstable(2, default_builder_opt_for_test(), sstable_store.clone())
                .await;

        let mut iter = ReverseConcatIterator::new(
            vec![Arc::new(table2), Arc::new(table1), Arc::new(table0)],
            sstable_store,
        );
        let mut i = TEST_KEYS_COUNT * 3;
        iter.rewind().await.unwrap();

        while iter.is_valid() {
            i -= 1;
            let table_idx = (i / TEST_KEYS_COUNT) as u64;
            let key = iter.key();
            let val = iter.value();
            assert_eq!(
                key,
                iterator_test_key_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            assert_eq!(
                val.into_put_value().unwrap(),
                iterator_test_value_of(table_idx, i % TEST_KEYS_COUNT).as_slice()
            );
            iter.next().await.unwrap();
        }
        assert_eq!(i, 0);
        assert!(!iter.is_valid());

        iter.rewind().await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(2, TEST_KEYS_COUNT - 1).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(2, TEST_KEYS_COUNT - 1).as_slice()
        );
    }

    #[tokio::test]
    async fn test_reverse_concat_seek_exists() {
        let sstable_store = mock_sstable_store();
        let table1 =
            gen_iterator_test_sstable(1, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let table2 =
            gen_iterator_test_sstable(2, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let table3 =
            gen_iterator_test_sstable(3, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let mut iter = ReverseConcatIterator::new(
            vec![Arc::new(table3), Arc::new(table2), Arc::new(table1)],
            sstable_store,
        );

        iter.seek(iterator_test_key_of(2, 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(2, 1).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(2, 1).as_slice()
        );

        // Left edge case
        iter.seek(iterator_test_key_of(1, 0).as_slice())
            .await
            .unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(1, 0).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(1, 0).as_slice()
        );

        // Right edge case
        iter.seek(iterator_test_key_of(3, TEST_KEYS_COUNT - 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(3, TEST_KEYS_COUNT - 1).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(3, TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(0, 10).as_slice())
            .await
            .unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_reverse_concat_seek_not_exists() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
        )
        .await;
        let mut iter = ReverseConcatIterator::new(
            vec![Arc::new(table2), Arc::new(table1), Arc::new(table0)],
            sstable_store,
        );

        iter.seek(iterator_test_key_of(1, 1).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(1, 0).as_slice());
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(1, 0).as_slice()
        );

        iter.seek(iterator_test_key_of(1, TEST_KEYS_COUNT * 114514).as_slice())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(
            key,
            iterator_test_key_of(1, (TEST_KEYS_COUNT - 1) * 2).as_slice()
        );
        assert_eq!(
            val.into_put_value().unwrap(),
            iterator_test_value_of(1, (TEST_KEYS_COUNT - 1) * 2).as_slice()
        );
    }
}
