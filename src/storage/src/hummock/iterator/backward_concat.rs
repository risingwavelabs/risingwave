// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::hummock::BackwardSstableIterator;
use crate::hummock::iterator::concat_inner::ConcatIteratorInner;

/// Iterates backwards on multiple non-overlapping tables.
pub type BackwardConcatIterator = ConcatIteratorInner<BackwardSstableIterator>;

/// Mirrors the tests used for `SstableIterator`
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_iterator_test_sstable_info,
        iterator_test_key_of, iterator_test_value_of, mock_sstable_store,
    };
    use crate::hummock::sstable::SstableIteratorReadOptions;

    #[tokio::test]
    async fn test_backward_concat_iterator() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = BackwardConcatIterator::new(
            vec![table2, table1, table0],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        let mut i = TEST_KEYS_COUNT * 3;
        iter.rewind().await.unwrap();

        while iter.is_valid() {
            i -= 1;
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, iterator_test_key_of(i).to_ref());
            assert_eq!(
                val.into_user_value().unwrap(),
                iterator_test_value_of(i).as_slice()
            );
            iter.next().await.unwrap();
        }
        assert_eq!(i, 0);
        assert!(!iter.is_valid());

        iter.rewind().await.unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT * 3 - 1).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 3 - 1).as_slice()
        );
    }

    #[tokio::test]
    async fn test_backward_concat_seek_exists() {
        let sstable_store = mock_sstable_store().await;
        let table1 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 2 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table3 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT * 3 + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = BackwardConcatIterator::new(
            vec![table3, table2, table1],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(2 * TEST_KEYS_COUNT + 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(2 * TEST_KEYS_COUNT + 1).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(2 * TEST_KEYS_COUNT + 1).as_slice()
        );

        // Left edge case
        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT).to_ref())
            .await
            .unwrap();
        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT).as_slice()
        );

        // Right edge case
        iter.seek(iterator_test_key_of(4 * TEST_KEYS_COUNT - 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(4 * TEST_KEYS_COUNT - 1).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(4 * TEST_KEYS_COUNT - 1).as_slice()
        );

        // Right overflow case
        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT - 1).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_backward_concat_seek_not_exists() {
        let sstable_store = mock_sstable_store().await;
        let table0 = gen_iterator_test_sstable_info(
            0,
            default_builder_opt_for_test(),
            |x| x * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_info(
            1,
            default_builder_opt_for_test(),
            |x| (TEST_KEYS_COUNT + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_info(
            2,
            default_builder_opt_for_test(),
            |x| (TEST_KEYS_COUNT * 2 + x) * 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let mut iter = BackwardConcatIterator::new(
            vec![table2, table1, table0],
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );

        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 2 + 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(key, iterator_test_key_of(TEST_KEYS_COUNT * 2).to_ref());
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 2).as_slice()
        );

        // table1 last
        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 4 - 1).to_ref())
            .await
            .unwrap();

        let key = iter.key();
        let val = iter.value();
        assert_eq!(
            key,
            iterator_test_key_of((TEST_KEYS_COUNT + 9) * 2).to_ref()
        );
        assert_eq!(
            val.into_user_value().unwrap(),
            iterator_test_value_of((TEST_KEYS_COUNT + 9) * 2).as_slice()
        );
    }
}
