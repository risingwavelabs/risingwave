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
//
use super::variants::BACKWARD;
use crate::hummock::iterator::merge_inner::MergeIteratorInner;

pub type ReverseMergeIterator<'a> = MergeIteratorInner<'a, BACKWARD>;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable, gen_iterator_test_sstable_base,
        iterator_test_key_of, iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::{BoxedHummockIterator, HummockIterator};
    use crate::hummock::ReverseSSTableIterator;
    use crate::monitor::StateStoreMetrics;

    #[tokio::test]
    async fn test_reverse_merge_basic() {
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
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(ReverseSSTableIterator::new(
                Arc::new(table2),
                sstable_store.clone(),
            )),
            Box::new(ReverseSSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(ReverseSSTableIterator::new(Arc::new(table0), sstable_store)),
        ];

        let mut iter = ReverseMergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let mut i = TEST_KEYS_COUNT * 3;
        iter.rewind().await.unwrap();
        while iter.is_valid() {
            let sort_index = ((i - 1) / TEST_KEYS_COUNT) as u64;
            let key = iter.key();
            let val = iter.value();
            assert_eq!(
                key,
                iterator_test_key_of(sort_index, (i - 1) % TEST_KEYS_COUNT).as_slice()
            );
            assert_eq!(
                val.into_put_value().unwrap(),
                iterator_test_value_of(sort_index, (i - 1) % TEST_KEYS_COUNT).as_slice()
            );
            i -= 1;
            iter.next().await.unwrap();
            if i == 0 {
                assert!(!iter.is_valid());
                break;
            }
        }
        // assert!(i >= 0);
    }

    #[tokio::test]
    async fn test_reverse_merge_seek() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x + 1,
            sstable_store.clone(),
            20,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x + 1,
            sstable_store.clone(),
            20,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x + 1,
            sstable_store.clone(),
            20,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(ReverseSSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(ReverseSSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(ReverseSSTableIterator::new(Arc::new(table2), sstable_store)),
        ];

        let mut mi = ReverseMergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        // right edge case
        mi.seek(iterator_test_key_of(0, 0).as_slice())
            .await
            .unwrap();
        assert!(!mi.is_valid());

        // normal case
        mi.seek(iterator_test_key_of(1, 4).as_slice())
            .await
            .unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(1, 4).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(1, 4).as_slice());

        mi.seek(iterator_test_key_of(0, 17).as_slice())
            .await
            .unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(0, 17).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(0, 17).as_slice());

        // left edge case
        mi.seek(iterator_test_key_of(2, 20).as_slice())
            .await
            .unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(2, 20).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(2, 20).as_slice());
        // let base_key = usize::MAX - 100;
        // let (iters, validators): (Vec<_>, Vec<_>) = (0..3)
        //     .map(|iter_id| {
        //         TestIteratorBuilder::<BACKWARD>::default()
        //             .sort_index(0)
        //             .total(20)
        //             .map_key(move |sort_index, x| {
        //                 iterator_test_key_of(sort_index, base_key - x * 3 + (3 - iter_id as
        // usize))             })
        //             .finish()
        //     })
        //     .unzip();
        // let iters: Vec<BoxedHummockIterator> = iters
        //     .into_iter()
        //     .map(|x| Box::new(x) as BoxedHummockIterator)
        //     .collect_vec();
        //
        // let mut mi = ReverseMergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        // let test_validator = &validators[2];
        //
        // // right edge case
        // mi.seek(test_key!(test_validator, 3 * TEST_KEYS_COUNT))
        //     .await
        //     .unwrap();
        // assert!(!mi.is_valid());
        //
        // // normal case
        // mi.seek(test_key!(test_validator, 4)).await.unwrap();
        // let k = mi.key();
        // let v = mi.value();
        // test_validator.assert_hummock_value(4, v);
        // test_validator.assert_key(4, k);
        //
        // mi.seek(test_key!(test_validator, 17)).await.unwrap();
        // let k = mi.key();
        // let v = mi.value();
        // test_validator.assert_hummock_value(17, v);
        // test_validator.assert_key(17, k);
        //
        // // left edge case
        // mi.seek(test_key!(test_validator, 0)).await.unwrap();
        // let k = mi.key();
        // let v = mi.value();
        // test_validator.assert_hummock_value(0, v);
        // test_validator.assert_key(0, k);
    }

    #[tokio::test]
    async fn test_reverse_merge_invalidate_reset() {
        let sstable_store = mock_sstable_store();
        let table0 =
            gen_iterator_test_sstable(0, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let table1 =
            gen_iterator_test_sstable(1, default_builder_opt_for_test(), sstable_store.clone())
                .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(ReverseSSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(ReverseSSTableIterator::new(Arc::new(table0), sstable_store)),
        ];

        let mut mi = ReverseMergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        mi.rewind().await.unwrap();
        let mut count = 0;
        while mi.is_valid() {
            count += 1;
            mi.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);

        mi.rewind().await.unwrap();
        let mut count = 0;
        while mi.is_valid() {
            count += 1;
            mi.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);
    }
}
