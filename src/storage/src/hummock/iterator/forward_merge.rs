// Copyright 2024 RisingWave Labs
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

#[cfg(test)]
mod test {
    use std::future::{pending, poll_fn};
    use std::iter::once;
    use std::sync::Arc;
    use std::task::Poll;

    use futures::{pin_mut, FutureExt};
    use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};
    use risingwave_hummock_sdk::EpochWithGap;

    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_info,
        gen_merge_iterator_interleave_test_sstable_iters, iterator_test_key_of,
        iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator, ValueMeta};
    use crate::hummock::sstable::{
        SstableIterator, SstableIteratorReadOptions, SstableIteratorType,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::HummockResult;
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    async fn test_merge_basic() {
        let mut iter = MergeIterator::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
        );

        // Test merge iterators
        let mut i = 0;
        iter.rewind().await.unwrap();
        while iter.is_valid() {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, iterator_test_key_of(i).to_ref());
            assert_eq!(
                val.into_user_value().unwrap(),
                iterator_test_value_of(i).as_slice()
            );
            i += 1;
            iter.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!iter.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT * 3);
    }

    #[tokio::test]
    async fn test_merge_seek() {
        let mut iter = MergeIterator::new(
            gen_merge_iterator_interleave_test_sstable_iters(TEST_KEYS_COUNT, 3).await,
        );

        // Test merge iterators
        // right edge case
        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 3).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // normal case
        iter.seek(iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).to_ref())
            .await
            .unwrap();
        let k = iter.key();
        let v = iter.value();
        assert_eq!(
            v.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 2 + 5).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).to_ref());

        iter.seek(iterator_test_key_of(17).to_ref()).await.unwrap();
        let k = iter.key();
        let v = iter.value();
        assert_eq!(
            v.into_user_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 7).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT + 7).to_ref());

        // left edge case
        iter.seek(iterator_test_key_of(0).to_ref()).await.unwrap();
        let k = iter.key();
        let v = iter.value();
        assert_eq!(
            v.into_user_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(0).to_ref());
    }

    #[tokio::test]
    async fn test_merge_invalidate_reset() {
        let sstable_store = mock_sstable_store();
        let read_options = Arc::new(SstableIteratorReadOptions::default());
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

        let mut stats = StoreLocalStatistic::default();
        let mut iter = MergeIterator::new(vec![
            SstableIterator::create(
                sstable_store.sstable(&table0, &mut stats).await.unwrap(),
                sstable_store.clone(),
                read_options.clone(),
            ),
            SstableIterator::create(
                sstable_store.sstable(&table1, &mut stats).await.unwrap(),
                sstable_store.clone(),
                read_options.clone(),
            ),
        ]);

        iter.rewind().await.unwrap();
        let mut count = 0;
        while iter.is_valid() {
            count += 1;
            iter.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);

        iter.rewind().await.unwrap();
        let mut count = 0;
        while iter.is_valid() {
            count += 1;
            iter.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);
    }

    struct CancellationTestIterator {}

    impl HummockIterator for CancellationTestIterator {
        type Direction = Forward;

        async fn next(&mut self) -> HummockResult<()> {
            pending::<HummockResult<()>>().await
        }

        fn key(&self) -> FullKey<&[u8]> {
            FullKey {
                user_key: UserKey {
                    table_id: Default::default(),
                    table_key: TableKey(&b"test_key"[..]),
                },
                epoch_with_gap: EpochWithGap::new_from_epoch(0),
            }
        }

        fn value(&self) -> HummockValue<&[u8]> {
            HummockValue::delete()
        }

        fn is_valid(&self) -> bool {
            true
        }

        async fn rewind(&mut self) -> HummockResult<()> {
            Ok(())
        }

        async fn seek<'a>(&'a mut self, _key: FullKey<&'a [u8]>) -> HummockResult<()> {
            Ok(())
        }

        fn collect_local_statistic(&self, _stats: &mut StoreLocalStatistic) {}

        fn value_meta(&self) -> ValueMeta {
            ValueMeta::default()
        }
    }

    #[tokio::test]
    async fn test_merge_iter_cancel() {
        let mut merge_iter = MergeIterator::new(vec![
            MergeIterator::new(once(CancellationTestIterator {})),
            MergeIterator::new(once(CancellationTestIterator {})),
        ]);
        merge_iter.rewind().await.unwrap();
        let future = merge_iter.next();

        pin_mut!(future);

        for _ in 0..10 {
            assert!(poll_fn(|cx| { Poll::Ready(future.poll_unpin(cx)) })
                .await
                .is_pending());
        }

        // Dropping the future will panic if the OrderedMergeIterator is not cancellation safe.
        // See https://github.com/risingwavelabs/risingwave/issues/6637
    }
}
