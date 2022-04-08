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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::hummock::iterator::variants::*;
use crate::hummock::iterator::HummockIterator;
use crate::hummock::value::HummockValue;
use crate::hummock::{key, HummockResult};

pub(super) type SharedBufferItem = (Bytes, HummockValue<Bytes>);

/// A write batch stored in the shared buffer.
#[derive(Clone, Debug)]
pub struct SharedBufferBatch {
    pub(super) inner: Arc<[SharedBufferItem]>,
    pub(super) epoch: u64,
}

impl SharedBufferBatch {
    pub fn new(sorted_items: Vec<SharedBufferItem>, epoch: u64) -> Self {
        Self {
            inner: sorted_items.into(),
            epoch,
        }
    }

    pub fn get(&self, user_key: &[u8]) -> Option<HummockValue<Vec<u8>>> {
        // Perform binary search on user key because the items in SharedBufferBatch is ordered by
        // user key.
        match self
            .inner
            .binary_search_by(|m| key::user_key(&m.0).cmp(user_key))
        {
            Ok(i) => Some(self.inner[i].1.to_vec()),
            Err(_) => None,
        }
    }

    pub fn iter(&self) -> SharedBufferBatchIterator<FORWARD> {
        SharedBufferBatchIterator::<FORWARD>::new(self.inner.clone())
    }

    pub fn reverse_iter(&self) -> SharedBufferBatchIterator<BACKWARD> {
        SharedBufferBatchIterator::<BACKWARD>::new(self.inner.clone())
    }

    #[allow(dead_code)]
    pub fn start_key(&self) -> &[u8] {
        &self.inner.first().unwrap().0
    }

    #[allow(dead_code)]
    pub fn end_key(&self) -> &[u8] {
        &self.inner.last().unwrap().0
    }

    pub fn start_user_key(&self) -> &[u8] {
        key::user_key(&self.inner.first().unwrap().0)
    }

    pub fn end_user_key(&self) -> &[u8] {
        key::user_key(&self.inner.last().unwrap().0)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

pub struct SharedBufferBatchIterator<const DIRECTION: usize> {
    inner: Arc<[SharedBufferItem]>,
    current_idx: usize,
}

impl<const DIRECTION: usize> SharedBufferBatchIterator<DIRECTION> {
    pub fn new(inner: Arc<[SharedBufferItem]>) -> Self {
        Self {
            inner,
            current_idx: 0,
        }
    }

    fn current_item(&self) -> &SharedBufferItem {
        assert!(self.is_valid());
        let idx = match DIRECTION {
            FORWARD => self.current_idx,
            BACKWARD => self.inner.len() - self.current_idx - 1,
            _ => unreachable!(),
        };
        self.inner.get(idx).unwrap()
    }
}

#[async_trait]
impl<const DIRECTION: usize> HummockIterator for SharedBufferBatchIterator<DIRECTION> {
    async fn next(&mut self) -> HummockResult<()> {
        assert!(self.is_valid());
        self.current_idx += 1;
        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.current_item().0
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.current_item().1.as_slice()
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.len()
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.current_idx = 0;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        // Perform binary search on user key because the items in SharedBufferBatch is ordered by
        // user key.
        let partition_point = self
            .inner
            .binary_search_by(|probe| key::user_key(&probe.0).cmp(key::user_key(key)));
        let seek_key_epoch = key::get_epoch(key);
        match DIRECTION {
            FORWARD => {
                match partition_point {
                    Ok(i) => {
                        self.current_idx = i;
                        // The user key part must be the same if we reach here.
                        let current_key_epoch = key::get_epoch(&self.inner[i].0);
                        if current_key_epoch > seek_key_epoch {
                            // Move onto the next key for forward iteration if the current key has a
                            // larger epoch
                            self.current_idx += 1;
                        }
                    }
                    Err(i) => self.current_idx = i,
                }
            }
            BACKWARD => {
                match partition_point {
                    Ok(i) => {
                        self.current_idx = self.inner.len() - i - 1;
                        // The user key part must be the same if we reach here.
                        let current_key_epoch = key::get_epoch(&self.inner[i].0);
                        if current_key_epoch < seek_key_epoch {
                            // Move onto the prev key for backward iteration if the current key has
                            // a smaller epoch
                            self.current_idx += 1;
                        }
                    }
                    // Seek to one item before the seek partition_point:
                    // If i == 0, the iterator will be invalidated with self.current_idx ==
                    // self.inner.len().
                    Err(i) => self.current_idx = self.inner.len() - i,
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use itertools::Itertools;
    use risingwave_hummock_sdk::key::user_key;

    use super::*;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, iterator_test_key_of_epoch};

    fn transform_shared_buffer(
        batches: Vec<(Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> Vec<(Bytes, HummockValue<Bytes>)> {
        batches
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_basic() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_key_of_epoch(0, epoch),
                HummockValue::put(b"value1".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(1, epoch),
                HummockValue::put(b"value2".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(2, epoch),
                HummockValue::put(b"value3".to_vec()),
            ),
        ];
        let shared_buffer_batch =
            SharedBufferBatch::new(transform_shared_buffer(shared_buffer_items.clone()), epoch);

        // Sketch
        assert_eq!(shared_buffer_batch.start_key(), shared_buffer_items[0].0);
        assert_eq!(shared_buffer_batch.end_key(), shared_buffer_items[2].0);
        assert_eq!(
            shared_buffer_batch.start_user_key(),
            user_key(&shared_buffer_items[0].0)
        );
        assert_eq!(
            shared_buffer_batch.end_user_key(),
            user_key(&shared_buffer_items[2].0)
        );

        // Point lookup
        for (k, v) in &shared_buffer_items {
            assert_eq!(
                shared_buffer_batch.get(user_key(k.as_slice())),
                Some(v.clone())
            );
        }
        assert_eq!(
            shared_buffer_batch.get(iterator_test_key_of(3).as_slice()),
            None
        );
        assert_eq!(
            shared_buffer_batch.get(iterator_test_key_of(4).as_slice()),
            None
        );

        // Forward iterator
        let mut iter = shared_buffer_batch.iter();
        iter.rewind().await.unwrap();
        let mut output = vec![];
        while iter.is_valid() {
            output.push((iter.key().to_owned(), iter.value().to_owned_value()));
            iter.next().await.unwrap();
        }
        assert_eq!(output, shared_buffer_items);

        // Backward iterator
        let mut reverse_iter = shared_buffer_batch.reverse_iter();
        reverse_iter.rewind().await.unwrap();
        let mut output = vec![];
        while reverse_iter.is_valid() {
            output.push((
                reverse_iter.key().to_owned(),
                reverse_iter.value().to_owned_value(),
            ));
            reverse_iter.next().await.unwrap();
        }
        output.reverse();
        assert_eq!(output, shared_buffer_items);
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_seek() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_key_of_epoch(1, epoch),
                HummockValue::put(b"value1".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(2, epoch),
                HummockValue::put(b"value2".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(3, epoch),
                HummockValue::put(b"value3".to_vec()),
            ),
        ];
        let shared_buffer_batch =
            SharedBufferBatch::new(transform_shared_buffer(shared_buffer_items.clone()), epoch);

        // FORWARD: Seek to a key < 1st key, expect all three items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(0, epoch))
            .await
            .unwrap();
        for item in &shared_buffer_items {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to a key > the last key, expect no items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(4, epoch))
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with current epoch, expect last two items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch))
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with future epoch, expect last two items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch + 1))
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with old epoch, expect last item to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch - 1))
            .await
            .unwrap();
        let item = shared_buffer_items.last().unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key < 1st key, expect no items to return
        let mut iter = shared_buffer_batch.reverse_iter();
        iter.seek(&iterator_test_key_of_epoch(0, epoch))
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key > the last key, expect all items to return
        let mut iter = shared_buffer_batch.reverse_iter();
        iter.seek(&iterator_test_key_of_epoch(4, epoch))
            .await
            .unwrap();
        for item in shared_buffer_items.iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with current epoch, expect first two items to return
        let mut iter = shared_buffer_batch.reverse_iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch))
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with future epoch, expect first item to return
        let mut iter = shared_buffer_batch.reverse_iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch + 1))
            .await
            .unwrap();
        assert!(iter.is_valid());
        let item = shared_buffer_items.first().unwrap();
        assert_eq!(iter.key(), item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with old epoch, expect first two item to return
        let mut iter = shared_buffer_batch.reverse_iter();
        iter.seek(&iterator_test_key_of_epoch(2, epoch - 1))
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }
}
