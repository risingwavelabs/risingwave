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

use std::future::Future;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_pb::hummock::Level;

use crate::hummock::iterator::{HummockIterator, HummockIteratorDirection};
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockResult, SstableIterator};

#[derive(Debug, Clone)]
pub struct LevelZeroCache {
    pub cache: Arc<SkipMap<FullKey<Vec<u8>>, HummockValue<Bytes>>>,
    pub level: Level,
}

impl LevelZeroCache {
    pub fn iter<D: HummockIteratorDirection>(
        &self,
        lower: Bound<FullKey<Vec<u8>>>,
        upper: Bound<FullKey<Vec<u8>>>,
        committed_epoch: u64,
    ) -> LevelZeroCacheIterator<D> {
        let mut iter = LevelZeroCacheIteratorBuilder {
            map: self.cache.clone(),
            iter_builder: |map| map.range((lower, upper)),
            borrow_phantom: PhantomData,
            committed_epoch,
            item: (
                FullKey::new(TableId::new(0), TableKey(vec![]), 0),
                HummockValue::Delete,
            ),
        }
        .build();
        let entry =
            iter.with_iter_mut(|iter| LevelZeroCacheIterator::<D>::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        iter
    }
}

impl PartialEq for LevelZeroCache {
    fn eq(&self, other: &Self) -> bool {
        self.level.eq(&other.level)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LevelZeroData {
    pub caches: Vec<LevelZeroCache>,
    pub sub_levels: Vec<Level>,
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    FullKey<Vec<u8>>,
    (Bound<FullKey<Vec<u8>>>, Bound<FullKey<Vec<u8>>>),
    FullKey<Vec<u8>>,
    HummockValue<Bytes>,
>;

#[self_referencing]
pub struct LevelZeroCacheIterator<D: HummockIteratorDirection> {
    map: Arc<SkipMap<FullKey<Vec<u8>>, HummockValue<Bytes>>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    committed_epoch: u64,
    item: (FullKey<Vec<u8>>, HummockValue<Bytes>),
    borrow_phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> LevelZeroCacheIterator<D> {
    fn entry_to_item(
        entry: Option<crossbeam_skiplist::map::Entry<'_, FullKey<Vec<u8>>, HummockValue<Bytes>>>,
    ) -> (FullKey<Vec<u8>>, HummockValue<Bytes>) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| {
                (
                    FullKey::new(TableId::new(0), TableKey(vec![]), 0),
                    HummockValue::Delete,
                )
            })
    }
}

impl<D: HummockIteratorDirection> HummockIterator for LevelZeroCacheIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            assert!(self.is_valid());
            let entry = self.with_iter_mut(|iter| Self::entry_to_item(iter.next()));
            self.with_mut(|x| *x.item = entry);
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.borrow_item().0.to_ref()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.borrow_item().1.as_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.user_key.is_empty()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move { Ok(()) }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move { Ok(()) }
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}
}
