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

use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;

use bytes::Bytes;
use dashmap::DashSet;
use risingwave_common::skiplist::{IterRef, Skiplist};
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_pb::hummock::Level;

use crate::hummock::iterator::{
    DeleteRangeIterator, DirectionEnum, HummockIterator, HummockIteratorDirection,
};
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
const BLOOM_FILTER_HASH_CAPACITY: usize = 256 * 1024;

#[derive(Clone)]
pub struct LevelZeroCache {
    pub cache: Skiplist<FullKey<Vec<u8>>, HummockValue<Bytes>>,
    pub level: Level,
    pub bloom_filter: DashSet<u32>,
}

impl Debug for LevelZeroCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LevelZeroCache")
            .field("level", &self.level)
            .finish()
    }
}

impl LevelZeroCache {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            cache: Skiplist::new(true),
            bloom_filter: DashSet::with_capacity(BLOOM_FILTER_HASH_CAPACITY),
        }
    }

    pub fn iter<D: HummockIteratorDirection>(
        &self,
        committed_epoch: u64,
    ) -> LevelZeroCacheIterator<D> {
        LevelZeroCacheIterator::<D> {
            iter: self.cache.iter(),
            borrow_phantom: PhantomData,
            committed_epoch,
            next_count: 0,
            seek_count: 0,
        }
    }

    pub fn get(&self, key: &FullKey<Vec<u8>>, dist_key_hash: u32) -> Option<HummockValue<Bytes>> {
        if !self.bloom_filter.contains(&dist_key_hash) {
            return None;
        }
        let mut iter = self.cache.iter();
        iter.seek(key);
        while iter.valid() && iter.key().epoch > key.epoch {
            if iter.key().user_key != key.user_key {
                return None;
            }
            iter.next();
        }
        if iter.valid() && iter.key().user_key == key.user_key {
            return Some(iter.value().clone());
        }
        None
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

pub struct LevelZeroCacheIterator<D: HummockIteratorDirection> {
    iter: IterRef<FullKey<Vec<u8>>, HummockValue<Bytes>>,
    committed_epoch: u64,
    borrow_phantom: PhantomData<D>,
    next_count: u64,
    seek_count: u64,
}

impl<D: HummockIteratorDirection> HummockIterator for LevelZeroCacheIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            debug_assert!(self.is_valid());
            self.next_count += 1;
            match D::direction() {
                DirectionEnum::Forward => {
                    self.iter.next();
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     self.iter.next();
                    // }
                }
                DirectionEnum::Backward => {
                    self.iter.prev();
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     self.iter.prev();
                    // }
                }
            }

            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.iter.key().to_ref()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.iter.value().as_slice()
    }

    fn is_valid(&self) -> bool {
        self.iter.valid()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.seek_count += 1;
            match D::direction() {
                DirectionEnum::Forward => {
                    self.iter.seek_to_first();
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     self.iter.next();
                    // }
                }
                DirectionEnum::Backward => {
                    self.iter.seek_to_last();
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     self.iter.prev();
                    // }
                }
            }

            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            self.seek_count += 1;
            match D::direction() {
                DirectionEnum::Forward => {
                    self.iter.seek(&key.to_vec());
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     if self.iter.key().user_key.as_ref() != key.user_key {
                    //         break;
                    //     }
                    //     self.iter.next();
                    // }
                }
                DirectionEnum::Backward => {
                    self.iter.seek_for_prev(&key.to_vec());
                    // while self.iter.valid() && self.iter.key().epoch > self.committed_epoch {
                    //     self.iter.prev();
                    // }
                }
            }
            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut crate::monitor::StoreLocalStatistic) {
        stats.total_key_count += self.next_count;
        stats.total_seek_count += self.seek_count;
    }
}
