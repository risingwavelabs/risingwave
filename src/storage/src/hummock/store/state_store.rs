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
use std::iter::once;
use std::ops::Bound::{Excluded, Included};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use minitrace::future::FutureExt;
use minitrace::Span;
use parking_lot::RwLock;
use risingwave_hummock_sdk::can_concat;
use risingwave_pb::hummock::LevelType;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc;

use super::event_handler::HummockEvent;
use super::memtable::Memtable;
use super::version::{HummockReadVersion, VersionUpdate};
use super::{GetFutureTrait, IterFutureTrait, ReadOptions, StateStore};
use crate::error::StorageResult;
use crate::hummock::compaction_group_client::CompactionGroupClientImpl;
use crate::hummock::iterator::{
    ConcatIterator, ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::{prune_ssts, search_sst_idx};
use crate::hummock::{hit_sstable_bloom_filter, HummockResult, SstableIterator};
use crate::monitor::StoreLocalStatistic;
use crate::{define_local_state_store_associated_type, StateStoreIter};

#[allow(unused)]
pub struct HummockStorageCore {
    /// Mutable memtable.
    memtable: Memtable,

    /// Read handle.
    read_version: RwLock<HummockReadVersion>,

    /// Event sender.
    event_sender: mpsc::UnboundedSender<HummockEvent>,

    // TODO: use a dedicated uploader implementation to replace `LocalVersionManager`
    uploader: Arc<LocalVersionManager>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    compaction_group_client: Arc<CompactionGroupClientImpl>,
}

#[allow(unused)]
#[derive(Clone)]
pub struct HummockStorage {
    core: Arc<HummockStorageCore>,
}

#[allow(unused)]
impl HummockStorageCore {
    /// See `HummockReadVersion::update` for more details.
    pub fn update(&mut self, info: VersionUpdate) -> HummockResult<()> {
        unimplemented!()
    }
}

#[allow(unused)]
impl StateStore for HummockStorage {
    type Iter = HummockStorageIterator;

    define_local_state_store_associated_type!();

    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn delete(&self, key: Bytes) -> StorageResult<()> {
        unimplemented!()
    }

    fn get(&self, key: &[u8], epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            // TODO: get compaction group id by table id
            let compaction_group_id = None;
            // 1. build iterator from staging data
            let (batches, sstable_infos, committed) = {
                let read_guard = self.core.read_version.read();
                let (batch_iter, sstable_info_iter) =
                    read_guard
                        .staging()
                        .prune_overlap(epoch, compaction_group_id, &key_range);
                (
                    batch_iter.cloned().collect_vec(),
                    sstable_info_iter.cloned().collect_vec(),
                    read_guard.committed().clone(),
                )
            };
            let mut local_stats = StoreLocalStatistic::default();
            let mut staging_iters = Vec::with_capacity(batches.len() + sstable_infos.len());
            staging_iters.extend(
                batches
                    .into_iter()
                    .map(|batch| HummockIteratorUnion::First(batch.into_forward_iter())),
            );
            for sstable_info in sstable_infos {
                let table_holder = self
                    .core
                    .sstable_store
                    .sstable(&sstable_info, &mut local_stats)
                    .in_span(Span::enter_with_local_parent("get_sstable"))
                    .await?;
                if let Some(prefix) = read_options.prefix_hint.as_ref() {
                    if !hit_sstable_bloom_filter(table_holder.value(), prefix, &mut local_stats) {
                        continue;
                    }
                }
                staging_iters.push(HummockIteratorUnion::Second(SstableIterator::new(
                    table_holder,
                    self.core.sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                )));
            }
            let staging_iter: StagingDataIterator = OrderedMergeIteratorInner::new(staging_iters);

            // 2. build iterator from committed
            let mut non_overlapping_iters = Vec::new();
            let mut overlapping_iters = Vec::new();
            // TODO: use the compaction group id obtained from table id
            for level in committed.levels(compaction_group_id) {
                let table_infos = prune_ssts(level.table_infos.iter(), &key_range);
                if table_infos.is_empty() {
                    continue;
                }

                if level.level_type == LevelType::Nonoverlapping as i32 {
                    debug_assert!(can_concat(&table_infos));
                    let start_table_idx = match key_range.start_bound() {
                        Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                        _ => 0,
                    };
                    let end_table_idx = match key_range.end_bound() {
                        Included(key) | Excluded(key) => search_sst_idx(&table_infos, key),
                        _ => table_infos.len().saturating_sub(1),
                    };
                    assert!(
                        start_table_idx < table_infos.len() && end_table_idx < table_infos.len()
                    );
                    let matched_table_infos = &table_infos[start_table_idx..=end_table_idx];

                    let mut sstables = vec![];
                    for sstable_info in matched_table_infos {
                        if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                            let sstable = self
                                .core
                                .sstable_store
                                .sstable(sstable_info, &mut local_stats)
                                .in_span(Span::enter_with_local_parent("get_sstable"))
                                .await?;

                            if hit_sstable_bloom_filter(
                                sstable.value(),
                                bloom_filter_key,
                                &mut local_stats,
                            ) {
                                sstables.push((*sstable_info).clone());
                            }
                        } else {
                            sstables.push((*sstable_info).clone());
                        }
                    }

                    non_overlapping_iters.push(ConcatIterator::new(
                        sstables,
                        self.core.sstable_store.clone(),
                        Arc::new(SstableIteratorReadOptions::default()),
                    ));
                } else {
                    for table_info in table_infos.into_iter().rev() {
                        let sstable = self
                            .core
                            .sstable_store
                            .sstable(table_info, &mut local_stats)
                            .in_span(Span::enter_with_local_parent("get_sstable"))
                            .await?;
                        if let Some(bloom_filter_key) = read_options.prefix_hint.as_ref() {
                            if !hit_sstable_bloom_filter(
                                sstable.value(),
                                bloom_filter_key,
                                &mut local_stats,
                            ) {
                                continue;
                            }
                        }

                        overlapping_iters.push(SstableIterator::new(
                            sstable,
                            self.core.sstable_store.clone(),
                            Arc::new(SstableIteratorReadOptions::default()),
                        ));
                    }
                }
            }

            // 3. build user_iterator
            let merge_iter = UnorderedMergeIteratorInner::new(
                once(HummockIteratorUnion::First(staging_iter))
                    .chain(
                        overlapping_iters
                            .into_iter()
                            .map(HummockIteratorUnion::Second),
                    )
                    .chain(
                        non_overlapping_iters
                            .into_iter()
                            .map(HummockIteratorUnion::Third),
                    ),
            );
            let mut user_iter = UserIterator::new(merge_iter, key_range, epoch, 0, Some(committed));
            user_iter.rewind().await?;
            Ok(HummockStorageIterator { inner: user_iter })
        }
    }

    fn flush(&self) -> StorageResult<usize> {
        unimplemented!()
    }

    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()> {
        unimplemented!()
    }
}

type StagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
type HummockStorageIteratorPayload = UnorderedMergeIteratorInner<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        SstableIterator,
        ConcatIteratorInner<SstableIterator>,
    >,
>;

pub struct HummockStorageIterator {
    inner: UserIterator<HummockStorageIteratorPayload>,
}

impl StateStoreIter for HummockStorageIterator {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                );
                iter.next().await?;
                Ok(Some(kv))
            } else {
                Ok(None)
            }
        }
    }
}
