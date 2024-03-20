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

pub mod hummock_storage;
pub mod local_hummock_storage;
pub mod version;

pub use hummock_storage::*;
pub use local_hummock_storage::*;

use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::iterator::{
    Backward, BackwardMergeRangeIterator, ConcatIteratorInner, Forward, ForwardMergeRangeIterator,
    HummockIteratorUnion, IteratorFactory, OrderedMergeIteratorInner, UnorderedMergeIteratorInner,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::{BackwardSstableIterator, SstableIterator};

pub type StagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;

pub type BackwardStagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Backward, SharedBufferBatchIterator<Backward>, BackwardSstableIterator>,
>;

pub type ForwardUnionIterator = HummockIteratorUnion<
    Forward,
    StagingDataIterator,
    SstableIterator,
    ConcatIteratorInner<SstableIterator>,
>;

pub type BackwardUnionIterator = HummockIteratorUnion<
    Backward,
    BackwardStagingDataIterator,
    BackwardSstableIterator,
    ConcatIteratorInner<BackwardSstableIterator>,
>;

pub type HummockStorageIteratorPayload = UnorderedMergeIteratorInner<ForwardUnionIterator>;

pub struct ForwardIteratorFactory {
    delete_range_iter: ForwardMergeRangeIterator,
    non_overlapping_iters: Vec<ConcatIteratorInner<SstableIterator>>,
    overlapping_iters: Vec<SstableIterator>,
    staging_iters:
    Vec<HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>>,
}

impl ForwardIteratorFactory {
    pub fn new(read_epoch: HummockEpoch) -> Self {
        Self {
            delete_range_iter: ForwardMergeRangeIterator::new(read_epoch),
            non_overlapping_iters: vec![],
            overlapping_iters: vec![],
            staging_iters: vec![],
        }
    }

    pub fn build(self) -> (HummockStorageIteratorPayload, ForwardMergeRangeIterator) {
        // 3. build user_iterator
        let staging_iter = StagingDataIterator::new(self.staging_iters);
        let merge_iter = UnorderedMergeIteratorInner::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    self.overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    self.non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                ),
        );
        (merge_iter, self.delete_range_iter)
    }
}

impl IteratorFactory for ForwardIteratorFactory {
    type Direction = Forward;
    type SstableIteratorType = SstableIterator;

    fn add_batch_iter(&mut self, batch: SharedBufferBatch) {
        self.staging_iters
            .push(HummockIteratorUnion::First(batch.into_forward_iter()));
    }

    fn add_staging_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.staging_iters.push(HummockIteratorUnion::Second(iter));
    }

    fn add_overlapping_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.overlapping_iters.push(iter);
    }

    fn add_concat_sst_iter(&mut self, iter: ConcatIteratorInner<Self::SstableIteratorType>) {
        self.non_overlapping_iters.push(iter);
    }
}

pub struct BackwardIteratorFactory {
    non_overlapping_iters: Vec<ConcatIteratorInner<BackwardSstableIterator>>,
    overlapping_iters: Vec<BackwardSstableIterator>,
    staging_iters: Vec<
        HummockIteratorUnion<
            Backward,
            SharedBufferBatchIterator<Backward>,
            BackwardSstableIterator,
        >,
    >,
}

impl BackwardIteratorFactory {
    pub fn new() -> Self {
        Self {
            non_overlapping_iters: vec![],
            overlapping_iters: vec![],
            staging_iters: vec![],
        }
    }

    pub fn build(
        self,
    ) -> UnorderedMergeIteratorInner<BackwardUnionIterator> {
        // 3. build user_iterator
        let staging_iter = BackwardStagingDataIterator::new(self.staging_iters);
        let merge_iter = UnorderedMergeIteratorInner::new(
            once(HummockIteratorUnion::First(staging_iter))
                .chain(
                    self.overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Second),
                )
                .chain(
                    self.non_overlapping_iters
                        .into_iter()
                        .map(HummockIteratorUnion::Third),
                ),
        );
        merge_iter
    }
}

impl IteratorFactory for BackwardIteratorFactory {
    type Direction = Backward;
    type SstableIteratorType = BackwardSstableIterator;

    fn add_batch_iter(&mut self, batch: SharedBufferBatch) {
        self.staging_iters
            .push(HummockIteratorUnion::First(batch.into_backward_iter()));
    }

    fn add_staging_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.staging_iters.push(HummockIteratorUnion::Second(iter));
    }

    fn add_overlapping_sst_iter(&mut self, iter: Self::SstableIteratorType) {
        self.overlapping_iters.push(iter);
    }

    fn add_concat_sst_iter(&mut self, iter: ConcatIteratorInner<Self::SstableIteratorType>) {
        self.non_overlapping_iters.push(iter);
    }
}