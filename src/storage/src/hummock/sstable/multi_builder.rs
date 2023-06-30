// Copyright 2023 RisingWave Labs
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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;

use num_integer::Integer;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::LocalSstableInfo;
use tokio::task::JoinHandle;

use super::{CompactionDeleteRanges, MonotonicDeleteEvent};
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::sstable::filter::FilterBuilder;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BatchUploadWriter, CachePolicy, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriter, SstableWriterOptions, Xor16FilterBuilder,
};
use crate::monitor::CompactorMetrics;

pub type UploadJoinHandle = JoinHandle<HummockResult<()>>;

#[async_trait::async_trait]
pub trait TableBuilderFactory {
    type Writer: SstableWriter<Output = UploadJoinHandle>;
    type Filter: FilterBuilder;
    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>>;
}

pub struct SplitTableOutput {
    pub sst_info: LocalSstableInfo,
    pub upload_join_handle: UploadJoinHandle,
}

/// A wrapper for [`SstableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// When creating a new [`SstableBuilder`], caller use this factory to generate it.
    builder_factory: F,

    sst_outputs: Vec<SplitTableOutput>,

    current_builder: Option<SstableBuilder<F::Writer, F::Filter>>,

    /// Statistics.
    pub compactor_metrics: Arc<CompactorMetrics>,

    /// Update the number of sealed Sstables.
    task_progress: Option<Arc<TaskProgress>>,

    last_sealed_key: UserKey<Vec<u8>>,
    pub del_agg: Arc<CompactionDeleteRanges>,
    key_range: KeyRange,
    last_table_id: u32,
    is_target_level_l0_or_lbase: bool,
    split_by_table: bool,
    split_weight_by_vnode: u32,
    /// When vnode of the coming key is greater than `largest_vnode_in_current_partition`, we will
    /// switch SST.
    largest_vnode_in_current_partition: usize,
    last_vnode: usize,
}

impl<F> CapacitySplitTableBuilder<F>
where
    F: TableBuilderFactory,
{
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        builder_factory: F,
        compactor_metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        del_agg: Arc<CompactionDeleteRanges>,
        key_range: KeyRange,
        is_target_level_l0_or_lbase: bool,
        mut split_by_table: bool,
        mut split_weight_by_vnode: u32,
    ) -> Self {
        let start_key = if key_range.left.is_empty() {
            UserKey::default()
        } else {
            FullKey::decode(&key_range.left).user_key.to_vec()
        };

        if !is_target_level_l0_or_lbase {
            split_weight_by_vnode = 0;
        }

        if split_weight_by_vnode > 0 {
            split_by_table = true;
        }

        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics,
            task_progress,
            del_agg,
            last_sealed_key: start_key,
            key_range,
            last_table_id: 0,
            is_target_level_l0_or_lbase,
            split_by_table,
            split_weight_by_vnode,
            largest_vnode_in_current_partition: VirtualNode::MAX.to_index(),
            last_vnode: 0,
        }
    }

    pub fn for_test(builder_factory: F) -> Self {
        Self {
            builder_factory,
            sst_outputs: Vec::new(),
            current_builder: None,
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            task_progress: None,
            last_sealed_key: UserKey::default(),
            del_agg: Arc::new(CompactionDeleteRanges::for_test()),
            key_range: KeyRange::inf(),
            last_table_id: 0,
            is_target_level_l0_or_lbase: false,
            split_by_table: false,
            split_weight_by_vnode: 0,
            largest_vnode_in_current_partition: VirtualNode::MAX.to_index(),
            last_vnode: 0,
        }
    }

    /// Returns the number of [`SstableBuilder`]s.
    pub fn len(&self) -> usize {
        self.sst_outputs.len() + self.current_builder.is_some() as usize
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.sst_outputs.is_empty() && self.current_builder.is_none()
    }

    pub async fn add_full_key_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        self.add_full_key(full_key, value, is_new_user_key).await
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        is_new_user_key: bool,
    ) -> HummockResult<()> {
        let mut switch_builder = false;
        let mut vnode_changed = false;
        if self.split_by_table && full_key.user_key.table_id.table_id != self.last_table_id {
            self.last_table_id = full_key.user_key.table_id.table_id;
            switch_builder = true;
            self.last_vnode = 0;
            vnode_changed = true;
            if self.split_weight_by_vnode > 1 {
                self.largest_vnode_in_current_partition =
                    VirtualNode::COUNT / (self.split_weight_by_vnode as usize) - 1;
            }
        }
        if self.largest_vnode_in_current_partition != VirtualNode::MAX.to_index() {
            let key_vnode = full_key.user_key.get_vnode_id();
            if key_vnode != self.last_vnode {
                self.last_vnode = key_vnode;
                vnode_changed = true;
            }
            if key_vnode > self.largest_vnode_in_current_partition {
                switch_builder = true;

                // SAFETY: `self.split_weight_by_vnode > 1` here.
                let (basic, remainder) =
                    VirtualNode::COUNT.div_rem(&(self.split_weight_by_vnode as usize));
                let small_segments_area = basic * (self.split_weight_by_vnode as usize - remainder);
                self.largest_vnode_in_current_partition = (if key_vnode < small_segments_area {
                    (key_vnode / basic + 1) * basic
                } else {
                    ((key_vnode - small_segments_area) / (basic + 1) + 1) * (basic + 1)
                        + small_segments_area
                }) - 1;
                self.last_vnode = key_vnode;
                debug_assert!(key_vnode <= self.largest_vnode_in_current_partition);
            }
        }

        // We use this `need_seal_current` flag to store whether we need to call `seal_current` and
        // then call `seal_current` later outside the `if let` instead of calling
        // `seal_current` at where we set `need_seal_current = true`. This is because
        // `seal_current` is an async method, and if we call `seal_current` within the `if let`,
        // this temporary reference to `current_builder` will be captured in the future generated
        // from the current method. Since this generated future is usually required to be `Send`,
        // the captured reference to `current_builder` is also required to be `Send`, and then
        // `current_builder` itself is required to be `Sync`, which is unnecessary.
        let mut need_seal_current = false;
        if let Some(builder) = self.current_builder.as_ref() {
            if is_new_user_key {
                if switch_builder {
                    need_seal_current = true;
                } else if builder.reach_capacity() {
                    need_seal_current = self.split_weight_by_vnode == 0
                        || (self.is_target_level_l0_or_lbase && vnode_changed);
                }
            }
        }
        if need_seal_current {
            let monotonic_deletes = self
                .del_agg
                .get_tombstone_between(self.last_sealed_key.as_ref(), full_key.user_key);
            self.seal_current(monotonic_deletes).await?;
            self.last_sealed_key.extend_from_other(&full_key.user_key);
        }

        if self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress
                    .num_pending_write_io
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let builder = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder.add(full_key, value, is_new_user_key).await
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub async fn seal_current(
        &mut self,
        monotonic_deletes: Vec<MonotonicDeleteEvent>,
    ) -> HummockResult<()> {
        if let Some(mut builder) = self.current_builder.take() {
            builder.add_monotonic_deletes(monotonic_deletes);
            let builder_output = builder.finish().await?;
            {
                // report
                if let Some(progress) = &self.task_progress {
                    progress.inc_ssts_sealed();
                }

                if builder_output.bloom_filter_size != 0 {
                    self.compactor_metrics
                        .sstable_bloom_filter_size
                        .observe(builder_output.bloom_filter_size as _);
                }

                if builder_output.sst_info.file_size() != 0 {
                    self.compactor_metrics
                        .sstable_file_size
                        .observe(builder_output.sst_info.file_size() as _);
                }

                if builder_output.avg_key_size != 0 {
                    self.compactor_metrics
                        .sstable_avg_key_size
                        .observe(builder_output.avg_key_size as _);
                }

                if builder_output.avg_value_size != 0 {
                    self.compactor_metrics
                        .sstable_avg_value_size
                        .observe(builder_output.avg_value_size as _);
                }

                if builder_output.epoch_count != 0 {
                    self.compactor_metrics
                        .sstable_distinct_epoch_count
                        .observe(builder_output.epoch_count as _);
                }
            }
            self.sst_outputs.push(SplitTableOutput {
                upload_join_handle: builder_output.writer_output,
                sst_info: builder_output.sst_info,
            });
        }
        Ok(())
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub async fn finish(mut self) -> HummockResult<Vec<SplitTableOutput>> {
        let largest_user_key = if self.key_range.right.is_empty() {
            UserKey::default()
        } else {
            FullKey::decode(&self.key_range.right).user_key.to_vec()
        };
        let monotonic_deletes = self
            .del_agg
            .get_tombstone_between(self.last_sealed_key.as_ref(), largest_user_key.as_ref());
        if !monotonic_deletes.is_empty() && self.current_builder.is_none() {
            if let Some(progress) = &self.task_progress {
                progress
                    .num_pending_write_io
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let builder = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
        }
        self.seal_current(monotonic_deletes).await?;
        Ok(self.sst_outputs)
    }
}

/// Used for unit tests and benchmarks.
pub struct LocalTableBuilderFactory {
    next_id: AtomicU64,
    sstable_store: SstableStoreRef,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    limiter: MemoryLimiter,
}

impl LocalTableBuilderFactory {
    pub fn new(
        next_id: u64,
        sstable_store: SstableStoreRef,
        options: SstableBuilderOptions,
    ) -> Self {
        Self {
            next_id: AtomicU64::new(next_id),
            sstable_store,
            options,
            policy: CachePolicy::NotFill,
            limiter: MemoryLimiter::new(1000000),
        }
    }
}

#[async_trait::async_trait]
impl TableBuilderFactory for LocalTableBuilderFactory {
    type Filter = Xor16FilterBuilder;
    type Writer = BatchUploadWriter;

    async fn open_builder(
        &mut self,
    ) -> HummockResult<SstableBuilder<BatchUploadWriter, Xor16FilterBuilder>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self.limiter.require_memory(1).await;
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .sstable_store
            .clone()
            .create_sst_writer(id, writer_options);
        let builder = SstableBuilder::for_test(id, writer, self.options.clone());

        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::test_utils::{default_builder_opt_for_test, test_key_of, test_user_key_of};
    use crate::hummock::{
        create_monotonic_events, CompactionDeleteRangesBuilder, DeleteRangeTombstone,
        SstableBuilderOptions, DEFAULT_RESTART_INTERVAL,
    };

    #[tokio::test]
    async fn test_empty() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts);
        let builder = CapacitySplitTableBuilder::for_test(builder_factory);
        let results = builder.finish().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let builder_factory = LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts);
        let mut builder = CapacitySplitTableBuilder::for_test(builder_factory);

        for i in 0..table_capacity {
            builder
                .add_full_key_for_test(
                    FullKey::from_user_key(
                        test_user_key_of(i).as_ref(),
                        (table_capacity - i) as u64,
                    ),
                    HummockValue::put(b"value"),
                    true,
                )
                .await
                .unwrap();
        }

        let results = builder.finish().await.unwrap();
        assert!(results.len() > 1);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let opts = default_builder_opt_for_test();
        let mut builder = CapacitySplitTableBuilder::for_test(LocalTableBuilderFactory::new(
            1001,
            mock_sstable_store(),
            opts,
        ));
        let mut epoch = 100;

        macro_rules! add {
            () => {
                epoch -= 1;
                builder
                    .add_full_key_for_test(
                        FullKey::from_user_key(test_user_key_of(1).as_ref(), epoch),
                        HummockValue::put(b"v"),
                        true,
                    )
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current(vec![]).await.unwrap();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current(vec![]).await.unwrap();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current(vec![]).await.unwrap();
        assert_eq!(builder.len(), 2);
        builder.seal_current(vec![]).await.unwrap();
        assert_eq!(builder.len(), 2);

        let results = builder.finish().await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let opts = default_builder_opt_for_test();
        let mut builder = CapacitySplitTableBuilder::for_test(LocalTableBuilderFactory::new(
            1001,
            mock_sstable_store(),
            opts,
        ));
        builder
            .add_full_key_for_test(test_key_of(0).to_ref(), HummockValue::put(b"v"), false)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_expand_boundary_by_range_tombstone() {
        let opts = default_builder_opt_for_test();
        let table_id = TableId::default();
        let mut builder = CompactionDeleteRangesBuilder::default();
        let events = create_monotonic_events(vec![
            DeleteRangeTombstone::new(
                table_id,
                [VirtualNode::ZERO.to_be_bytes().as_slice(), b"k"]
                    .concat()
                    .to_vec(),
                false,
                [VirtualNode::ZERO.to_be_bytes().as_slice(), b"kkk"]
                    .concat()
                    .to_vec(),
                false,
                100,
            ),
            DeleteRangeTombstone::new(
                table_id,
                [VirtualNode::ZERO.to_be_bytes().as_slice(), b"aaa"]
                    .concat()
                    .to_vec(),
                false,
                [VirtualNode::ZERO.to_be_bytes().as_slice(), b"ddd"]
                    .concat()
                    .to_vec(),
                false,
                200,
            ),
        ]);
        builder.add_delete_events(events);
        let mut builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            builder.build_for_compaction(false),
            KeyRange::inf(),
            false,
            false,
            0,
        );
        builder
            .add_full_key(
                FullKey::for_test(
                    table_id,
                    &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"k"].concat(),
                    233,
                ),
                HummockValue::put(b"v"),
                false,
            )
            .await
            .unwrap();
        let mut sst_infos = builder.finish().await.unwrap();
        let key_range = sst_infos
            .pop()
            .unwrap()
            .sst_info
            .sst_info
            .key_range
            .unwrap();
        assert_eq!(
            key_range.left,
            FullKey::for_test(
                table_id,
                &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"aaa"].concat(),
                u64::MAX,
            )
            .encode()
        );
        assert_eq!(
            key_range.right,
            FullKey::for_test(
                table_id,
                &[VirtualNode::ZERO.to_be_bytes().as_slice(), b"kkk"].concat(),
                u64::MAX
            )
            .encode()
        );
    }

    #[tokio::test]
    async fn test_only_delete_range() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let opts = SstableBuilderOptions {
            capacity: table_capacity,
            block_capacity: block_size,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::None,
        };
        let table_id = TableId::new(1);
        let mut builder = CompactionDeleteRangesBuilder::default();
        builder.add_delete_events(create_monotonic_events(vec![
            DeleteRangeTombstone::new_for_test(table_id, b"k".to_vec(), b"kkk".to_vec(), 100),
            DeleteRangeTombstone::new_for_test(table_id, b"aaa".to_vec(), b"ddd".to_vec(), 200),
        ]));
        let builder = CapacitySplitTableBuilder::new(
            LocalTableBuilderFactory::new(1001, mock_sstable_store(), opts),
            Arc::new(CompactorMetrics::unused()),
            None,
            builder.build_for_compaction(false),
            KeyRange::inf(),
            false,
            false,
            0,
        );
        let results = builder.finish().await.unwrap();
        assert_eq!(results[0].sst_info.sst_info.table_ids, vec![1]);
    }
}
