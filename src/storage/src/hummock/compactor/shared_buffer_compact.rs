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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use await_tree::InstrumentAwait;
use bytes::Bytes;
use foyer::memory::CacheContext;
use futures::future::{try_join, try_join_all};
use futures::{stream, FutureExt, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, FullKeyTracker, UserKey, EPOCH_LEN};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{CompactionGroupId, EpochWithGap, LocalSstableInfo};
use risingwave_pb::hummock::compact_task;
use thiserror_ext::AsReport;
use tracing::{error, warn};

use crate::filter_key_extractor::{FilterKeyExtractorImpl, FilterKeyExtractorManager};
use crate::hummock::compactor::compaction_filter::DummyCompactionFilter;
use crate::hummock::compactor::context::{await_tree_key, CompactorContext};
use crate::hummock::compactor::{check_flush_result, CompactOutput, Compactor};
use crate::hummock::event_handler::uploader::{UploadTaskOutput, UploadTaskPayload};
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::{Forward, HummockIterator, MergeIterator, UserIterator};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchInner, SharedBufferBatchOldValues, SharedBufferKeyEntry,
    VersionedSharedBufferValue,
};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, GetObjectId, HummockError, HummockResult,
    SstableBuilderOptions, SstableObjectIdManagerRef,
};
use crate::mem_table::ImmutableMemtable;
use crate::opts::StorageOpts;

const GC_DELETE_KEYS_FOR_FLUSH: bool = false;
const GC_WATERMARK_FOR_FLUSH: u64 = 0;

/// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
pub async fn compact(
    context: CompactorContext,
    sstable_object_id_manager: SstableObjectIdManagerRef,
    payload: UploadTaskPayload,
    compaction_group_index: Arc<HashMap<TableId, CompactionGroupId>>,
    filter_key_extractor_manager: FilterKeyExtractorManager,
) -> HummockResult<UploadTaskOutput> {
    let mut grouped_payload: HashMap<CompactionGroupId, UploadTaskPayload> = HashMap::new();
    for imm in &payload {
        let compaction_group_id = match compaction_group_index.get(&imm.table_id) {
            // compaction group id is used only as a hint for grouping different data.
            // If the compaction group id is not found for the table id, we can assign a
            // default compaction group id for the batch.
            //
            // On meta side, when we commit a new epoch, it is acceptable that the
            // compaction group id provided from CN does not match the latest compaction
            // group config.
            None => StaticCompactionGroupId::StateDefault as CompactionGroupId,
            Some(group_id) => *group_id,
        };
        grouped_payload
            .entry(compaction_group_id)
            .or_default()
            .push(imm.clone());
    }

    let mut new_value_futures = vec![];
    for (id, group_payload) in grouped_payload {
        let id_copy = id;
        new_value_futures.push(
            compact_shared_buffer::<true>(
                context.clone(),
                sstable_object_id_manager.clone(),
                filter_key_extractor_manager.clone(),
                group_payload,
            )
            .map_ok(move |results| {
                results
                    .into_iter()
                    .map(move |mut result| {
                        result.compaction_group_id = id_copy;
                        result
                    })
                    .collect_vec()
            })
            .instrument_await(format!("shared_buffer_compact_compaction_group {}", id)),
        );
    }

    let old_value_payload = payload
        .into_iter()
        .filter(|imm| imm.has_old_value())
        .collect_vec();

    let old_value_future = async {
        if old_value_payload.is_empty() {
            Ok(vec![])
        } else {
            compact_shared_buffer::<false>(
                context.clone(),
                sstable_object_id_manager,
                filter_key_extractor_manager,
                old_value_payload,
            )
            .await
        }
    };

    // Note that the output is reordered compared with input `payload`.
    let (grouped_new_value_ssts, old_value_ssts) =
        try_join(try_join_all(new_value_futures), old_value_future).await?;

    let new_value_ssts = grouped_new_value_ssts.into_iter().flatten().collect_vec();
    Ok(UploadTaskOutput {
        new_value_ssts,
        old_value_ssts,
        wait_poll_timer: None,
    })
}

/// For compaction from shared buffer to level 0, this is the only function gets called.
///
/// The `IS_NEW_VALUE` flag means for the given payload, we are doing compaction using its new value or old value.
/// When `IS_NEW_VALUE` is false, we are compacting with old value, and the payload imms should have `old_values` not `None`
async fn compact_shared_buffer<const IS_NEW_VALUE: bool>(
    context: CompactorContext,
    sstable_object_id_manager: SstableObjectIdManagerRef,
    filter_key_extractor_manager: FilterKeyExtractorManager,
    mut payload: UploadTaskPayload,
) -> HummockResult<Vec<LocalSstableInfo>> {
    if !IS_NEW_VALUE {
        assert!(payload.iter().all(|imm| imm.has_old_value()));
    }
    // Local memory compaction looks at all key ranges.

    let mut existing_table_ids: HashSet<u32> = payload
        .iter()
        .map(|imm| imm.table_id.table_id)
        .dedup()
        .collect();
    assert!(!existing_table_ids.is_empty());

    let multi_filter_key_extractor = filter_key_extractor_manager
        .acquire(existing_table_ids.clone())
        .await?;
    if let FilterKeyExtractorImpl::Multi(multi) = &multi_filter_key_extractor {
        existing_table_ids = multi.get_existing_table_ids();
    }
    let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);

    payload.retain(|imm| {
        let ret = existing_table_ids.contains(&imm.table_id.table_id);
        if !ret {
            error!(
                "can not find table {:?}, it may be removed by meta-service",
                imm.table_id
            );
        }
        ret
    });

    let total_key_count = payload.iter().map(|imm| imm.key_count()).sum::<usize>();
    let (splits, sub_compaction_sstable_size, split_weight_by_vnode) =
        generate_splits(&payload, &existing_table_ids, context.storage_opts.as_ref());
    let parallelism = splits.len();
    let mut compact_success = true;
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];
    let use_block_based_filter = BlockedXor16FilterBuilder::is_kv_count_too_large(total_key_count);

    let table_vnode_partition = if existing_table_ids.len() == 1 {
        let table_id = existing_table_ids.iter().next().unwrap();
        vec![(*table_id, split_weight_by_vnode)]
            .into_iter()
            .collect()
    } else {
        BTreeMap::default()
    };
    for (split_index, key_range) in splits.into_iter().enumerate() {
        let compactor = SharedBufferCompactRunner::new(
            split_index,
            key_range,
            context.clone(),
            sub_compaction_sstable_size as usize,
            table_vnode_partition.clone(),
            use_block_based_filter,
            Box::new(sstable_object_id_manager.clone()),
        );
        let mut forward_iters = Vec::with_capacity(payload.len());
        for imm in &payload {
            forward_iters.push(imm.clone().into_directed_iter::<Forward, IS_NEW_VALUE>());
        }
        let compaction_executor = context.compaction_executor.clone();
        let multi_filter_key_extractor = multi_filter_key_extractor.clone();
        let handle = compaction_executor.spawn({
            static NEXT_SHARED_BUFFER_COMPACT_ID: LazyLock<AtomicUsize> =
                LazyLock::new(|| AtomicUsize::new(0));
            let tree_root = context.await_tree_reg.as_ref().map(|reg| {
                let id = NEXT_SHARED_BUFFER_COMPACT_ID.fetch_add(1, Relaxed);
                reg.register(
                    await_tree_key::CompactSharedBuffer { id },
                    format!(
                        "Compact Shared Buffer: {:?}",
                        payload
                            .iter()
                            .flat_map(|imm| imm.epochs().iter())
                            .copied()
                            .collect::<BTreeSet<_>>()
                    ),
                )
            });
            let future = compactor.run(
                MergeIterator::new(forward_iters),
                multi_filter_key_extractor,
            );
            if let Some(root) = tree_root {
                root.instrument(future).left_future()
            } else {
                future.right_future()
            }
        });
        compaction_futures.push(handle);
    }

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    let mut err = None;
    while let Some(future_result) = buffered.next().await {
        match future_result {
            Ok(Ok((split_index, ssts, table_stats_map))) => {
                output_ssts.push((split_index, ssts, table_stats_map));
            }
            Ok(Err(e)) => {
                compact_success = false;
                tracing::warn!(error = %e.as_report(), "Shared Buffer Compaction failed with error");
                err = Some(e);
            }
            Err(e) => {
                compact_success = false;
                tracing::warn!(
                    error = %e.as_report(),
                    "Shared Buffer Compaction failed with future error",
                );
                err = Some(HummockError::compaction_executor(
                    "failed while execute in tokio",
                ));
            }
        }
    }

    // Sort by split/key range index.
    output_ssts.sort_by_key(|(split_index, ..)| *split_index);

    if compact_success {
        let mut level0 = Vec::with_capacity(parallelism);
        let mut sst_infos = vec![];
        for (_, ssts, _) in output_ssts {
            for sst_info in &ssts {
                context
                    .compactor_metrics
                    .write_build_l0_bytes
                    .inc_by(sst_info.file_size());
                sst_infos.push(sst_info.sst_info.clone());
            }
            level0.extend(ssts);
        }
        if context.storage_opts.check_compaction_result {
            let compaction_executor = context.compaction_executor.clone();
            let mut forward_iters = Vec::with_capacity(payload.len());
            for imm in &payload {
                if !existing_table_ids.contains(&imm.table_id.table_id) {
                    continue;
                }
                forward_iters.push(imm.clone().into_forward_iter());
            }
            let iter = MergeIterator::new(forward_iters);
            let left_iter = UserIterator::new(
                iter,
                (Bound::Unbounded, Bound::Unbounded),
                u64::MAX,
                0,
                None,
            );
            compaction_executor.spawn(async move {
                match check_flush_result(
                    left_iter,
                    Vec::from_iter(existing_table_ids.iter().cloned()),
                    sst_infos,
                    context,
                )
                .await
                {
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), "Failed check flush result of memtable");
                    }
                    Ok(true) => (),
                    Ok(false) => {
                        panic!(
                            "failed to check flush result consistency of state-table {:?}",
                            existing_table_ids
                        );
                    }
                }
            });
        }
        Ok(level0)
    } else {
        Err(err.unwrap())
    }
}

/// Merge multiple batches into a larger one
pub async fn merge_imms_in_memory(
    table_id: TableId,
    instance_id: LocalInstanceId,
    imms: Vec<ImmutableMemtable>,
    memory_tracker: Option<MemoryTracker>,
) -> ImmutableMemtable {
    let mut epochs = vec![];
    let mut merged_size = 0;
    assert!(imms.iter().rev().map(|imm| imm.batch_id()).is_sorted());
    let max_imm_id = imms[0].batch_id();

    let has_old_value = imms[0].has_old_value();
    // TODO: make sure that the corner case on switch_op_consistency is handled
    // If the imm of a table id contains old value, all other imm of the same table id should have old value
    assert!(imms.iter().all(|imm| imm.has_old_value() == has_old_value));

    let (old_value_size, global_old_value_size) = if has_old_value {
        (
            imms.iter()
                .map(|imm| imm.old_values().expect("has old value").size)
                .sum(),
            Some(
                imms[0]
                    .old_values()
                    .expect("has old value")
                    .global_old_value_size
                    .clone(),
            ),
        )
    } else {
        (0, None)
    };

    let mut imm_iters = Vec::with_capacity(imms.len());
    let key_count = imms.iter().map(|imm| imm.key_count()).sum();
    let value_count = imms.iter().map(|imm| imm.value_count()).sum();
    for imm in imms {
        assert!(imm.key_count() > 0, "imm should not be empty");
        assert_eq!(
            table_id,
            imm.table_id(),
            "should only merge data belonging to the same table"
        );

        epochs.push(imm.min_epoch());
        merged_size += imm.size();

        imm_iters.push(imm.into_forward_iter());
    }
    epochs.sort();

    // use merge iterator to merge input imms
    let mut mi = MergeIterator::new(imm_iters);
    mi.rewind_no_await();
    assert!(mi.is_valid());

    let first_item_key = mi.current_key_entry().key.clone();

    let mut merged_entries: Vec<SharedBufferKeyEntry> = Vec::with_capacity(key_count);
    let mut values: Vec<VersionedSharedBufferValue> = Vec::with_capacity(value_count);
    let mut old_values: Option<Vec<Bytes>> = if has_old_value {
        Some(Vec::with_capacity(value_count))
    } else {
        None
    };

    merged_entries.push(SharedBufferKeyEntry {
        key: first_item_key.clone(),
        value_offset: 0,
    });

    // Use first key, max epoch to initialize the tracker to ensure that the check first call to full_key_tracker.observe will succeed
    let mut full_key_tracker = FullKeyTracker::<Bytes>::new(FullKey::new_with_gap_epoch(
        table_id,
        first_item_key,
        EpochWithGap::new_max_epoch(),
    ));

    while mi.is_valid() {
        let key_entry = mi.current_key_entry();
        let user_key = UserKey {
            table_id,
            table_key: key_entry.key.clone(),
        };
        if full_key_tracker.observe_multi_version(
            user_key,
            key_entry
                .new_values
                .iter()
                .map(|(epoch_with_gap, _)| *epoch_with_gap),
        ) {
            let last_entry = merged_entries.last_mut().expect("non-empty");
            if last_entry.value_offset == values.len() {
                warn!(key = ?last_entry.key, "key has no value in imm compact. skipped");
                last_entry.key = full_key_tracker.latest_user_key().table_key.clone();
            } else {
                // Record kv entries
                merged_entries.push(SharedBufferKeyEntry {
                    key: full_key_tracker.latest_user_key().table_key.clone(),
                    value_offset: values.len(),
                });
            }
        }
        values.extend(
            key_entry
                .new_values
                .iter()
                .map(|(epoch_with_gap, value)| (*epoch_with_gap, value.clone())),
        );
        if let Some(old_values) = &mut old_values {
            old_values.extend(key_entry.old_values.expect("should exist").iter().cloned())
        }
        mi.advance_peek_to_next_key();
        // Since there is no blocking point in this method, but it is cpu intensive, we call this method
        // to do cooperative scheduling
        tokio::task::consume_budget().await;
    }

    let old_values = old_values.map(|old_values| {
        SharedBufferBatchOldValues::new(
            old_values,
            old_value_size,
            global_old_value_size.expect("should exist when has old value"),
        )
    });

    SharedBufferBatch {
        inner: Arc::new(SharedBufferBatchInner::new_with_multi_epoch_batches(
            epochs,
            merged_entries,
            values,
            old_values,
            merged_size,
            max_imm_id,
            memory_tracker,
        )),
        table_id,
        instance_id,
    }
}

///  Based on the incoming payload and opts, calculate the sharding method and sstable size of shared buffer compaction.
fn generate_splits(
    payload: &UploadTaskPayload,
    existing_table_ids: &HashSet<u32>,
    storage_opts: &StorageOpts,
) -> (Vec<KeyRange>, u64, u32) {
    let mut size_and_start_user_keys = vec![];
    let mut compact_data_size = 0;
    for imm in payload {
        let data_size = {
            // calculate encoded bytes of key var length
            (imm.value_count() * EPOCH_LEN + imm.size()) as u64
        };
        compact_data_size += data_size;
        size_and_start_user_keys.push((data_size, imm.start_user_key()));
    }
    size_and_start_user_keys.sort_by(|a, b| a.1.cmp(&b.1));
    let mut splits = Vec::with_capacity(size_and_start_user_keys.len());
    splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
    let mut key_split_append = |key_before_last: &Bytes| {
        splits.last_mut().unwrap().right = key_before_last.clone();
        splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
    };
    let sstable_size = (storage_opts.sstable_size_mb as u64) << 20;
    let parallel_compact_size = (storage_opts.parallel_compact_size_mb as u64) << 20;
    let parallelism = std::cmp::min(
        storage_opts.share_buffers_sync_parallelism as u64,
        size_and_start_user_keys.len() as u64,
    );
    let sub_compaction_data_size = if compact_data_size > parallel_compact_size && parallelism > 1 {
        compact_data_size / parallelism
    } else {
        compact_data_size
    };

    let mut vnode_partition_count = 0;
    if existing_table_ids.len() > 1 {
        if parallelism > 1 && compact_data_size > sstable_size {
            let mut last_buffer_size = 0;
            let mut last_user_key: UserKey<Vec<u8>> = UserKey::default();
            for (data_size, user_key) in size_and_start_user_keys {
                if last_buffer_size >= sub_compaction_data_size
                    && last_user_key.as_ref() != user_key
                {
                    last_user_key.set(user_key);
                    key_split_append(
                        &FullKey {
                            user_key,
                            epoch_with_gap: EpochWithGap::new_max_epoch(),
                        }
                        .encode()
                        .into(),
                    );
                    last_buffer_size = data_size;
                } else {
                    last_user_key.set(user_key);
                    last_buffer_size += data_size;
                }
            }
        }
    } else {
        // Collect vnodes in imm
        let mut vnodes = vec![];
        for imm in payload {
            vnodes.extend(imm.collect_vnodes());
        }
        vnodes.sort();
        vnodes.dedup();

        // Based on the estimated `vnode_avg_size`, calculate the required `vnode_partition_count` to avoid small files and further align
        const MIN_SSTABLE_SIZE: u64 = 16 * 1024 * 1024;
        if compact_data_size >= MIN_SSTABLE_SIZE && !vnodes.is_empty() {
            let mut avg_vnode_size = compact_data_size / (vnodes.len() as u64);
            vnode_partition_count = VirtualNode::COUNT;
            while avg_vnode_size < MIN_SSTABLE_SIZE && vnode_partition_count > 0 {
                vnode_partition_count /= 2;
                avg_vnode_size *= 2;
            }
        }
    }

    // mul 1.2 for other extra memory usage.
    // Ensure that the size of each sstable is still less than `sstable_size` after optimization to avoid generating a huge size sstable which will affect the object store
    let sub_compaction_sstable_size = std::cmp::min(sstable_size, sub_compaction_data_size * 6 / 5);
    (
        splits,
        sub_compaction_sstable_size,
        vnode_partition_count as u32,
    )
}

pub struct SharedBufferCompactRunner {
    compactor: Compactor,
    split_index: usize,
}

impl SharedBufferCompactRunner {
    pub fn new(
        split_index: usize,
        key_range: KeyRange,
        context: CompactorContext,
        sub_compaction_sstable_size: usize,
        table_vnode_partition: BTreeMap<u32, u32>,
        use_block_based_filter: bool,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.capacity = sub_compaction_sstable_size;
        let compactor = Compactor::new(
            context,
            options,
            super::TaskConfig {
                key_range,
                cache_policy: CachePolicy::Fill(CacheContext::Default),
                gc_delete_keys: GC_DELETE_KEYS_FOR_FLUSH,
                watermark: GC_WATERMARK_FOR_FLUSH,
                stats_target_table_ids: None,
                task_type: compact_task::TaskType::SharedBuffer,
                is_target_l0_or_lbase: true,
                table_vnode_partition,
                use_block_based_filter,
                table_schemas: Default::default(),
                disable_drop_column_optimization: false,
            },
            object_id_getter,
        );
        Self {
            compactor,
            split_index,
        }
    }

    pub async fn run(
        self,
        iter: impl HummockIterator<Direction = Forward>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        let (ssts, table_stats_map) = self
            .compactor
            .compact_key_range(
                iter,
                dummy_compaction_filter,
                filter_key_extractor,
                None,
                None,
                None,
            )
            .await?;
        Ok((self.split_index, ssts, table_stats_map))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{prefix_slice_with_vnode, TableKey};

    use crate::hummock::compactor::shared_buffer_compact::generate_splits;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferValue;
    use crate::mem_table::ImmutableMemtable;
    use crate::opts::StorageOpts;

    fn generate_key(key: &str) -> TableKey<Bytes> {
        TableKey(prefix_slice_with_vnode(
            VirtualNode::from_index(1),
            key.as_bytes(),
        ))
    }

    #[tokio::test]
    async fn test_generate_splits_in_order() {
        let imm1 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("dddd"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            1024 * 1024,
            TableId::new(1),
        );
        let imm2 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("abb"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 256) * 1024,
            TableId::new(1),
        );

        let imm3 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(2),
            0,
            vec![(
                generate_key("abc"),
                SharedBufferValue::Insert(Bytes::from_static(b"v2")),
            )],
            (1024 + 512) * 1024,
            TableId::new(1),
        );
        let imm4 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("aaa"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 512) * 1024,
            TableId::new(1),
        );

        let imm5 = ImmutableMemtable::build_shared_buffer_batch_for_test(
            test_epoch(3),
            0,
            vec![(
                generate_key("aaa"),
                SharedBufferValue::Insert(Bytes::from_static(b"v3")),
            )],
            (1024 + 256) * 1024,
            TableId::new(2),
        );

        let storage_opts = StorageOpts {
            share_buffers_sync_parallelism: 3,
            parallel_compact_size_mb: 1,
            sstable_size_mb: 1,
            ..Default::default()
        };
        let payload = vec![imm1, imm2, imm3, imm4, imm5];
        let (splits, _sstable_capacity, vnode) =
            generate_splits(&payload, &HashSet::from_iter([1, 2]), &storage_opts);
        assert_eq!(
            splits.len(),
            storage_opts.share_buffers_sync_parallelism as usize
        );
        assert_eq!(vnode, 0);
        for i in 1..splits.len() {
            assert_eq!(splits[i].left, splits[i - 1].right);
            assert!(splits[i].left > splits[i - 1].left);
            assert!(splits[i].right.is_empty() || splits[i].left < splits[i].right);
        }
    }
}
