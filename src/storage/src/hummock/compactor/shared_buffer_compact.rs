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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::try_join_all;
use futures::{stream, StreamExt, TryFutureExt};
use itertools::Itertools;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorImpl;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::{CompactionGroupId, HummockEpoch};
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::compaction_filter::DummyCompactionFilter;
use crate::hummock::compactor::context::Context;
use crate::hummock::compactor::{CompactOutput, Compactor};
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::shared_buffer::{build_ordered_merge_iter, UncommittedData};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::state_store::ForwardIter;
use crate::hummock::{CachePolicy, HummockError, HummockResult};
use crate::monitor::StoreLocalStatistic;

/// Flush shared buffer to level0. Resulted SSTs are grouped by compaction group.
pub async fn compact(
    context: Arc<Context>,
    payload: UploadTaskPayload,
    sst_watermark_epoch: HummockEpoch,
) -> HummockResult<Vec<(CompactionGroupId, SstableInfo)>> {
    let mut grouped_payload: HashMap<CompactionGroupId, UploadTaskPayload> = HashMap::new();
    for uncommitted_list in payload {
        let mut next_inner = HashSet::new();
        for uncommitted in uncommitted_list {
            let compaction_group_id = match &uncommitted {
                UncommittedData::Sst((compaction_group_id, _)) => *compaction_group_id,
                UncommittedData::Batch(batch) => batch.compaction_group_id(),
            };
            let group = grouped_payload
                .entry(compaction_group_id)
                .or_insert_with(std::vec::Vec::new);
            if !next_inner.contains(&compaction_group_id) {
                group.push(vec![]);
                next_inner.insert(compaction_group_id);
            }
            group.last_mut().unwrap().push(uncommitted);
        }
    }

    let mut futures = vec![];
    for (id, group_payload) in grouped_payload {
        let id_copy = id;
        futures.push(
            compact_shared_buffer(context.clone(), group_payload, sst_watermark_epoch).map_ok(
                move |results| {
                    results
                        .into_iter()
                        .map(move |result| (id_copy, result))
                        .collect_vec()
                },
            ),
        );
    }
    // Note that the output is reordered compared with input `payload`.
    let result = try_join_all(futures)
        .await?
        .into_iter()
        .flatten()
        .collect_vec();
    Ok(result)
}

/// For compaction from shared buffer to level 0, this is the only function gets called.
async fn compact_shared_buffer(
    context: Arc<Context>,
    payload: UploadTaskPayload,
    sst_watermark_epoch: HummockEpoch,
) -> HummockResult<Vec<SstableInfo>> {
    let mut start_user_keys = payload
        .iter()
        .flat_map(|data_list| data_list.iter().map(UncommittedData::start_user_key))
        .collect_vec();
    start_user_keys.sort();
    start_user_keys.dedup();
    let mut splits = Vec::with_capacity(start_user_keys.len());
    splits.push(KeyRange::new(Bytes::new(), Bytes::new()));
    let mut key_split_append = |key_before_last: &Bytes| {
        splits.last_mut().unwrap().right = key_before_last.clone();
        splits.push(KeyRange::new(key_before_last.clone(), Bytes::new()));
    };
    if start_user_keys.len() > 1 {
        let split_num = context.options.share_buffers_sync_parallelism as usize;
        let buffer_per_split = start_user_keys.len() / split_num;
        for i in 1..split_num {
            key_split_append(
                &FullKey::from_user_key_slice(
                    start_user_keys[i * buffer_per_split],
                    HummockEpoch::MAX,
                )
                .into_inner()
                .into(),
            );
        }
    }

    let existing_table_ids: HashSet<u32> = payload
        .iter()
        .flat_map(|data_list| {
            data_list
                .iter()
                .flat_map(|uncommitted_data| match uncommitted_data {
                    UncommittedData::Sst(local_sst_info) => local_sst_info.1.table_ids.clone(),

                    UncommittedData::Batch(shared_buffer_write_batch) => {
                        vec![shared_buffer_write_batch.table_id]
                    }
                })
        })
        .dedup()
        .collect();

    assert!(!existing_table_ids.is_empty());

    let multi_filter_key_extractor = context
        .filter_key_extractor_manager
        .acquire(existing_table_ids)
        .await;
    let multi_filter_key_extractor = Arc::new(multi_filter_key_extractor);

    // Local memory compaction looks at all key ranges.
    let sstable_store = context.sstable_store.clone();
    let stats = context.stats.clone();

    let parallelism = splits.len();
    let mut compact_success = true;
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];

    let mut local_stats = StoreLocalStatistic::default();
    for (split_index, key_range) in splits.into_iter().enumerate() {
        let compactor = SharedBufferCompactRunner::new(
            split_index,
            key_range,
            context.clone(),
            sst_watermark_epoch,
        );
        let iter = build_ordered_merge_iter::<ForwardIter>(
            &payload,
            sstable_store.clone(),
            stats.clone(),
            &mut local_stats,
            Arc::new(SstableIteratorReadOptions::default()),
        )
        .await?;
        let compaction_executor = context.compaction_executor.clone();
        let multi_filter_key_extractor = multi_filter_key_extractor.clone();
        let handle = compaction_executor
            .execute(async move { compactor.run(iter, multi_filter_key_extractor).await });
        compaction_futures.push(handle);
    }
    local_stats.report(stats.as_ref());

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    let mut err = None;
    while let Some(future_result) = buffered.next().await {
        match future_result {
            Ok(Ok((split_index, ssts))) => {
                output_ssts.push((split_index, ssts));
            }
            Ok(Err(e)) => {
                compact_success = false;
                tracing::warn!("Shared Buffer Compaction failed with error: {:#?}", e);
                err = Some(e);
            }
            Err(e) => {
                compact_success = false;
                tracing::warn!(
                    "Shared Buffer Compaction failed with future error: {:#?}",
                    e
                );
                err = Some(HummockError::compaction_executor(
                    "failed while execute in tokio",
                ));
            }
        }
    }

    // Sort by split/key range index.
    output_ssts.sort_by_key(|(split_index, _)| *split_index);

    if compact_success {
        let mut level0 = Vec::with_capacity(parallelism);

        for (_, ssts) in output_ssts {
            for sst_info in &ssts {
                context
                    .stats
                    .write_build_l0_bytes
                    .inc_by(sst_info.file_size as u64);
            }
            level0.extend(ssts);
        }

        Ok(level0)
    } else {
        Err(err.unwrap())
    }
}

pub struct SharedBufferCompactRunner {
    compactor: Compactor,
    split_index: usize,
}

impl SharedBufferCompactRunner {
    pub fn new(
        split_index: usize,
        key_range: KeyRange,
        context: Arc<Context>,
        sst_watermark_epoch: HummockEpoch,
    ) -> Self {
        let options = context.options.as_ref().into();
        let compactor = Compactor::new(
            context,
            options,
            key_range,
            CachePolicy::Fill,
            false,
            sst_watermark_epoch,
        );
        Self {
            compactor,
            split_index,
        }
    }

    pub async fn run(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        filter_key_extractor: Arc<FilterKeyExtractorImpl>,
    ) -> HummockResult<CompactOutput> {
        let dummy_compaction_filter = DummyCompactionFilter {};
        let ssts = self
            .compactor
            .compact_key_range_impl(iter, dummy_compaction_filter, filter_key_extractor, None)
            .await?;
        Ok((self.split_index, ssts))
    }
}
