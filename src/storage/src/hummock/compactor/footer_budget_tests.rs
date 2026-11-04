// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::can_concat;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::level::InputLevel;
use risingwave_pb::hummock::compact_task::TaskType;
use risingwave_pb::hummock::{LevelType, PbCompactTask, PbSstableFilterType};

use super::compaction_utils::{check_compaction_result, optimize_by_copy_block};
use super::compactor_runner::CompactorRunner as NormalCompactorRunner;
use super::fast_compactor_runner::CompactorRunner as FastCompactorRunner;
use super::task_progress::TaskProgress;
use super::{CompactorContext, MultiCompactionFilter};
use crate::compaction_catalog_manager::CompactionCatalogAgent;
use crate::hummock::iterator::test_utils::mock_sstable_store;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, default_opts_for_test, gen_test_sstable_impl, test_value_of,
};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, CompressionAlgorithm, SharedComapctorObjectIdManager,
    SstableBuilderOptions,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};
use crate::opts::StorageOpts;

#[derive(Clone, Copy)]
enum Workload {
    LongKeys,
    LargeFilter,
}

#[tokio::test]
async fn test_compaction_splits_large_block_metadata() {
    check_footer_budget(Workload::LongKeys).await;
}

#[tokio::test]
async fn test_compaction_splits_large_filter() {
    check_footer_budget(Workload::LargeFilter).await;
}

async fn check_footer_budget(workload: Workload) {
    let (block_size_kb, keys_per_input, footer_limit) = match workload {
        Workload::LongKeys => (1, 3, 52 * 1024),
        Workload::LargeFilter => (16, 1000, 8 * 1024),
    };
    let storage_opts = StorageOpts {
        sstable_size_mb: 256,
        block_size_kb,
        enable_fast_compaction: true,
        ..default_opts_for_test()
    };
    let metrics = Arc::new(CompactorMetrics {
        compact_fast_runner_bytes: prometheus::IntCounter::new(
            "test_compact_fast_runner_bytes",
            "Bytes copied by the fast compaction under test",
        )
        .unwrap(),
        ..CompactorMetrics::unused()
    });
    let context = CompactorContext::new_local_compact_context(
        Arc::new(storage_opts),
        mock_sstable_store().await,
        metrics.clone(),
        None,
    );
    let mut task = build_task(&context, workload, keys_per_input).await;
    let object_ids = SharedComapctorObjectIdManager::for_test((100..200).collect());
    let catalog = CompactionCatalogAgent::for_test(vec![1]);

    for limit in [None, Some(footer_limit)] {
        task.sst_footer_size_limit_bytes = limit;
        let mut task = CompactTask::from(PbCompactTask::from(task.clone()));

        let (_, normal_outputs, _) =
            NormalCompactorRunner::new(0, context.clone(), task.clone(), object_ids.clone())
                .run(
                    MultiCompactionFilter::default(),
                    catalog.clone(),
                    Arc::new(TaskProgress::default()),
                )
                .await
                .unwrap();

        assert!(optimize_by_copy_block(&task, &context));
        let copied_before = metrics.compact_fast_runner_bytes.get();
        let (fast_outputs, _) = FastCompactorRunner::<BlockedXor16FilterBuilder, _>::new(
            context.clone(),
            task.clone(),
            catalog.clone(),
            object_ids.clone(),
            Arc::new(TaskProgress::default()),
            MultiCompactionFilter::default(),
        )
        .run()
        .await
        .unwrap();
        assert!(metrics.compact_fast_runner_bytes.get() > copied_before);

        for (path, outputs) in [("normal", normal_outputs), ("fast", fast_outputs)] {
            task.sorted_output_ssts = outputs.into_iter().map(|output| output.sst_info).collect();
            let ssts = &task.sorted_output_ssts;
            let largest_footer = ssts
                .iter()
                .map(|sst| sst.file_size - sst.meta_offset)
                .max()
                .unwrap();
            if limit.is_some() {
                assert!(ssts.len() > 1, "{path}");
                assert!(
                    largest_footer < 2 * footer_limit,
                    "{path}: {largest_footer}"
                );
            } else {
                assert_eq!(ssts.len(), 1, "{path}");
                assert!(largest_footer > footer_limit, "{path}: {largest_footer}");
                if matches!(workload, Workload::LargeFilter) {
                    let sst = context
                        .sstable_store
                        .sstable(&ssts[0], &mut StoreLocalStatistic::default())
                        .await
                        .unwrap();
                    let filter_size = sst.filter_reader.encode_to_bytes().len() as u64;
                    assert!(filter_size > footer_limit, "{path}: {filter_size}");
                    assert!(largest_footer - filter_size < footer_limit, "{path}");
                }
            }
            assert!(can_concat(ssts), "{path}");
            assert!(
                check_compaction_result(&task, context.clone(), catalog.clone())
                    .await
                    .unwrap(),
                "{path}",
            );
        }
    }
}

async fn build_task(
    context: &CompactorContext,
    workload: Workload,
    keys_per_input: usize,
) -> CompactTask {
    const INPUTS_PER_LEVEL: usize = 3;
    let options = SstableBuilderOptions {
        compression_algorithm: CompressionAlgorithm::Zstd,
        block_capacity: context.storage_opts.block_size_kb as usize * 1024,
        ..default_builder_opt_for_test()
    };
    let mut input_ssts = Vec::new();
    for level_idx in 1..=2 {
        let first_input = (level_idx as usize - 1) * INPUTS_PER_LEVEL;
        let mut table_infos = Vec::new();
        for input in first_input..first_input + INPUTS_PER_LEVEL {
            let start = input * keys_per_input;
            let rows = (start..start + keys_per_input).flat_map(|idx| workload.rows(idx));
            table_infos.push(
                gen_test_sstable_impl::<_, BlockedXor16FilterBuilder>(
                    options.clone(),
                    input as u64 + 1,
                    rows,
                    context.sstable_store.clone(),
                    CachePolicy::NotFill,
                    HashMap::from([(1, VirtualNode::COUNT_FOR_TEST)]),
                    HashMap::from([(1, None)]),
                )
                .await,
            );
        }
        input_ssts.push(InputLevel {
            level_idx,
            level_type: LevelType::Nonoverlapping,
            table_infos,
        });
    }
    CompactTask {
        input_ssts,
        splits: vec![KeyRange::inf()],
        target_level: 2,
        existing_table_ids: vec![TableId::new(1)],
        target_file_size: u64::from(context.storage_opts.sstable_size_mb) << 20,
        compression_algorithm: 2, // Zstd
        task_type: TaskType::Dynamic,
        blocked_xor_filter_kv_count_threshold: Some(0),
        sstable_filter_type: PbSstableFilterType::SstableFilterXor16,
        ..Default::default()
    }
}

impl Workload {
    fn rows(self, idx: usize) -> impl Iterator<Item = (FullKey<Vec<u8>>, HummockValue<Vec<u8>>)> {
        let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
        let latest_epoch = match self {
            Self::LongKeys => {
                key.extend(std::iter::repeat_n(b'x', 6 * 1024));
                // The first decoded key after seven raw copies spans two blocks.
                if idx == 7 { 2 } else { 1 }
            }
            Self::LargeFilter => 1,
        };
        key.extend_from_slice(&idx.to_be_bytes());
        (1..=latest_epoch).rev().map(move |epoch| {
            (
                FullKey::for_test(TableId::new(1), key.clone(), test_epoch(epoch)),
                HummockValue::put(test_value_of(idx + epoch as usize)),
            )
        })
    }
}
