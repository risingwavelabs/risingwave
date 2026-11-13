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

//! Run with `cargo bench -p risingwave_storage --bench bench_scan_init --features test`.
//!
//! Measures scan creation (including initial rewind and iterator destruction), without consuming
//! rows. Each SST contains one distinct key; metadata and data blocks are warmed before timing.
//! Compare revisions with Criterion's `--save-baseline NAME` and `--baseline NAME`, using the
//! `concurrency_1` cases to compare the original sequential path, buffered path, and fast path.

use std::collections::HashMap;
use std::hint::black_box;
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{FullKey, gen_key_from_bytes};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::{
    LevelType, PbHummockVersion, PbLevel, PbOverlappingLevel, PbStateTableInfo,
};
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::store::version::HummockVersionReader;
use risingwave_storage::hummock::test_utils::{
    default_builder_opt_for_test, gen_test_sstable_with_table_ids,
};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::monitor::{HummockStateStoreMetrics, StoreLocalStatistic};
use risingwave_storage::store::ReadOptions;
use tokio::sync::mpsc::unbounded_channel;

risingwave_common::enable_jemalloc!();

fn committed_version(table_id: TableId, ssts: &[SstableInfo]) -> PinnedVersion {
    let group_id = StaticCompactionGroupId::NewCompactionGroup;
    let version = PbHummockVersion {
        id: 1.into(),
        levels: HashMap::from_iter([(
            group_id,
            PbLevels {
                group_id,
                l0: Some(PbOverlappingLevel {
                    sub_levels: vec![PbLevel {
                        level_type: LevelType::Overlapping as i32,
                        table_infos: ssts.iter().cloned().map(Into::into).collect(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )]),
        state_table_info: HashMap::from_iter([(
            table_id,
            PbStateTableInfo {
                committed_epoch: test_epoch(1),
                compaction_group_id: group_id,
            },
        )]),
        ..Default::default()
    };
    PinnedVersion::new(HummockVersion::from(&version), unbounded_channel().0)
}

fn bench_scan_init(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let table_id = TableId::new(42);
    let store = runtime.block_on(mock_sstable_store());
    let ssts = runtime.block_on(async {
        let mut ssts = Vec::new();
        for id in 1_u64..=1024 {
            let (_, info) = gen_test_sstable_with_table_ids(
                default_builder_opt_for_test(),
                id,
                std::iter::once((
                    FullKey::new_with_gap_epoch(
                        table_id,
                        gen_key_from_bytes(VirtualNode::ZERO, &id.to_be_bytes()),
                        EpochWithGap::new_from_epoch(test_epoch(1)),
                    ),
                    HummockValue::put(Bytes::from_static(b"value")),
                )),
                store.clone(),
                vec![table_id.as_raw_id()],
            )
            .await;
            ssts.push(info);
        }
        ssts
    });

    let mut group = c.benchmark_group("scan_init_warm_cache");
    for count in [16, 128, 1024] {
        group.throughput(Throughput::Elements(count as u64));
        for layout in ["staging", "overlapping"] {
            let (staging, committed) = if layout == "staging" {
                (ssts[..count].to_vec(), committed_version(table_id, &[]))
            } else {
                (vec![], committed_version(table_id, &ssts[..count]))
            };
            for concurrency in [1, 8] {
                let reader = HummockVersionReader::new(
                    store.clone(),
                    Arc::new(HummockStateStoreMetrics::unused()),
                    concurrency,
                );
                let init = || async {
                    reader
                        .iter(
                            (Bound::Unbounded, Bound::Unbounded),
                            test_epoch(1),
                            table_id,
                            TableOption::default(),
                            ReadOptions::default(),
                            (vec![], staging.clone(), committed.clone()),
                        )
                        .await
                        .unwrap()
                };
                // Warm both caches so the measurement exposes CPU overhead in scan creation.
                runtime.block_on(async { drop(init().await) });
                runtime.block_on(async {
                    let mut stats = StoreLocalStatistic::default();
                    for sst in &ssts[..count] {
                        black_box(store.sstable(sst, &mut stats).await.unwrap());
                    }
                    assert_eq!(stats.cache_meta_block_total, count as u64);
                    assert_eq!(stats.cache_meta_block_miss, 0);
                    stats.discard();
                });
                group.bench_function(
                    BenchmarkId::new(format!("{layout}/concurrency_{concurrency}"), count),
                    |b| {
                        b.to_async(&runtime).iter(|| async {
                            drop(black_box(init().await));
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_scan_init
}
criterion_main!(benches);
