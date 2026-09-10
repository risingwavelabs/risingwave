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

use std::hint::black_box;
use std::time::Duration;

use criterion::async_executor::FuturesExecutor;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::{FullKey, TABLE_PREFIX_LEN};
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::sstable::{
    BlockBuilder, BlockBuilderOptions, CompressionAlgorithm, FilterBuilder, InMemWriter,
    NoneFilterBuilder, SstableBuilder, SstableBuilderOptions, Xor8FilterBuilder,
    Xor16FilterBuilder,
};
use risingwave_storage::hummock::value::HummockValue;

// Generate sorted keys outside the timed loop. Keep either the full padding or only the vnode
// common between neighboring keys, without changing the key length.
fn keys(count: usize, len: usize, shared_prefix: bool) -> Vec<FullKey<Vec<u8>>> {
    (0..count)
        .map(|i| {
            let mut key = VirtualNode::ZERO.to_be_bytes().to_vec();
            if !shared_prefix {
                key.extend_from_slice(&(i as u64).to_be_bytes());
            }
            key.resize(len - 8, b'k');
            key.extend_from_slice(&(i as u64).to_be_bytes());
            FullKey::for_test(TableId::default(), key, test_epoch(1))
        })
        .collect()
}

fn bench_block_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("block_builder");
    let count = 256;
    group.throughput(Throughput::Elements(count as u64));
    for (key_len, shared_prefix) in [(32, false), (32, true), (256, false), (256, true)] {
        let keys: Vec<_> = keys(count, key_len, shared_prefix)
            .into_iter()
            .map(|key| key.encode())
            .collect();
        let value = [0; 32];
        let mut builder = BlockBuilder::new(BlockBuilderOptions {
            capacity: 128 * 1024,
            restart_interval: 16,
            compression_algorithm: CompressionAlgorithm::None,
        });
        group.bench_function(
            BenchmarkId::new(format!("key_{key_len}"), format!("shared_{shared_prefix}")),
            |b| {
                b.iter(|| {
                    for key in &keys {
                        builder.add(
                            TableId::default(),
                            black_box(&key[TABLE_PREFIX_LEN..]),
                            black_box(&value),
                        );
                    }
                    black_box(builder.build());
                    builder.clear();
                });
            },
        );
    }
    group.finish();
}

fn bench_sstable_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_builder");
    let count = 4096;
    group.throughput(Throughput::Elements(count as u64));
    let catalog = CompactionCatalogAgent::for_test(vec![0]);
    // No filter here: measure the key/value path separately from filter construction. The full
    // SST, including its metadata, is materialized by the existing in-memory writer.
    for (key_len, value_len, compression) in [
        (32, 8, CompressionAlgorithm::None),
        (256, 8, CompressionAlgorithm::None),
        (32, 256, CompressionAlgorithm::None),
        (32, 4096, CompressionAlgorithm::None),
        (32, 256, CompressionAlgorithm::Lz4),
        (32, 256, CompressionAlgorithm::Zstd),
    ] {
        let keys = keys(count, key_len, true);
        // Fixed payload, generated once. Compression results are workload-specific.
        let value: Vec<_> = (0..value_len).map(|i| (i * 31) as u8).collect();
        let options = SstableBuilderOptions {
            capacity: count * (key_len + value_len + 32),
            block_capacity: 64 * 1024,
            compression_algorithm: compression,
            ..Default::default()
        };
        group.bench_function(
            BenchmarkId::new(
                format!("key_{key_len}_value_{value_len}"),
                format!("{compression:?}"),
            ),
            |b| {
                b.to_async(FuturesExecutor).iter(|| async {
                    let mut builder = SstableBuilder::new(
                        1,
                        InMemWriter::from(&options),
                        NoneFilterBuilder,
                        options.clone(),
                        catalog.clone(),
                        None,
                    );
                    for key in &keys {
                        builder
                            .add(
                                black_box(key.to_ref()),
                                HummockValue::put(black_box(&value[..])),
                            )
                            .await
                            .unwrap();
                    }
                    black_box(builder.finish().await.unwrap());
                });
            },
        );
    }
    group.finish();
}

fn bench_filter<F: FilterBuilder>(c: &mut Criterion, name: &str) {
    let mut group = c.benchmark_group(name);
    for count in [1024, 256 * 1024] {
        group.throughput(Throughput::Elements(count as u64));
        for repeat in [1, 4] {
            let keys: Vec<_> = (0..count)
                .map(|i| ((i / repeat) as u64).to_be_bytes())
                .collect();
            let options = SstableBuilderOptions {
                estimated_output_key_count: Some(count),
                ..Default::default()
            }
            .filter_builder_options();
            group.bench_function(
                BenchmarkId::new(count.to_string(), format!("repeat_{repeat}")),
                |b| {
                    // Input hashing and allocation are setup costs; time sorting, deduplication,
                    // xor construction and serialization in finish, not just a stand-in sort.
                    b.iter_batched_ref(
                        || {
                            let mut builder = F::create(options);
                            for key in &keys {
                                builder.add_key(key, 0);
                            }
                            builder
                        },
                        |builder| black_box(builder.finish(None)),
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    group.finish();
}

fn bench_filters(c: &mut Criterion) {
    bench_filter::<Xor8FilterBuilder>(c, "xor8_finish");
    bench_filter::<Xor16FilterBuilder>(c, "xor16_finish");
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_block_builder, bench_sstable_builder, bench_filters
}
criterion_main!(benches);
