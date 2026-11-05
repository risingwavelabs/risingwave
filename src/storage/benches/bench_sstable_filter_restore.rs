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

//! Run with `cargo bench -p risingwave_storage --bench bench_sstable_filter_restore --features test`.

mod sstable_meta_fixture;

use std::hint::black_box;
use std::ops::Bound;
use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::sstable::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, Sstable, SstableMeta, Xor8FilterBuilder,
    Xor16FilterBuilder, XorFilterReader,
};
use sstable_meta_fixture::sstable_meta;

risingwave_common::enable_jemalloc!();

fn bench_sstable_filter_restore(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("sstable_filter_restore");
    let mut measure = |name: &str, meta: SstableMeta| {
        let restored = XorFilterReader::new(&meta.bloom_filter, &meta.block_metas);
        assert!(!restored.is_empty());
        assert_eq!(restored.encode_to_bytes(), meta.bloom_filter);
        eprintln!(
            "fixture={name} blocks={} filter_bytes={} xxh64={:016x}",
            meta.block_metas.len(),
            meta.bloom_filter.len(),
            xxhash_rust::xxh64::xxh64(&meta.bloom_filter, 0)
        );
        group.throughput(Throughput::Bytes(meta.bloom_filter.len() as u64));
        group.bench_function(name, |b| {
            // Time the production reader on real SST bytes. PerIteration drops the
            // restored reader outside timing; fixture construction and checks are excluded.
            b.iter_batched(
                || (),
                |()| {
                    XorFilterReader::new(
                        black_box(&meta.bloom_filter),
                        black_box(&meta.block_metas),
                    )
                },
                BatchSize::PerIteration,
            );
        });
        group.bench_function(BenchmarkId::new("restore_and_drop", name), |b| {
            b.iter(|| {
                drop(black_box(XorFilterReader::new(
                    black_box(&meta.bloom_filter),
                    black_box(&meta.block_metas),
                )));
            });
        });
        if restored.is_block_based_filter() {
            measure_blocked_probes(&mut group, name, &meta, &restored);
        }
    };
    measure(
        "xor8",
        runtime.block_on(sstable_meta::<Xor8FilterBuilder>(1_100_000, 64)),
    );
    measure(
        "xor16",
        runtime.block_on(sstable_meta::<Xor16FilterBuilder>(1_100_000, 64)),
    );
    measure(
        "blocked_xor8",
        runtime.block_on(sstable_meta::<BlockedXor8FilterBuilder>(1_100_000, 64)),
    );
    measure(
        "blocked_xor16",
        runtime.block_on(sstable_meta::<BlockedXor16FilterBuilder>(1_100_000, 64)),
    );
    measure(
        "blocked_xor8_large",
        runtime.block_on(sstable_meta::<BlockedXor8FilterBuilder>(4_400_000, 64)),
    );
    measure(
        "blocked_xor16_large",
        runtime.block_on(sstable_meta::<BlockedXor16FilterBuilder>(4_400_000, 64)),
    );
    group.finish();
}

fn measure_blocked_probes(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    meta: &SstableMeta,
    reader: &XorFilterReader,
) {
    let metas = &meta.block_metas;
    group.throughput(Throughput::Elements(1));
    let queries: Vec<_> = (0..256_u64)
        .map(|i| {
            let index = (i as usize * 7919) % metas.len();
            let left = FullKey::decode(&metas[index].smallest_key).user_key;
            let right =
                FullKey::decode(&metas[(index + 3).min(metas.len() - 1)].smallest_key).user_key;
            let point = (Bound::Included(left), Bound::Included(left));
            let range = (Bound::Included(left), Bound::Included(right));
            let point_hit = Sstable::hash_for_filter(&left.encode(), left.table_id.as_raw_id());
            let range_hit = Sstable::hash_for_filter(&right.encode(), right.table_id.as_raw_id());
            assert!(reader.may_match(metas, &point, point_hit));
            assert!(reader.may_match(metas, &range, range_hit));

            // Reject filter false positives so the miss benchmarks always scan their full range.
            let miss = (0..256)
                .map(|salt| (i * 256 + salt).wrapping_mul(0x9e37_79b9_7f4a_7c15))
                .find(|&hash| !reader.may_match(metas, &range, hash))
                .unwrap();
            assert!(!reader.may_match(metas, &point, miss));
            [
                (point, point_hit),
                (point, miss),
                (range, range_hit),
                (range, miss),
            ]
        })
        .collect();
    let mut query_hash = xxhash_rust::xxh64::Xxh64::new(0);
    for query in &queries {
        for (_, hash) in query {
            query_hash.update(&hash.to_le_bytes());
        }
    }
    eprintln!(
        "fixture={name} probe_queries={} query_hash={:016x}",
        queries.len(),
        query_hash.digest()
    );

    for (index, operation) in ["point_hit", "point_miss", "range_hit", "range_miss"]
        .into_iter()
        .enumerate()
    {
        group.bench_function(BenchmarkId::new(operation, name), |b| {
            let mut cursor = 0;
            b.iter(|| {
                let (range, hash) = &queries[cursor % queries.len()][index];
                cursor = cursor.wrapping_add(1);
                reader.may_match(black_box(metas), black_box(range), black_box(*hash))
            });
        });
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_sstable_filter_restore
}
criterion_main!(benches);
