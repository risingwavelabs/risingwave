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

//! Production blocked-filter restoration and lookup on real 64 KiB-block SSTs.
#[path = "blocked_xor_fixture/sstable.rs"]
mod sstable_meta_fixture;

use std::hint::black_box;
use std::ops::Bound;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::sstable::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, FilterBuilder, XorFilterReader,
};

risingwave_common::enable_jemalloc!();

fn case<F: FilterBuilder>(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    filter: &str,
    keys: usize,
) {
    let (_, meta) = runtime.block_on(
        sstable_meta_fixture::sstable_fixture_with_block_capacity::<F>(keys, 64, 64 << 10),
    );
    let data = &meta.bloom_filter;
    let metas = &meta.block_metas;
    let reader = XorFilterReader::new(data, metas);
    assert_eq!(reader.encode_to_bytes(), *data);
    let queries: Vec<_> = (0..256_u64)
        .map(|i| {
            let index = (i as usize * 7919) % metas.len();
            let left = FullKey::decode(&metas[index].smallest_key).user_key;
            let right =
                FullKey::decode(&metas[(index + 3).min(metas.len() - 1)].smallest_key).user_key;
            let hash = i.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            (
                (Bound::Included(left), Bound::Included(left)),
                (Bound::Included(left), Bound::Included(right)),
                hash,
            )
        })
        .collect();
    let label = format!("{filter}_{keys}keys_{}blocks", metas.len());
    let mut group = c.benchmark_group("blocked_xor_filter");
    group.bench_function(BenchmarkId::new("new", &label), |b| {
        // Exclude destruction of the restored reader from this measurement.
        b.iter_batched(
            || (),
            |()| XorFilterReader::new(black_box(data), black_box(metas)),
            BatchSize::PerIteration,
        );
    });
    for (name, wide) in [("point", false), ("range", true)] {
        let mut cursor = 0_usize;
        group.bench_function(BenchmarkId::new(name, &label), |b| {
            b.iter(|| {
                let (point, range, hash) = &queries[cursor % queries.len()];
                cursor = cursor.wrapping_add(1);
                reader.may_match(
                    black_box(metas),
                    black_box(if wide { range } else { point }),
                    black_box(*hash),
                )
            });
        });
    }
    group.finish();
}

fn bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // The large fixture has 2234 blocks with 64-byte keys and 16-byte values.
    for keys in [16_000, 4_400_000] {
        case::<BlockedXor16FilterBuilder>(c, &runtime, "xor16", keys);
        case::<BlockedXor8FilterBuilder>(c, &runtime, "xor8", keys);
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
