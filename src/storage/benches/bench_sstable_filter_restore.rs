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
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use risingwave_storage::hummock::sstable::{
    BlockedXor16FilterBuilder, SstableMeta, Xor8FilterBuilder, Xor16FilterBuilder, XorFilterReader,
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
        "blocked_xor16",
        runtime.block_on(sstable_meta::<BlockedXor16FilterBuilder>(1_100_000, 64)),
    );
    group.finish();
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
