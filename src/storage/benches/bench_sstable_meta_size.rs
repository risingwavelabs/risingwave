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

//! Run with `cargo bench -p risingwave_storage --bench bench_sstable_meta_size --features test`.

mod sstable_meta_fixture;

use std::hint::black_box;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use foyer::Code;
use risingwave_storage::hummock::SstableMetaCacheValue;
use risingwave_storage::hummock::sstable::{BlockedXor16FilterBuilder, Sstable};
use sstable_meta_fixture::sstable_meta;

fn bench_sstable_meta_size(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("sstable_meta_size");
    for (name, key_count, key_len) in [
        ("filter_heavy", 1_100_000, 64),
        ("long_keys", 160_000, 4096),
    ] {
        let meta = runtime.block_on(sstable_meta::<BlockedXor16FilterBuilder>(
            key_count, key_len,
        ));
        for skip_filter in [false, true] {
            let legacy = Box::new(Sstable::new(1.into(), meta.clone(), skip_filter));
            let value = SstableMetaCacheValue::from((*legacy).clone());
            let mut legacy_bytes = Vec::new();
            let mut bytes = Vec::new();
            legacy.encode(&mut legacy_bytes).unwrap();
            value.encode(&mut bytes).unwrap();
            assert_eq!(bytes, legacy_bytes);
            assert_eq!(legacy.estimated_size(), bytes.len());
            assert_eq!(value.estimated_size(), bytes.len());
            let case = format!("{name}/skip_filter_{skip_filter}");
            eprintln!(
                "fixture={case} blocks={} encoded_bytes={} xxh64={:016x}",
                meta.block_metas.len(),
                bytes.len(),
                xxhash_rust::xxh64::xxh64(&bytes, 0)
            );
            group.throughput(Throughput::Elements(1));
            // Both implementations use identical SST contents in the same binary.
            // Only size estimation is timed, including its temporary allocations/copies.
            group.bench_function(BenchmarkId::new(&case, "legacy"), |b| {
                b.iter(|| black_box(black_box(&legacy).estimated_size()));
            });
            group.bench_function(BenchmarkId::new(&case, "cheap"), |b| {
                b.iter(|| black_box(black_box(&value).estimated_size()));
            });
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
    targets = bench_sstable_meta_size
}
criterion_main!(benches);
