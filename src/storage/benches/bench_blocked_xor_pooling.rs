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

//! Restore and actual reader range lookup on real SST blocked filters.
#[path = "blocked_xor_fixture/boxed_control.rs"]
mod boxed_control;
#[path = "blocked_xor_fixture/resources.rs"]
mod resources;
#[path = "blocked_xor_fixture/sstable.rs"]
mod sstable_meta_fixture;

use std::hint::black_box;
use std::ops::Bound;
use std::time::Duration;

use boxed_control::BoxedReader;
use criterion::{Criterion, criterion_group, criterion_main};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::sstable::{
    BlockedXor8FilterBuilder, BlockedXor16FilterBuilder, FilterBuilder, XorFilterReader,
};

fn case<F: FilterBuilder>(
    c: &mut Criterion,
    runtime: &tokio::runtime::Runtime,
    label: &str,
    keys: usize,
    block_capacity: usize,
) {
    let (_, meta) = runtime.block_on(
        sstable_meta_fixture::sstable_fixture_with_block_capacity::<F>(keys, 64, block_capacity),
    );
    let data = &meta.bloom_filter;
    let metas = &meta.block_metas;
    let old = BoxedReader::new(data, metas);
    let new = XorFilterReader::new(data, metas);
    assert_eq!(new.encode_to_bytes(), *data);
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
    for (point, range, hash) in &queries {
        assert_eq!(
            old.may_match(metas, point, *hash),
            new.may_match(metas, point, *hash)
        );
        assert_eq!(
            old.may_match(metas, range, *hash),
            new.may_match(metas, range, *hash)
        );
    }
    eprintln!(
        "FIXTURE name={label} blocks={} filter_bytes={} xxh64={:016x}",
        metas.len(),
        data.len(),
        xxhash_rust::xxh64::xxh64(data, 0)
    );
    let mut group = c.benchmark_group("blocked_xor_pooling");
    resources::measure(&mut group, &format!("{label}_restore_bulk_control"), || {
        BoxedReader::new(black_box(data), black_box(metas))
    });
    resources::measure(&mut group, &format!("{label}_restore_pooled"), || {
        XorFilterReader::new(black_box(data), black_box(metas))
    });
    for (name, wide) in [("point", false), ("range", true)] {
        let mut cursor = 0_usize;
        resources::measure(
            &mut group,
            &format!("{label}_{name}_query_bulk_control"),
            || {
                let (point, range, hash) = &queries[cursor % queries.len()];
                cursor = cursor.wrapping_add(1);
                old.may_match(
                    black_box(metas),
                    black_box(if wide { range } else { point }),
                    black_box(*hash),
                )
            },
        );
        let mut cursor = 0_usize;
        resources::measure(&mut group, &format!("{label}_{name}_query_pooled"), || {
            let (point, range, hash) = &queries[cursor % queries.len()];
            cursor = cursor.wrapping_add(1);
            new.may_match(
                black_box(metas),
                black_box(if wide { range } else { point }),
                black_box(*hash),
            )
        });
    }
    group.finish();
}

fn bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for capacity in [16 << 10, 8 << 10, 4 << 10, 64 << 10] {
        let keys = if capacity == 64 << 10 {
            16_000
        } else {
            1_100_000
        };
        case::<BlockedXor16FilterBuilder>(
            c,
            &runtime,
            &format!("{}k_xor16", capacity >> 10),
            keys,
            capacity,
        );
        case::<BlockedXor8FilterBuilder>(
            c,
            &runtime,
            &format!("{}k_xor8", capacity >> 10),
            keys,
            capacity,
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20).warm_up_time(Duration::from_millis(300)).measurement_time(Duration::from_secs(1));
    targets = bench
}
criterion_main!(benches);
