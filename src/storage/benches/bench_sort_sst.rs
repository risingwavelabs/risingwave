// Copyright 2025 RisingWave Labs
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

use std::cmp::Ordering;
use std::iter;

use criterion::{Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use rand::Rng;
use rand::distributions::Alphanumeric;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

fn prepare_data() -> (Vec<SstableInfo>, Vec<SstableInfo>) {
    let total_sst_count: usize = 1_000_000;
    let insert_sst_count: usize = 100;
    let key_length = 1000;
    let mut rng = rand::thread_rng();
    let mut sorted_ssts = (1..=total_sst_count)
        .map(|i| {
            // let key = bytes::Bytes::copy_from_slice(&i.to_be_bytes());
            let key = iter::repeat(())
                .map(|_| rng.sample(Alphanumeric))
                .take(key_length)
                .collect::<Vec<_>>();
            let key = bytes::Bytes::copy_from_slice(&key);
            let key_range = KeyRange::new(key.clone(), key);
            let inner = SstableInfoInner {
                sst_id: i as _,
                object_id: i as _,
                table_ids: vec![1],
                key_range,
                ..Default::default()
            };
            SstableInfo::from(inner)
        })
        .sorted_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range))
        .collect::<Vec<_>>();
    let insert_ssts = sorted_ssts
        .drain(
            (total_sst_count / 2 - insert_sst_count / 2)
                ..(total_sst_count / 2 + insert_sst_count / 2),
        )
        .collect::<Vec<_>>();
    (sorted_ssts, insert_ssts)
}

fn insertion_sort(base: &mut Vec<SstableInfo>, insert: Vec<SstableInfo>) {
    for i in insert {
        let pos = base.partition_point(|b|b.key_range.cmp(&i.key_range) == Ordering::Less);
        base.insert(pos, i);
    }
}

fn batch_insertion_sort(base: &mut Vec<SstableInfo>, insert: Vec<SstableInfo>) {
    let i = &insert[0];
    let pos = base.partition_point(|b|b.key_range.cmp(&i.key_range) == Ordering::Less);
    base.splice(pos..pos, insert);
}

fn bench_sort_sst(c: &mut Criterion) {
    let (base, insert) = prepare_data();
    c.bench_function("clone data", |b| {
        b.iter(|| {
            let _base = base.clone();
            let _insert = insert.clone();
        })
    });
    c.bench_function("base sort", |b| {
        b.iter(|| {
            let mut base = base.clone();
            let insert = insert.clone();
            base.extend(insert);
            base.sort_by(|sst1, sst2| sst1.key_range.cmp(&sst2.key_range));
        })
    });
    c.bench_function("insertion sort", |b| {
        b.iter(|| {
            let mut base = base.clone();
            let insert = insert.clone();
            insertion_sort(&mut base, insert);
        })
    });
    c.bench_function("batch insertion sort", |b| {
        b.iter(|| {
            let mut base = base.clone();
            let insert = insert.clone();
            batch_insertion_sort(&mut base, insert);
        })
    });
}

criterion_group!(
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::new(30, 0)).sample_size(10);
    targets = bench_sort_sst
);
criterion_main!(benches);
