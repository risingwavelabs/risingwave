// Copyright 2023 RisingWave Labs
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

use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use risingwave_storage::hummock::sstable::{Bloom, Sstable};
use xorf::{Filter, Xor16, Xor8};

const TEST_COUNT: usize = 1024 * 16 * 2;

fn bench_bloom_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut data = Vec::with_capacity(TEST_COUNT);
    for idx in 0..TEST_COUNT {
        let key = format!("test_000001_{:08}", idx);
        data.push(Sstable::hash_for_bloom_filter(key.as_bytes(), 0));
        origin_data.push(key);
    }
    let mut rng = SmallRng::seed_from_u64(10244021u64);
    let data = Bloom::build_from_key_hashes(&data[..(TEST_COUNT / 2)], 14);
    let mut fp: usize = 0;
    let mut negative_case = 0;
    let mut total_case = 0;
    let bloom = Bloom::new(&data);
    c.bench_function("bench_bloom_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % TEST_COUNT;
                let ret = bloom.surely_not_have_hash(Sstable::hash_for_bloom_filter(
                    origin_data[idx].as_bytes(),
                    0,
                ));
                if idx < TEST_COUNT / 2 {
                    assert!(!ret);
                } else {
                    negative_case += 1;
                    if !ret {
                        fp += 1;
                    }
                }
            }
            total_case += 100;
        });
    });
    println!(
        "===bench_bloom_filter_read fpr===: {}%",
        (fp as f64) * 100.0 / (negative_case as f64)
    );
    println!(
        "===bench_bloom_filter_read bpe===: {}",
        ((data.len() * 8) as f64) / ((TEST_COUNT / 2) as f64)
    );
}

fn bench_xor8_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut data = Vec::with_capacity(TEST_COUNT);
    for idx in 0..TEST_COUNT {
        let key = format!("test_000001_{:08}", idx);
        data.push(Sstable::hash_for_bloom_filter_u64(key.as_bytes(), 0));
        origin_data.push(key);
    }
    let mut rng = SmallRng::seed_from_u64(10244021u64);
    let filter = Xor8::from(&data[..(TEST_COUNT / 2)]);
    let mut fp: usize = 0;
    let mut negative_case = 0;
    let mut total_case = 0;
    c.bench_function("bench_xor8_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % TEST_COUNT;
                let ret = filter.contains(&Sstable::hash_for_bloom_filter_u64(
                    origin_data[idx].as_bytes(),
                    0,
                ));
                if idx < TEST_COUNT / 2 {
                    assert!(ret);
                } else {
                    negative_case += 1;
                    if ret {
                        fp += 1;
                    }
                }
            }
            total_case += 100;
        });
    });
    println!(
        "===bench_xor8_filter_read fpr===: {}%",
        (fp as f64) * 100.0 / (negative_case as f64)
    );
    println!(
        "===bench_xor8_filter_read bpe===: {}",
        ((filter.len() * 8) as f64) / ((TEST_COUNT / 2) as f64)
    );
}

fn bench_xor16_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut data = Vec::with_capacity(TEST_COUNT);
    for idx in 0..TEST_COUNT {
        let key = format!("test_000001_{:08}", idx);
        data.push(Sstable::hash_for_bloom_filter_u64(key.as_bytes(), 0));
        origin_data.push(key);
    }
    let mut rng = SmallRng::seed_from_u64(10244021u64);
    let filter = Xor16::from(&data[..(TEST_COUNT / 2)]);
    let mut fp: usize = 0;
    let mut negative_case = 0;
    let mut total_case = 0;
    c.bench_function("bench_xor16_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % TEST_COUNT;
                let ret = filter.contains(&Sstable::hash_for_bloom_filter_u64(
                    origin_data[idx].as_bytes(),
                    0,
                ));
                if idx < TEST_COUNT / 2 {
                    assert!(ret);
                } else {
                    negative_case += 1;
                    if ret {
                        fp += 1;
                    }
                }
            }
            total_case += 100;
        });
    });
    println!(
        "===bench_xor16_filter_read fpr===: {}%",
        (fp as f64) * 100.0 / (negative_case as f64)
    );
    println!(
        "===bench_xor16_filter_read bpe===: {}",
        ((filter.len() * 16) as f64) / ((TEST_COUNT / 2) as f64)
    );
}

criterion_group!(
    benches,
    bench_bloom_filter_read,
    bench_xor8_filter_read,
    bench_xor16_filter_read,
);
criterion_main!(benches);
