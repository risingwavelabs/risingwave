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

use std::collections::BTreeSet;

use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use risingwave_storage::hummock::sstable::Bloom;
use xorf::{BinaryFuse16, BinaryFuse8, Filter};
use xorfilter::Xor8;

const SAMPLE_SIZE: usize = 10_000_000;

fn bench_bloom_filter_read(_c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(10244021u64);

    let keys: Vec<u32> = (0..SAMPLE_SIZE).map(|_| rng.next_u32()).collect();

    let data = Bloom::build_from_key_hashes(&keys, 20);
    let bloom = Bloom::new(&data);

    // no false negatives
    for key in &keys {
        assert!(!bloom.surely_not_have_hash(*key));
    }

    let keys = BTreeSet::from_iter(keys.into_iter());

    // bits per entry
    let bpe = ((data.len() * 8) as f64) / (SAMPLE_SIZE as f64);
    println!("===bench_bloom_filter_read bpe===: {}", bpe);

    let negatives = (0..SAMPLE_SIZE)
        .map(|_| rng.next_u32())
        .filter(|n| !keys.contains(n))
        .collect_vec();

    // false positive rate
    let false_positives: usize = negatives
        .iter()
        .filter(|n| !bloom.surely_not_have_hash(**n))
        .count();
    let fp_rate: f64 = (false_positives * 100) as f64 / (negatives.len()) as f64;
    println!("===bench_bloom_filter_read fpr===: {}%", fp_rate);
}

fn bench_xor8_filter_read(_c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(10244021u64);

    let keys: Vec<u64> = (0..SAMPLE_SIZE).map(|_| rng.next_u64()).collect();
    let mut filter = Xor8::new();
    filter.build_keys(&keys);

    // no false negatives
    for key in &keys {
        assert!(filter.contains_key(*key));
    }

    let keys = BTreeSet::from_iter(keys.into_iter());

    let negatives = (0..SAMPLE_SIZE)
        .map(|_| rng.next_u64())
        .filter(|n| !keys.contains(n))
        .collect_vec();

    // false positive rate
    let false_positives: usize = negatives
        .iter()
        .filter(|n| filter.contains_key(**n))
        .count();
    let fp_rate: f64 = (false_positives * 100) as f64 / (negatives.len()) as f64;
    println!("===bench_xor8_filter_read fpr===: {}%", fp_rate);
}

fn bench_fuse8_filter_read(_c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(10244021u64);

    let keys: Vec<u64> = (0..SAMPLE_SIZE).map(|_| rng.next_u64()).collect();
    let filter = BinaryFuse8::try_from(&keys).unwrap();

    // no false negatives
    for key in &keys {
        assert!(filter.contains(key));
    }

    let keys = BTreeSet::from_iter(keys.into_iter());

    // bits per entry
    let bpe = ((filter.len() * 8) as f64) / (SAMPLE_SIZE as f64);
    println!("===bench_fuse8_filter_read bpe===: {}", bpe);

    let negatives = (0..SAMPLE_SIZE)
        .map(|_| rng.next_u64())
        .filter(|n| !keys.contains(n))
        .collect_vec();

    // false positive rate
    let false_positives: usize = negatives.iter().filter(|n| filter.contains(n)).count();
    let fp_rate: f64 = (false_positives * 100) as f64 / (negatives.len()) as f64;
    println!("===bench_fuse8_filter_read fpr===: {}%", fp_rate);
}

fn bench_fuse16_filter_read(_c: &mut Criterion) {
    let mut rng = SmallRng::seed_from_u64(10244021u64);

    let keys: Vec<u64> = (0..SAMPLE_SIZE).map(|_| rng.next_u64()).collect();
    let filter = BinaryFuse16::try_from(&keys).unwrap();

    // no false negatives
    for key in &keys {
        assert!(filter.contains(key));
    }

    let keys = BTreeSet::from_iter(keys.into_iter());

    // bits per entry
    let bpe = ((filter.len() * 16) as f64) / (SAMPLE_SIZE as f64);
    println!("===bench_fuse16_filter_read bpe===: {}", bpe);

    let negatives = (0..SAMPLE_SIZE)
        .map(|_| rng.next_u64())
        .filter(|n| !keys.contains(n))
        .collect_vec();

    // false positive rate
    let false_positives: usize = negatives.iter().filter(|n| filter.contains(n)).count();
    let fp_rate: f64 = (false_positives * 100) as f64 / (negatives.len()) as f64;
    println!("===bench_fuse16_filter_read fpr===: {}%", fp_rate);
}

criterion_group!(
    benches,
    bench_bloom_filter_read,
    bench_xor8_filter_read,
    bench_fuse8_filter_read,
    bench_fuse16_filter_read,
);
criterion_main!(benches);
