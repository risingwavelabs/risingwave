use criterion::{criterion_group, criterion_main, Criterion};
use minstant::Instant;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use risingwave_storage::hummock::sstable::Sstable;
use risingwave_storage::hummock::{BloomFilterBuilder, BloomFilterReader, FilterBuilder};
use rocksdb_util::{FilterBitsBuilderWrapper, FilterBitsReaderWrapper, FilterType};
use xorf::{Filter, Xor16};
use xxhash_rust::xxh64::xxh64;

const TEST_COUNT: usize = 1024 * 512;
const SEED: u64 = 10244027u64;
const BLOOM_BITS_PER_KEY: usize = 50;
const RIBBON_BITS_PER_KEY: f64 = 24.0;

fn generate_key(idx: usize) -> String {
    let a = 1000000007usize - idx;
    format!("{:?}_{:?}", &a.to_le_bytes(), &idx.to_be_bytes())
}

fn bench_bloom_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut builder = BloomFilterBuilder::for_test(BLOOM_BITS_PER_KEY, TEST_COUNT / 2);
    for idx in 0..TEST_COUNT {
        let key = generate_key(idx);
        if idx < TEST_COUNT / 2 {
            builder.add_key(key.as_bytes(), 0);
        }
        origin_data.push(key);
    }
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut fp: usize = 0;
    let build_timer = Instant::now();
    let data = builder.finish();
    println!("bench_bloom_filter build time: {:?}", build_timer.elapsed());
    let mut negative_case = 0;
    let data_len = data.len();
    let bloom = BloomFilterReader::new(data);
    c.bench_function("bench_bloom_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % (TEST_COUNT / 2) + TEST_COUNT / 2;
                assert!(idx >= TEST_COUNT / 2);
                let ret = bloom.may_match(Sstable::hash_for_bloom_filter(
                    origin_data[idx].as_bytes(),
                    0,
                ));
                negative_case += 1;
                if ret {
                    fp += 1;
                }
            }
        });
    });
    println!(
        "===bench_bloom_filter_read fpr===: {:.8}%, cost: {}KB",
        (fp as f64) * 100.0 / (negative_case as f64),
        data_len / 1024,
    );
}

fn bench_ribbon_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut builder = FilterBitsBuilderWrapper::create(RIBBON_BITS_PER_KEY, FilterType::Ribbon);
    for idx in 0..TEST_COUNT {
        let key = generate_key(idx);
        if idx < TEST_COUNT / 2 {
            builder.add_key(key.as_bytes());
        }
        origin_data.push(key);
    }
    let mut rng = StdRng::seed_from_u64(SEED);
    let build_timer = Instant::now();
    let data = builder.finish();
    println!("ribbon_filter build time: {:?}", build_timer.elapsed());
    let mut fp: usize = 0;
    let mut negative_case = 0;
    let data_len = data.len();
    let reader = FilterBitsReaderWrapper::create(data);
    c.bench_function("bench_ribbon_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % (TEST_COUNT / 2) + TEST_COUNT / 2;
                assert!(idx >= TEST_COUNT / 2);
                let ret = reader.may_match(origin_data[idx].as_bytes());
                negative_case += 1;
                if ret {
                    fp += 1;
                }
            }
        });
    });
    println!(
        "===bench_ribbon_filter_read fpr===: {:.8}%, cost {}KB",
        (fp as f64) * 100.0 / (negative_case as f64),
        data_len / 1024,
    );
}

fn bench_xor16_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut data = Vec::with_capacity(TEST_COUNT);
    for idx in 0..TEST_COUNT {
        let key = generate_key(idx);
        data.push(xxh64(key.as_bytes(), 0));
        origin_data.push(key);
    }
    let mut rng = StdRng::seed_from_u64(SEED);
    let build_timer = Instant::now();
    let filter = Xor16::from(&data[..(TEST_COUNT / 2)]);
    println!("xor16_filter build time: {:?}", build_timer.elapsed());
    let mut fp: usize = 0;
    let mut negative_case = 0;
    c.bench_function("bench_xor16_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % (TEST_COUNT / 2) + TEST_COUNT / 2;
                assert!(idx >= TEST_COUNT / 2);
                let ret = filter.contains(&xxh64(origin_data[idx].as_bytes(), 0));
                negative_case += 1;
                if ret {
                    fp += 1;
                }
            }
        });
    });
    println!(
        "===bench_xor16_filter_read fpr===: {:.8}%, cost: {}KB",
        (fp as f64) * 100.0 / (negative_case as f64),
        filter.len() * std::mem::size_of::<u16>() / 1024
    );
}

criterion_group!(
    benches,
    bench_bloom_filter_read,
    bench_ribbon_filter_read,
    bench_xor16_filter_read
);
criterion_main!(benches);
