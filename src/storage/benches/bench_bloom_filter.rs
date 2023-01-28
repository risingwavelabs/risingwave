use criterion::{criterion_group, criterion_main, Criterion};
use rand::rngs::SmallRng;
use rand::{RngCore, SeedableRng};
use risingwave_storage::hummock::sstable::{Bloom, InMemWriter, Sstable, SstableBuilder};
use rocksdb_util::{FilterBitsBuilderWrapper, FilterBitsReaderWrapper, FilterType};

const TEST_COUNT: usize = 1024 * 16;

fn bench_bloom_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut data = Vec::with_capacity(TEST_COUNT);
    for idx in 0..TEST_COUNT {
        let key = format!("test_000001_{:08}", idx);
        data.push(Sstable::hash_for_bloom_filter(key.as_bytes()));
        origin_data.push(key);
    }
    let mut rng = SmallRng::seed_from_u64(10244021u64);
    let data = Bloom::build_from_key_hashes(&data[..(TEST_COUNT / 2)], 14);
    let mut fp: usize = 0;
    let mut total_case = 0;
    let bloom = Bloom::new(&data);
    c.bench_function("bench_bloom_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % TEST_COUNT;
                let ret = bloom.surely_not_have_hash(Sstable::hash_for_bloom_filter(
                    origin_data[idx].as_bytes(),
                ));
                if idx < TEST_COUNT / 2 {
                    assert!(!ret);
                } else if !ret {
                    fp += 1;
                }
            }
            total_case += 100;
        });
    });
    println!(
        "===bench_bloom_filter_read fpr===: {}%",
        (fp as f64) * 100.0 / (total_case as f64)
    );
}

fn bench_ribbon_filter_read(c: &mut Criterion) {
    let mut origin_data = Vec::with_capacity(TEST_COUNT);
    let mut builder = FilterBitsBuilderWrapper::create(14.0, FilterType::Ribbon);
    for idx in 0..TEST_COUNT {
        let key = format!("test_000001_{:08}", idx);
        if idx < TEST_COUNT / 2 {
            builder.add_key(key.as_bytes());
        }
        origin_data.push(key);
    }
    let mut rng = SmallRng::seed_from_u64(10244021u64);
    let data = builder.finish();
    let mut fp: usize = 0;
    let mut total_case = 0;
    let reader = FilterBitsReaderWrapper::create(data);
    c.bench_function("bench_ribbon_filter_read", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let idx = rng.next_u64() as usize % TEST_COUNT;
                let ret = reader.may_match(origin_data[idx].as_bytes());
                if idx < TEST_COUNT / 2 {
                    assert!(ret);
                } else if ret {
                    fp += 1;
                }
            }
            total_case += 100;
        });
    });
    println!(
        "===bench_ribbon_filter_read fpr===: {}%",
        (fp as f64) * 100.0 / (total_case as f64)
    );
}

criterion_group!(benches, bench_bloom_filter_read, bench_ribbon_filter_read,);
criterion_main!(benches);
