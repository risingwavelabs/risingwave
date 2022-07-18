
use criterion::{criterion_group, criterion_main, Criterion};
use prost::Message;
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_storage::hummock::{CompressionAlgorithm, SSTableBuilder, SSTableBuilderOptions};
use risingwave_storage::hummock::value::HummockValue;


pub fn test_key_of(idx: usize) -> Vec<u8> {
    let user_key = format!("key_test_{:08}", idx * 2).as_bytes().to_vec();
    key_with_epoch(user_key, 233)
}

const MAX_KEY_COUNT: usize = 1024 * 1024;


fn bench_compactor(c: &mut Criterion) {
    c.bench_function("bench_compactor", | b| {
        b.iter( || {
            let mut  builder = SSTableBuilder::new(0, SSTableBuilderOptions {
                capacity: 128 * 1024 * 1024,
                block_capacity: 1024 * 1024,
                restart_interval: 16,
                bloom_false_positive: 0.01,
                compression_algorithm: CompressionAlgorithm::None,
            });
            let value = b"1234567890123456789";
            let mut full_key = test_key_of(0);
            let user_len = full_key.len() - 8;
            for i in 0..MAX_KEY_COUNT {
                let start = i % 8;
                let end = start + 8;
                full_key[(user_len - 8)..user_len].copy_from_slice(&(i as u64).to_be_bytes());
                builder.add(&full_key, HummockValue::put(&value[start..end]));
            }
            let _ = builder.finish();
        });
    });
}

criterion_group!(benches, bench_compactor);
criterion_main!(benches);
