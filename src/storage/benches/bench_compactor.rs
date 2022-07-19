use std::sync::Arc;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_hummock_sdk::key::key_with_epoch;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_storage::hummock::{CachePolicy, CompressionAlgorithm, Sstable, SSTableBuilder, SSTableBuilderOptions, SSTableIterator, SstableMeta, SstableStore};
use risingwave_storage::hummock::iterator::{HummockIterator, ReadOptions};
use risingwave_storage::hummock::sstable_store::SstableStoreRef;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::monitor::StoreLocalStatistic;

pub fn mock_sstable_store() -> SstableStoreRef {
    let store = InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused()));
    let store = Arc::new(ObjectStoreImpl::InMem(store));
    let path = "test".to_string();
    Arc::new(SstableStore::new(store, path, 64 << 20, 64 << 20))
}


pub fn test_key_of(idx: usize) -> Vec<u8> {
    let user_key = format!("key_test_{:08}", idx * 2).as_bytes().to_vec();
    key_with_epoch(user_key, 233)
}

const MAX_KEY_COUNT: usize = 128 * 1024;

fn build_table(sstable_id: u64) -> (Bytes, SstableMeta){
    let mut  builder = SSTableBuilder::new(sstable_id, SSTableBuilderOptions {
        capacity: 32 * 1024 * 1024,
        block_capacity: 16 * 1024,
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
    let (_, data, meta, _) = builder.finish();
    (data, meta)
}

async fn scan_all_table(sstable_store: SstableStoreRef) {
    let mut stats = StoreLocalStatistic::default();
    let table = sstable_store.sstable(1, &mut stats).await.unwrap();
    let default_read_options = Arc::new(ReadOptions::default());
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch block from meta.
    let mut iter = SSTableIterator::new(table, sstable_store.clone(), default_read_options);
    iter.rewind().await.unwrap();
    while iter.is_valid() {
        iter.next().await.unwrap();
    }
}

async fn scan_all_table_inline(sstable_store: SstableStoreRef) {
    let mut stats = StoreLocalStatistic::default();
    let table = sstable_store.sstable(1, &mut stats).await.unwrap();
    let default_read_options = Arc::new(ReadOptions::default());
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch block from meta.
    let mut iter = SSTableIterator::new(table, sstable_store.clone(), default_read_options);
    iter.rewind().await.unwrap();
    while iter.is_valid() {
        iter.next_inner().await.unwrap();
    }
}

fn bench_table_build(c: &mut Criterion) {
    c.bench_function("bench_table_build", |b| {
        b.iter(|| {
            let _ = build_table(0);
        });
    });
}

fn bench_table_scan(c: &mut Criterion) {
    let (data, meta) = build_table(1);
    let sstable_store = mock_sstable_store();
    let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let sstable_store1 = sstable_store.clone();
    runtime.block_on(async move {
        sstable_store1.put(Sstable::new(1, meta.clone()), data, CachePolicy::NotFill).await.unwrap();
    });
    // warm up to make them all in memory. I do not use CachePolicy::Fill because it will fetch block from meta.
    let sstable_store1 = sstable_store.clone();
    runtime.block_on(async move {
        scan_all_table(sstable_store1).await;
    });

    c.bench_function("bench_table_iterator", | b| {
        b.iter( || {
            let sstable_store2 = sstable_store.clone();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async move {
                scan_all_table(sstable_store2).await;
            });
        });
    });

    c.bench_function("bench_table_iterator_inline", | b| {
        b.iter( || {
            let sstable_store2 = sstable_store.clone();
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            runtime.block_on(async move {
                scan_all_table_inline(sstable_store2).await;
            });
        });
    });
}

criterion_group!(benches, bench_table_build, bench_table_scan);
criterion_main!(benches);
