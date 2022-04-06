use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use risingwave_storage::hummock::{
    Block, BlockBuilder, BlockBuilderOptions, BlockIterator, CompressionAlgorithm,
};

const TABLES_PER_SSTABLE: u32 = 10;
const KEYS_PER_TABLE: u64 = 100;
const RESTART_INTERVAL: usize = 16;
const BLOCK_CAPACITY: usize = TABLES_PER_SSTABLE as usize * KEYS_PER_TABLE as usize * 64;

fn block_iter_next(block: Arc<Block>) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_first();
    while iter.is_valid() {
        iter.next();
    }
}

fn block_iter_prev(block: Arc<Block>) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_last();
    while iter.is_valid() {
        iter.prev();
    }
}

fn bench_block_iter(c: &mut Criterion) {
    let block = Arc::new(build_block(TABLES_PER_SSTABLE, KEYS_PER_TABLE));

    println!("block size: {}", block.len());

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "block - iter next - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &block,
        |b, block| {
            b.iter(|| block_iter_next(block.clone()));
        },
    );

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "block - iter prev - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &block,
        |b, block| {
            b.iter(|| block_iter_prev(block.clone()));
        },
    );

    let mut iter = BlockIterator::new(block);
    iter.seek_to_first();
    for t in 1..=TABLES_PER_SSTABLE {
        for i in 1..=KEYS_PER_TABLE {
            assert_eq!(iter.key(), key(t, i).to_vec());
            assert_eq!(iter.value(), value(i).to_vec());
            iter.next();
        }
    }
    assert!(!iter.is_valid());
}

criterion_group!(benches, bench_block_iter);
criterion_main!(benches);

fn build_block(t: u32, i: u64) -> Block {
    let options = BlockBuilderOptions {
        capacity: BLOCK_CAPACITY,
        compression_algorithm: CompressionAlgorithm::None,
        restart_interval: RESTART_INTERVAL,
    };
    let mut builder = BlockBuilder::new(options);
    for tt in 1..=t {
        for ii in 1..=i {
            builder.add(&key(tt, ii), &value(ii));
        }
    }
    let data = builder.build();
    Block::decode(data).unwrap()
}

fn key(t: u32, i: u64) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(b't');
    buf.put_u32(t);
    buf.put_u64(i);
    buf.freeze()
}

fn value(i: u64) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u64(i);
    buf.freeze()
}
