use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use risingwave_pb::hummock::checksum::Algorithm;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    Block, BlockIterator, BlockIteratorV2, BlockV2, BlockV2Builder, BlockV2BuilderOptions,
    CompressionAlgorithm, SSTableBuilder, SSTableBuilderOptions,
};

fn block_iter_next(block: Arc<Block>) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_first();
    while iter.is_valid() {
        iter.next();
    }
}

fn block_v2_iter_next(block: Arc<BlockV2>) {
    let mut iter = BlockIteratorV2::new(block);
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

fn block_v2_iter_prev(block: Arc<BlockV2>) {
    let mut iter = BlockIteratorV2::new(block);
    iter.seek_to_last();
    while iter.is_valid() {
        iter.prev();
    }
}

fn bench_block_iter(c: &mut Criterion) {
    let block = Arc::new(build_block(10, 100));
    let blockv2 = Arc::new(build_block_v2(10, 100));

    c.bench_with_input(
        BenchmarkId::new("block - iter next", ""),
        &block,
        |b, block| {
            b.iter(|| block_iter_next(block.clone()));
        },
    );

    c.bench_with_input(
        BenchmarkId::new("block v2 - iter next", ""),
        &blockv2,
        |b, block| {
            b.iter(|| block_v2_iter_next(block.clone()));
        },
    );

    c.bench_with_input(
        BenchmarkId::new("block - iter prev", ""),
        &block,
        |b, block| {
            b.iter(|| block_iter_prev(block.clone()));
        },
    );

    c.bench_with_input(
        BenchmarkId::new("block v2 - iter prev", ""),
        &blockv2,
        |b, block| {
            b.iter(|| block_v2_iter_prev(block.clone()));
        },
    );
}

criterion_group!(benches, bench_block_iter);
criterion_main!(benches);

fn build_block(t: u32, i: u64) -> Block {
    let options = SSTableBuilderOptions {
        table_capacity: 65536,
        block_size: 65536,
        bloom_false_positive: 0.1,
        checksum_algo: Algorithm::XxHash64,
    };
    let mut builder = SSTableBuilder::new(options);
    for tt in 1..=t {
        for ii in 1..=i {
            builder.add(&key(tt, ii), HummockValue::Put(&value(ii)));
        }
    }
    let (data, meta) = builder.finish();
    assert_eq!(meta.block_metas.len(), 1);

    println!("block size: {}", data.len());

    Block::decode(data).unwrap()
}

fn build_block_v2(t: u32, i: u64) -> BlockV2 {
    let options = BlockV2BuilderOptions {
        capacity: 65536,
        compression_algorithm: CompressionAlgorithm::None,
        restart_interval: 16,
    };
    let mut builder = BlockV2Builder::new(options);
    for tt in 1..=t {
        for ii in 1..=i {
            builder.add(&key(tt, ii), &value(ii));
        }
    }
    let data = builder.build();

    println!("block v2 size: {}", data.len());

    BlockV2::decode(data).unwrap()
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
