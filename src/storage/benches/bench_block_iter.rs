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

#![feature(once_cell)]
use std::sync::LazyLock;

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::{
    Block, BlockBuilder, BlockBuilderOptions, BlockHolder, BlockIterator, CompressionAlgorithm,
};

const TABLES_PER_SSTABLE: u32 = 10;
const KEYS_PER_TABLE: u64 = 100;
const RESTART_INTERVAL: usize = 16;
const BLOCK_CAPACITY: usize = TABLES_PER_SSTABLE as usize * KEYS_PER_TABLE as usize * 64;
const EXCHANGE_INTERVAL: usize = RESTART_INTERVAL / 2;

fn block_iter_next(block: BlockHolder) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_first();
    while iter.is_valid() {
        iter.next();
    }
}

fn block_iter_prev(block: BlockHolder) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_last();
    while iter.is_valid() {
        iter.prev();
    }
}

fn bench_block_iter(c: &mut Criterion) {
    let data = build_block_data(TABLES_PER_SSTABLE, KEYS_PER_TABLE);

    println!("block size: {}", data.len());

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "block - iter next - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &data,
        |b, data| {
            b.iter(|| {
                let block = BlockHolder::from_owned_block(Box::new(
                    Block::decode(data.clone(), data.len()).unwrap(),
                ));
                block_iter_next(block)
            });
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
        &data,
        |b, data| {
            b.iter(|| {
                let block = BlockHolder::from_owned_block(Box::new(
                    Block::decode(data.clone(), data.len()).unwrap(),
                ));
                block_iter_prev(block)
            });
        },
    );

    let l = data.len();
    let block = BlockHolder::from_owned_block(Box::new(Block::decode(data, l).unwrap()));
    let mut iter = BlockIterator::new(block);
    let mut item_count = 0;
    let mut ext_index = 0;
    let (mut k_ext, mut v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);

    iter.seek_to_first();
    for t in 1..=TABLES_PER_SSTABLE {
        for i in 1..=KEYS_PER_TABLE {
            item_count += 1;

            if item_count % EXCHANGE_INTERVAL == 0 {
                ext_index = (ext_index + 1) % DATA_LEN_SET.len();
                (k_ext, v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);
            }

            assert_eq!(iter.key(), FullKey::decode(&key(t, i, k_ext)));
            assert_eq!(iter.value(), value(i, v_ext).to_vec());
            iter.next();
        }
    }
    assert!(!iter.is_valid());
}

criterion_group!(benches, bench_block_iter);
criterion_main!(benches);

static DATA_LEN_SET: LazyLock<Vec<(Vec<u8>, Vec<u8>)>> = LazyLock::new(|| {
    vec![
        (vec![b'a'; 100], vec![b'a'; 100]),     // U8U8
        (vec![b'a'; 100], vec![b'a'; 300]),     // U8U16
        (vec![b'a'; 100], vec![b'a'; 65550]),   // U8U32
        (vec![b'a'; 300], vec![b'a'; 100]),     // U16U8
        (vec![b'a'; 300], vec![b'a'; 300]),     // U16U16
        (vec![b'a'; 300], vec![b'a'; 65550]),   // U16U32
        (vec![b'a'; 65550], vec![b'a'; 100]),   // U32U8
        (vec![b'a'; 65550], vec![b'a'; 300]),   // U32U16
        (vec![b'a'; 65550], vec![b'a'; 65550]), // U32U32
    ]
});

fn build_block_data(t: u32, i: u64) -> Bytes {
    let options = BlockBuilderOptions {
        capacity: BLOCK_CAPACITY,
        compression_algorithm: CompressionAlgorithm::None,
        restart_interval: RESTART_INTERVAL,
    };
    let mut builder = BlockBuilder::new(options);
    let mut item_count = 0;
    let mut ext_index = 0;
    let (mut k_ext, mut v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);

    for tt in 1..=t {
        for ii in 1..=i {
            item_count += 1;

            if item_count % EXCHANGE_INTERVAL == 0 {
                ext_index = (ext_index + 1) % DATA_LEN_SET.len();
                (k_ext, v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);
            }

            builder.add(FullKey::decode(&key(tt, ii, k_ext)), &value(ii, v_ext));
        }
    }
    Bytes::from(builder.build().to_vec())
}

fn key(t: u32, i: u64, ext: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_slice(ext);
    buf.put_u32(t);
    buf.put_u64(i);
    buf.freeze()
}

fn value(i: u64, ext: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u64(i);
    buf.put(ext);
    buf.freeze()
}
