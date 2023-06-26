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

#![feature(lazy_cell)]
use std::ops::Bound;
use std::sync::LazyLock;

use bytes::{BufMut, Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::hummock::iterator::test_utils::transform_shared_buffer;
use risingwave_storage::hummock::iterator::HummockIterator;
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    Block, BlockBuilder, BlockBuilderOptions, BlockHolder, BlockIterator, CompressionAlgorithm,
};

const TABLES_PER_SSTABLE: u32 = 1;
const KEYS_PER_TABLE: u64 = 100;
const RESTART_INTERVAL: usize = 1;
const BLOCK_CAPACITY: usize = TABLES_PER_SSTABLE as usize * KEYS_PER_TABLE as usize * 64;

fn build_shared_buffer(t: u32, i: u64) -> SharedBufferBatch {
    let epoch = 1;
    let mut ext_index = 0;
    let mut shared_buffer_items: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![];
    for _ in 1..=t {
        for _ in 1..=i {
            ext_index = (ext_index + 1) % DATA_LEN_SET.len();
            let (key, value) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);

            shared_buffer_items.push((key.clone(), HummockValue::put(Bytes::from(value.clone()))));
        }
    }
    SharedBufferBatch::for_test(
        transform_shared_buffer(shared_buffer_items.clone()),
        epoch,
        Default::default(),
    )
}

fn bench_shared_buffer(c: &mut Criterion) {
    let shared_buffer_batch = build_shared_buffer(TABLES_PER_SSTABLE, KEYS_PER_TABLE);

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "shared buffer - forward iter next - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &shared_buffer_batch,
        |b, _| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            b.to_async(runtime).iter(|| async {
                // Forward iterator
                shared_buffer_forward_iter(shared_buffer_batch.clone()).await;
            });
        },
    );

    c.bench_with_input(
        BenchmarkId::new(
            format!(
                "shared buffer - backward iter - {} tables * {} keys",
                TABLES_PER_SSTABLE, KEYS_PER_TABLE
            ),
            "",
        ),
        &shared_buffer_batch,
        |b, _| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            b.to_async(runtime).iter(|| async {
                // Backward iterator
                shared_buffer_backward_iter(shared_buffer_batch.clone()).await;
            });
        },
    );
}

async fn shared_buffer_backward_iter(shared_buffer_batch: SharedBufferBatch) {
    let mut iter = shared_buffer_batch.clone().into_backward_iter();
    let first_key = FullKey::from_user_key(shared_buffer_batch.start_user_key(), 1);
    iter.seek(first_key).await.unwrap();
    while iter.is_valid() {
        iter.next().await.unwrap();
    }
}

async fn shared_buffer_forward_iter(shared_buffer_batch: SharedBufferBatch) {
    let mut iter = shared_buffer_batch.clone().into_backward_iter();
    if let Bound::Included(last_table_key) = shared_buffer_batch.end_table_key() {
        let last_key = FullKey::new(TableId { table_id: 0 }, last_table_key, 1);
        iter.seek(last_key).await.unwrap();
        while iter.is_valid() {
            iter.next().await.unwrap();
        }
    } else {
        panic!("last key do not exist");
    }
}

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
    let mut ext_index = 0;
    let (mut k_ext, mut v_ext) ;

    iter.seek_to_first();
    for t in 1..=TABLES_PER_SSTABLE {
        for i in 1..=KEYS_PER_TABLE {
            ext_index = (ext_index + 1) % DATA_LEN_SET.len();
            (k_ext, v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);

            assert_eq!(iter.key(), FullKey::decode(&key(t, i, k_ext)));
            assert_eq!(iter.value(), value(i, v_ext).to_vec());
            iter.next();
        }
    }
    assert!(!iter.is_valid());
}

criterion_group!(benches, bench_block_iter, bench_shared_buffer);
criterion_main!(benches);

static DATA_LEN_SET: LazyLock<Vec<(Vec<u8>, Vec<u8>)>> = LazyLock::new(|| {
    vec![
        (vec![b'a'; 10], vec![b'a'; 10]),       // U8U8
        (vec![b'a'; 10], vec![b'a'; 300]),      // U8U16
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
    let mut ext_index = 0;
    let (mut k_ext, mut v_ext);

    for tt in 1..=t {
        for ii in 1..=i {
            ext_index = (ext_index + 1) % DATA_LEN_SET.len();
            (k_ext, v_ext) = (&DATA_LEN_SET[ext_index].0, &DATA_LEN_SET[ext_index].1);

            builder.add_for_test(FullKey::decode(&key(tt, ii, k_ext)), &value(ii, v_ext));
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
