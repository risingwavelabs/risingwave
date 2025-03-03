// Copyright 2025 RisingWave Labs
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

use bytes::Bytes;
use criterion::async_executor::FuturesExecutor;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::TableKey;
use risingwave_storage::hummock::compactor::merge_imms_in_memory;
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferValue,
};

fn gen_interleave_shared_buffer_batch(
    batch_size: usize,
    batch_count: usize,
    epoch: u64,
) -> Vec<SharedBufferBatch> {
    let mut batches = Vec::new();
    for i in 0..batch_count {
        let mut batch_data = vec![];
        for j in 0..batch_size {
            batch_data.push((
                TableKey(Bytes::copy_from_slice(
                    format!("test_key_{:08}", j * batch_count + i).as_bytes(),
                )),
                SharedBufferValue::Insert(Bytes::copy_from_slice("value".as_bytes())),
            ));
        }
        let batch = SharedBufferBatch::for_test(batch_data, epoch, Default::default());
        batches.push(batch);
    }
    batches.reverse();
    batches
}

fn criterion_benchmark(c: &mut Criterion) {
    let batches = gen_interleave_shared_buffer_batch(10000, 100, 100);
    c.bench_with_input(
        BenchmarkId::new("bench-imm-merge", "single-epoch"),
        &batches,
        |b, batches| {
            b.to_async(FuturesExecutor).iter(|| async {
                let imm = merge_imms_in_memory(TableId::default(), batches.clone(), None).await;
                assert_eq!(imm.key_count(), 10000 * 100);
                assert_eq!(imm.value_count(), 10000 * 100);
            })
        },
    );

    let mut later_batches = gen_interleave_shared_buffer_batch(2000, 100, 600);

    for i in 1..5 {
        let mut batches = gen_interleave_shared_buffer_batch(2000, 100, 600 - i * 100);
        batches.extend(later_batches);
        later_batches = batches;
    }

    c.bench_with_input(
        BenchmarkId::new("bench-imm-merge", "multi-epoch"),
        &later_batches,
        |b, batches| {
            b.to_async(FuturesExecutor).iter(|| async {
                let imm = merge_imms_in_memory(TableId::default(), batches.clone(), None).await;
                assert_eq!(imm.key_count(), 2000 * 100);
                assert_eq!(imm.value_count(), 2000 * 100 * 5);
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
