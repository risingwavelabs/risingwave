// Copyright 2024 RisingWave Labs
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

#![feature(let_chains)]

//! To run this benchmark you can use the following command:
//! ```sh
//! cargo bench --bench stream_hash_join_rt
//! ```

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use risingwave_stream::executor::test_utils::hash_join_executor::*;
use tokio::runtime::Runtime;
use risingwave_stream::executor::JoinType;

risingwave_expr_impl::enable!();

fn bench_hash_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("benchmark_hash_join");
    group.sample_size(10);

    let rt = Runtime::new().unwrap();
    for amp in [10_000, 20_000, 30_000, 40_000, 100_000, 200_000, 400_000] {
        for workload in [HashJoinWorkload::NotInCache, HashJoinWorkload::InCache] {
            for join_type in [JoinType::Inner, JoinType::LeftOuter] {
                let name = format!("hash_join_rt_{}_{}_{}", amp, workload, join_type);
                group.bench_function(&name, |b| {
                    b.to_async(&rt).iter_batched(
                        || block_on(setup_bench_stream_hash_join(amp, workload, join_type)),
                        |(tx_l, tx_r, out)| handle_streams(workload, join_type, amp, tx_l, tx_r, out),
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    }
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);
