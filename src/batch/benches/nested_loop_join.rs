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
pub mod utils;

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_batch::executor::{BoxedExecutor, JoinType, NestedLoopJoinExecutor};
use risingwave_common::enable_jemalloc_on_unix;
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_expr::expr::build_from_pretty;
use utils::{bench_join, create_input};

enable_jemalloc_on_unix!();

fn create_nested_loop_join_executor(
    join_type: JoinType,
    _with_cond: bool,
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;
    let left_input = create_input(&[DataType::Int64], left_chunk_size, left_chunk_num);
    let right_input = create_input(&[DataType::Int64], right_chunk_size, right_chunk_num);

    let output_indices = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => vec![0],
        JoinType::RightSemi | JoinType::RightAnti => vec![0],
        _ => vec![0, 1],
    };

    Box::new(NestedLoopJoinExecutor::new(
        build_from_pretty(
            "(equal:boolean
                (modulus:int8 $0:int8 2:int8) 
                (modulus:int8 $1:int8 3:int8))",
        ),
        join_type,
        output_indices,
        left_input,
        right_input,
        "NestedLoopJoinExecutor".into(),
        CHUNK_SIZE,
        MemoryContext::none(),
        // TODO: In practice this `shutdown_rx` will be constantly poll in execution, may need to
        // use it in bench too.
        None,
    ))
}

fn bench_nested_loop_join(c: &mut Criterion) {
    let with_conds = vec![false];
    let join_types = vec![
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightOuter,
        JoinType::RightSemi,
        JoinType::RightAnti,
    ];
    bench_join(
        c,
        "NestedLoopJoinExecutor",
        with_conds,
        join_types,
        create_nested_loop_join_executor,
    );
}

criterion_group!(benches, bench_nested_loop_join);
criterion_main!(benches);
