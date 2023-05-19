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
use risingwave_batch::executor::hash_join::HashJoinExecutor;
use risingwave_batch::executor::test_utils::{gen_projected_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, JoinType};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::types::DataType;
use risingwave_common::{enable_jemalloc_on_unix, hash};
use risingwave_expr::expr::build_from_pretty;
use utils::bench_join;

enable_jemalloc_on_unix!();

fn create_hash_join_executor(
    join_type: JoinType,
    with_cond: bool,
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;

    let left_input = gen_projected_data(
        left_chunk_size,
        left_chunk_num,
        build_from_pretty("(modulus:int8 $0:int8 123:int8)"),
    );
    let right_input = gen_projected_data(
        right_chunk_size,
        right_chunk_num,
        build_from_pretty("(modulus:int8 $0:int8 456:int8)"),
    );

    let mut left_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    left_input.into_iter().for_each(|c| left_child.add(c));

    let mut right_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    right_input.into_iter().for_each(|c| right_child.add(c));

    let output_indices = match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => vec![0],
        JoinType::RightSemi | JoinType::RightAnti => vec![0],
        _ => vec![0, 1],
    };

    let cond = with_cond.then(|| build_from_pretty("(greater_than:int8 $0:int8 100:int8)"));

    Box::new(HashJoinExecutor::<hash::Key64>::new(
        join_type,
        output_indices,
        left_child,
        right_child,
        vec![0],
        vec![0],
        vec![false],
        cond,
        "HashJoinExecutor".into(),
        CHUNK_SIZE,
        // TODO: In practice this `shutdown_rx` will be constantly poll in execution, may need to
        // use it in bench too.
        None,
    ))
}

fn bench_hash_join(c: &mut Criterion) {
    let with_conds = vec![false, true];
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
        "HashJoinExecutor",
        with_conds,
        join_types,
        create_hash_join_executor,
    );
}

criterion_group!(benches, bench_hash_join);
criterion_main!(benches);
