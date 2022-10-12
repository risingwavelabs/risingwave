// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub mod utils;

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_batch::executor::test_utils::{gen_sorted_data, MockExecutor};
use risingwave_batch::executor::{BoxedExecutor, JoinType, SortMergeJoinExecutor};
use risingwave_common::catalog::schema_test_utils::field_n;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use tikv_jemallocator::Jemalloc;
use utils::bench_join;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn create_sort_merge_join_executor(
    join_type: JoinType,
    _with_cond: bool,
    left_chunk_size: usize,
    left_chunk_num: usize,
    right_chunk_size: usize,
    right_chunk_num: usize,
) -> BoxedExecutor {
    const CHUNK_SIZE: usize = 1024;
    let left_input = gen_sorted_data(left_chunk_size, left_chunk_num, "0".into(), 10);
    let right_input = gen_sorted_data(right_chunk_size, right_chunk_num, "1000".into(), 20);

    let mut left_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    left_input.into_iter().for_each(|c| left_child.add(c));

    let mut right_child = Box::new(MockExecutor::new(field_n::<1>(DataType::Int64)));
    right_input.into_iter().for_each(|c| right_child.add(c));

    Box::new(SortMergeJoinExecutor::new(
        OrderType::Ascending,
        join_type,
        // [field[0] of the left schema, field[0] of the right schema]
        vec![0, 1],
        // field[0] of the left schema
        vec![0],
        // field[0] of the right schema
        vec![0],
        left_child,
        right_child,
        "SortMergeJoinExecutor".into(),
        CHUNK_SIZE,
    ))
}

fn bench_sort_merge_join(c: &mut Criterion) {
    let with_conds = vec![false];
    let join_types = vec![JoinType::Inner];
    bench_join(
        c,
        "SortMergeJoinExecutor",
        with_conds,
        join_types,
        create_sort_merge_join_executor,
    );
}

criterion_group!(benches, bench_sort_merge_join);
criterion_main!(benches);
