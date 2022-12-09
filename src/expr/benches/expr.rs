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

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::{DataChunk, I32Array};
use risingwave_common::types::DataType;
use risingwave_expr::expr::*;
use risingwave_pb::expr::expr_node::Type;

criterion_group!(benches, bench_expr, bench_raw);
criterion_main!(benches);

fn bench_expr(c: &mut Criterion) {
    let input = DataChunk::new(
        vec![
            I32Array::from_iter(0..1024).into(),
            I32Array::from_iter(0..1024).into(),
        ],
        1024,
    );

    let add = new_binary_expr(
        Type::Add,
        DataType::Int32,
        InputRefExpression::new(DataType::Int32, 0).boxed(),
        InputRefExpression::new(DataType::Int32, 1).boxed(),
    )
    .unwrap();
    c.bench_function("add/i32", |bencher| {
        bencher.iter(|| add.eval(&input).unwrap())
    });

    let mul = new_binary_expr(
        Type::Multiply,
        DataType::Int32,
        InputRefExpression::new(DataType::Int32, 0).boxed(),
        InputRefExpression::new(DataType::Int32, 1).boxed(),
    )
    .unwrap();
    c.bench_function("mul/i32", |bencher| {
        bencher.iter(|| mul.eval(&input).unwrap())
    });
}

/// Evaluate on raw Rust array.
fn bench_raw(c: &mut Criterion) {
    let a = (0..1024).collect::<Vec<_>>();
    let b = (0..1024).collect::<Vec<_>>();
    c.bench_function("raw/add/i32", |bencher| {
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
}
