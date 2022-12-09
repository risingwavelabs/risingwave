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

#![feature(iterator_try_collect)]

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::{DataChunk, I32Array};
use risingwave_common::types::DataType;
use risingwave_expr::expr::*;
use risingwave_pb::expr::expr_node::Type;

criterion_group!(benches, bench_expr, bench_raw);
criterion_main!(benches);

const CHUNK_SIZE: usize = 1024;

fn bench_expr(c: &mut Criterion) {
    let input = DataChunk::new(
        vec![
            I32Array::from_iter(0..CHUNK_SIZE as i32).into(),
            I32Array::from_iter(0..CHUNK_SIZE as i32).into(),
        ],
        1024,
    );

    let i0 = || InputRefExpression::new(DataType::Int32, 0).boxed();
    let i1 = || InputRefExpression::new(DataType::Int32, 0).boxed();
    c.bench_function("expr/inputref", |bencher| {
        let inputref = i0();
        bencher.iter(|| inputref.eval(&input).unwrap())
    });
    c.bench_function("expr/add/i32", |bencher| {
        let add = new_binary_expr(Type::Add, DataType::Int32, i0(), i1()).unwrap();
        bencher.iter(|| add.eval(&input).unwrap())
    });
    c.bench_function("expr/mul/i32", |bencher| {
        let mul = new_binary_expr(Type::Multiply, DataType::Int32, i0(), i1()).unwrap();
        bencher.iter(|| mul.eval(&input).unwrap())
    });
}

/// Evaluate on raw Rust array.
///
/// This could be used as a baseline to compare and tune our expressions.
fn bench_raw(c: &mut Criterion) {
    // ~80ns
    c.bench_function("raw/add/i32", |bencher| {
        let a = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).collect::<Vec<_>>();
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| a + b)
                .collect::<Vec<_>>()
        })
    });
    // ~200ns
    c.bench_function("raw/add/Option<i32>", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(a + b),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
    });
    // ~1300ns
    c.bench_function("raw/add/Option<i32> (checked)", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        struct Overflow;
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(a.checked_add(*b).ok_or(Overflow)),
                    _ => None,
                })
                .try_collect::<Vec<_>>()
        })
    });
    // ~2600ns
    c.bench_function("raw/add/Option<i32> (cast,checked)", |bencher| {
        let a = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        let b = (0..CHUNK_SIZE as i32).map(Some).collect::<Vec<_>>();
        enum Error {
            Overflow,
            Cast,
        }
        fn checked_add(a: i32, b: i32) -> Result<i32, Error> {
            let a: i32 = a.try_into().map_err(|_| Error::Cast)?;
            let b: i32 = b.try_into().map_err(|_| Error::Cast)?;
            a.checked_add(b).ok_or(Error::Overflow)
        }
        bencher.iter(|| {
            a.iter()
                .zip(b.iter())
                .map(|(a, b)| match (a, b) {
                    (Some(a), Some(b)) => Some(checked_add(*a, *b)),
                    _ => None,
                })
                .try_collect::<Vec<_>>()
        })
    });
}
