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

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use risingwave_common::array::ArrayBuilderImpl;
use risingwave_common::types::{DataType, Datum};

pub fn bench_bigint(c: &mut Criterion) {
    c.bench_function("bench_bigint", |b| {
        b.iter_batched(
            || ArrayBuilderImpl::from_type(&black_box(DataType::Int64), 1024),
            |mut builder| {
                for _i in 0..black_box(100) {
                    let datum: i64 = black_box(3);
                    let datum: Datum = Some(datum.into());
                    builder.append_n(10, datum);
                    black_box(());
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_bigint);
criterion_main!(benches);
