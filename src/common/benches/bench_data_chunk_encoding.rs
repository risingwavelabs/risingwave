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

use criterion::{criterion_group, criterion_main, Criterion};
use itertools::Itertools;
use risingwave_common::row::Row;
use risingwave_common::test_utils::rand_chunk;
use risingwave_common::types::DataType;

static SEED: u64 = 998244353u64;
static CHUNK_SIZES: &[usize] = &[128, 1024];
static NULL_RATIOS: &[f64] = &[0.0, 0.01, 0.1];

struct DataChunkBenchCase {
    pub name: String,
    pub data_types: Vec<DataType>,
}

impl DataChunkBenchCase {
    pub fn new(name: &str, data_types: Vec<DataType>) -> Self {
        Self {
            name: name.to_string(),
            data_types,
        }
    }
}

fn bench_data_chunk_encoding(c: &mut Criterion) {
    let test_cases = vec![
        DataChunkBenchCase::new("Int16", vec![DataType::Int16]),
        DataChunkBenchCase::new("String", vec![DataType::Varchar]),
        DataChunkBenchCase::new("Int16 and String", vec![DataType::Int16, DataType::Varchar]),
        DataChunkBenchCase::new(
            "Int16, Int32, Int64 and String",
            vec![
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::Varchar,
            ],
        ),
    ];
    for case in test_cases {
        for null_ratio in NULL_RATIOS {
            for chunk_size in CHUNK_SIZES {
                let chunk = rand_chunk::gen_chunk(&case.data_types, *chunk_size, SEED, *null_ratio);
                let mut group = c.benchmark_group(&format!(
                    "data chunk encoding: {}, {} rows, Pr[null]={}",
                    case.name, chunk_size, null_ratio
                ));
                group.bench_function("chunk serialize", |b| b.iter(|| chunk.serialize()));
                group.bench_function("row serialize", |b| {
                    b.iter(|| {
                        chunk
                            .rows()
                            .map(|x| x.value_serialize_bytes())
                            .collect_vec()
                    })
                });
            }
        }
    }
}

criterion_group!(benches, bench_data_chunk_encoding);
criterion_main!(benches);
