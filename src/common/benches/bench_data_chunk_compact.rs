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
use risingwave_common::test_utils::{rand_bitmap, rand_chunk};
use risingwave_common::types::DataType;

static SEED: u64 = 998244353u64;
static CHUNK_SIZES: &[usize] = &[128, 1024];
static VIS_RATE: &[f64] = &[0.05, 0.25, 0.5, 0.75, 0.95];

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

fn bench_data_chunk_compact(c: &mut Criterion) {
    let test_cases = vec![
        DataChunkBenchCase::new("1 column", vec![DataType::Int16]),
        DataChunkBenchCase::new("2 columns", vec![DataType::Int16, DataType::Int16]),
        DataChunkBenchCase::new("5 columns", vec![DataType::Int16; 5]),
        DataChunkBenchCase::new("10 columns", vec![DataType::Int16; 10]),
    ];
    for case in test_cases {
        for vis_rate in VIS_RATE {
            for chunk_size in CHUNK_SIZES {
                let mut chunk = rand_chunk::gen_chunk(&case.data_types, *chunk_size, SEED, 1.0);
                if vis_rate < &1.0 {
                    chunk.set_visibility(rand_bitmap::gen_rand_bitmap(
                        *chunk_size,
                        (*chunk_size as f64 * vis_rate) as usize,
                        SEED,
                    ));
                }
                c.bench_function(
                    &format!(
                        "data chunk compact: {}, {} rows, vis rate {}",
                        case.name, chunk_size, vis_rate
                    ),
                    |b| {
                        b.iter(|| {
                            let _ = chunk.clone().compact();
                        })
                    },
                );
            }
        }
    }
}

criterion_group!(benches, bench_data_chunk_compact);
criterion_main!(benches);
