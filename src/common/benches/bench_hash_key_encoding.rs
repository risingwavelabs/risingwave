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

use criterion::{Criterion, criterion_group, criterion_main};
use itertools::Itertools;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, calc_hash_key_kind};
use risingwave_common::test_utils::rand_chunk;
use risingwave_common::types::DataType;

static SEED: u64 = 998244353u64;
static CHUNK_SIZES: &[usize] = &[128, 1024];
static NULL_RATIOS: &[f64] = &[0.0, 0.01, 0.1];

trait Case: Send + 'static {
    fn bench(&self, c: &mut Criterion);
}
type BoxedCase = Box<dyn Case>;

struct HashKeyBenchCaseBuilder {
    pub data_types: Vec<DataType>,
    pub describe: String,
}
impl HashKeyBenchCaseBuilder {
    pub fn gen_cases(self) -> Vec<BoxedCase> {
        self.dispatch()
    }
}
impl HashKeyDispatcher for HashKeyBenchCaseBuilder {
    type Output = Vec<BoxedCase>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        let mut ret: Vec<BoxedCase> = vec![];
        for null_ratio in NULL_RATIOS {
            for chunk_size in CHUNK_SIZES {
                let id = format!(
                    "{} rows, {} {:?}, Pr[null]={}",
                    chunk_size,
                    self.describe,
                    calc_hash_key_kind(self.data_types()),
                    null_ratio
                );
                let input_chunk =
                    rand_chunk::gen_chunk(self.data_types(), *chunk_size, SEED, *null_ratio);
                ret.push(Box::new(HashKeyBenchCase::<K>::new(
                    id,
                    input_chunk,
                    self.data_types.clone(),
                )));
            }
        }
        ret
    }

    fn data_types(&self) -> &[DataType] {
        &self.data_types
    }
}

struct HashKeyBenchCase<K: HashKey> {
    id: String,
    input_chunk: DataChunk,
    keys: Vec<K>,
    data_types: Vec<DataType>,
    col_idxes: Vec<usize>,
}

impl<K: HashKey> HashKeyBenchCase<K> {
    pub fn new(id: String, input_chunk: DataChunk, data_types: Vec<DataType>) -> Self {
        // please reference the `bench_vec_deser` and `bench_deser` method for benchmarking partial
        // `col_idxes`
        let col_idxes = (0..input_chunk.columns().len()).collect_vec();
        let keys = HashKey::build_many(&col_idxes, &input_chunk);
        Self {
            id,
            input_chunk,
            keys,
            data_types,
            col_idxes,
        }
    }

    pub fn bench_vec_ser(&self, c: &mut Criterion) {
        let vectorize_serialize_id = "vec ser ".to_owned() + &self.id;
        c.bench_function(&vectorize_serialize_id, |b| {
            b.iter(|| K::build_many(&self.col_idxes, &self.input_chunk))
        });
    }

    pub fn bench_vec_deser(&self, c: &mut Criterion) {
        let vectorize_deserialize_id = "vec deser ".to_owned() + &self.id;
        c.bench_function(&vectorize_deserialize_id, |b| {
            let mut array_builders = self
                .input_chunk
                .columns()
                .iter()
                .map(|c| c.create_builder(self.input_chunk.capacity()))
                .collect::<Vec<ArrayBuilderImpl>>();
            b.iter(|| {
                for key in &self.keys {
                    key.deserialize_to_builders(&mut array_builders[..], &self.data_types)
                        .unwrap();
                }
            })
        });
    }

    pub fn bench_deser(&self, c: &mut Criterion) {
        let vectorize_deserialize_id = "row deser ".to_owned() + &self.id;
        c.bench_function(&vectorize_deserialize_id, |b| {
            b.iter(|| {
                for key in &self.keys {
                    key.deserialize(&self.data_types).unwrap();
                }
            })
        });
    }
}
impl<K: HashKey> Case for HashKeyBenchCase<K> {
    fn bench(&self, c: &mut Criterion) {
        self.bench_vec_ser(c);
        self.bench_vec_deser(c);
        self.bench_deser(c);
    }
}

fn case_builders() -> Vec<HashKeyBenchCaseBuilder> {
    vec![
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Serial],
            describe: "Serial".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32],
            describe: "int32".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64],
            describe: "int64".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Varchar],
            describe: "varchar".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Varchar, DataType::Varchar],
            describe: "composite varchar".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Int32, DataType::Int32],
            describe: "composite fixed, case 1".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Int64, DataType::Int32],
            describe: "composite fixed, case 2".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Varchar],
            describe: "mix fixed and not fixed, case 1".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64, DataType::Varchar],
            describe: "mix fixed and not fixed, case 2".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 8],
            describe: "medium fixed".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: {
                let mut v = vec![DataType::Int64; 8];
                v[7] = DataType::Varchar;
                v
            },
            describe: "medium mixed".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 16],
            describe: "large fixed".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: {
                let mut v = vec![DataType::Int64; 16];
                v[15] = DataType::Varchar;
                v
            },
            describe: "large mixed".to_owned(),
        },
        // These benchmark cases will test unaligned key sizes.
        // For instance five keys of Int64 cannot fit within Key256 (5 * 64 = 320 > 256),
        // so it has to go to next largest keysize, Key512.
        // This means 24 bytes of wasted memory.
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 5],
            describe: "unaligned small fixed".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 9],
            describe: "unaligned medium fixed".to_owned(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 17],
            describe: "unaligned large fixed".to_owned(),
        },
    ]
}

fn bench_hash_key_encoding(c: &mut Criterion) {
    for case_builder in case_builders() {
        let cases = case_builder.gen_cases();
        for case in cases {
            case.bench(c);
        }
    }
}

// `cargo bench -- "vec ser[\s\S]*KeySerialized[\s\S]*null ratio 0$"` bench all the
// `KeySerialized` hash key vectorized serialize cases with data's null ratio is 0,001
criterion_group!(benches, bench_hash_key_encoding);
criterion_main!(benches);
