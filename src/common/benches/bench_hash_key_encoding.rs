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
use risingwave_common::array::column::Column;
use risingwave_common::array::serial_array::SerialArray;
use risingwave_common::array::{
    ArrayBuilderImpl, BoolArray, DataChunk, DateArray, DecimalArray, F32Array, F64Array, I16Array,
    I32Array, I64Array, IntervalArray, TimeArray, TimestampArray, Utf8Array,
};
use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};
use risingwave_common::test_utils::rand_array::seed_rand_array_ref;
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
                    "{} {:?}, {} rows, Pr[null]={}",
                    self.describe,
                    calc_hash_key_kind(self.data_types()),
                    chunk_size,
                    null_ratio
                );
                let input_chunk = gen_chunk(self.data_types(), *chunk_size, SEED, *null_ratio);
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
        let keys = HashKey::build(&col_idxes, &input_chunk).unwrap();
        Self {
            id,
            input_chunk,
            keys,
            data_types,
            col_idxes,
        }
    }

    pub fn bench_vec_ser(&self, c: &mut Criterion) {
        let vectorize_serialize_id = "vec ser ".to_string() + &self.id;
        c.bench_function(&vectorize_serialize_id, |b| {
            b.iter(|| K::build(&self.col_idxes, &self.input_chunk).unwrap())
        });
    }

    pub fn bench_vec_deser(&self, c: &mut Criterion) {
        let vectorize_deserialize_id = "vec deser ".to_string() + &self.id;
        c.bench_function(&vectorize_deserialize_id, |b| {
            let mut array_builders = self
                .input_chunk
                .columns()
                .iter()
                .map(|c| c.array_ref().create_builder(self.input_chunk.capacity()))
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
        let vectorize_deserialize_id = "row deser ".to_string() + &self.id;
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

fn gen_chunk(data_types: &[DataType], size: usize, seed: u64, null_ratio: f64) -> DataChunk {
    let mut columns = vec![];

    for d in data_types {
        columns.push(Column::new(match d {
            DataType::Boolean => seed_rand_array_ref::<BoolArray>(size, seed, null_ratio),
            DataType::Int16 => seed_rand_array_ref::<I16Array>(size, seed, null_ratio),
            DataType::Int32 => seed_rand_array_ref::<I32Array>(size, seed, null_ratio),
            DataType::Int64 => seed_rand_array_ref::<I64Array>(size, seed, null_ratio),
            DataType::Float32 => seed_rand_array_ref::<F32Array>(size, seed, null_ratio),
            DataType::Float64 => seed_rand_array_ref::<F64Array>(size, seed, null_ratio),
            DataType::Decimal => seed_rand_array_ref::<DecimalArray>(size, seed, null_ratio),
            DataType::Date => seed_rand_array_ref::<DateArray>(size, seed, null_ratio),
            DataType::Varchar => seed_rand_array_ref::<Utf8Array>(size, seed, null_ratio),
            DataType::Time => seed_rand_array_ref::<TimeArray>(size, seed, null_ratio),
            DataType::Serial => seed_rand_array_ref::<SerialArray>(size, seed, null_ratio),
            DataType::Timestamp => seed_rand_array_ref::<TimestampArray>(size, seed, null_ratio),
            DataType::Timestamptz => seed_rand_array_ref::<I64Array>(size, seed, null_ratio),
            DataType::Interval => seed_rand_array_ref::<IntervalArray>(size, seed, null_ratio),
            DataType::Struct(_) | DataType::Bytea | DataType::Jsonb => {
                todo!()
            }
            DataType::List { datatype: _ } => {
                todo!()
            }
        }));
    }
    risingwave_common::util::schema_check::schema_check(data_types, &columns).unwrap();
    DataChunk::new(columns, size)
}

fn case_builders() -> Vec<HashKeyBenchCaseBuilder> {
    vec![
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Serial],
            describe: "Serial".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32],
            describe: "int32".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64],
            describe: "int64".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Varchar],
            describe: "varchar".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Varchar, DataType::Varchar],
            describe: "composite varchar".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Int32, DataType::Int32],
            describe: "composite fixed, case 1".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Int64, DataType::Int32],
            describe: "composite fixed, case 2".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int32, DataType::Varchar],
            describe: "mix fixed and not fixed, case 1".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64, DataType::Varchar],
            describe: "mix fixed and not fixed, case 2".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 8],
            describe: "medium fixed".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: {
                let mut v = vec![DataType::Int64; 8];
                v[7] = DataType::Varchar;
                v
            },
            describe: "medium mixed".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: vec![DataType::Int64; 16],
            describe: "large fixed".to_string(),
        },
        HashKeyBenchCaseBuilder {
            data_types: {
                let mut v = vec![DataType::Int64; 16];
                v[15] = DataType::Varchar;
                v
            },
            describe: "large mixed".to_string(),
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
