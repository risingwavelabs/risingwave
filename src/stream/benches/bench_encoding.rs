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

use std::env;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::{
    deserialize_datum_from, serialize_datum_into, DataType, Datum, IntervalUnit,
    NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, ScalarImpl,
};
use risingwave_common::util::value_encoding::{deserialize_datum, serialize_datum};

const ENV_BENCH_SER: &str = "BENCH_SER";
const ENV_BENCH_DE: &str = "BENCH_DE";
const ENV_CASE: &str = "CASE";

struct Case {
    name: String,
    ty: DataType,
    datum: Datum,
}

impl Case {
    pub fn new(name: &str, ty: DataType, scalar: ScalarImpl) -> Self {
        Self {
            name: name.to_string(),
            ty,
            datum: Some(scalar),
        }
    }
}

fn key_serialization(datum: &Datum) -> Vec<u8> {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    serialize_datum_into(datum, &mut serializer).unwrap();
    serializer.into_inner()
}

fn value_serialization(datum: &Datum) -> Vec<u8> {
    let mut buf = vec![];
    serialize_datum(datum, &mut buf);
    buf
}

fn key_deserialization(ty: &DataType, datum: &[u8]) {
    let mut deserializer = memcomparable::Deserializer::new(datum);
    let _ = deserialize_datum_from(ty, &mut deserializer);
}

fn value_deserialization(ty: &DataType, datum: &[u8]) {
    let _ = deserialize_datum(datum, ty);
}

fn bench_encoding(c: &mut Criterion) {
    let cases = vec![
        Case::new("Int16", DataType::Int16, ScalarImpl::Int16(1)),
        Case::new("Int32", DataType::Int32, ScalarImpl::Int32(1)),
        Case::new("Int64", DataType::Int64, ScalarImpl::Int64(1)),
        Case::new(
            "Float32",
            DataType::Float32,
            ScalarImpl::Float32(1.0.into()),
        ),
        Case::new(
            "Float64",
            DataType::Float64,
            ScalarImpl::Float64(1.0.into()),
        ),
        Case::new("Bool", DataType::Boolean, ScalarImpl::Bool(true)),
        Case::new(
            "Decimal",
            DataType::Decimal,
            ScalarImpl::Decimal("12.13".parse().unwrap()),
        ),
        Case::new(
            "Interval",
            DataType::Interval,
            ScalarImpl::Interval(IntervalUnit::default()),
        ),
        Case::new(
            "NaiveDate",
            DataType::Date,
            ScalarImpl::NaiveDate(NaiveDateWrapper::default()),
        ),
        Case::new(
            "NaiveDateTime",
            DataType::Timestamp,
            ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::default()),
        ),
        Case::new(
            "NaiveTime",
            DataType::Time,
            ScalarImpl::NaiveTime(NaiveTimeWrapper::default()),
        ),
        Case::new(
            "Utf8 (len = 10)",
            DataType::Varchar,
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 10])),
        ),
        Case::new(
            "Utf8 (len = 1000)",
            DataType::Varchar,
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 1000])),
        ),
        Case::new(
            "Utf8 (len = 10000)",
            DataType::Varchar,
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 100000])),
        ),
        // Use bool as the inner elem/field type to eliminate the performance gap in elem/field
        // encoding.
        Case::new(
            "Struct of Bool (len = 100)",
            DataType::Struct(Arc::new(StructType::new(vec![
                (
                    DataType::Boolean,
                    "".to_string()
                );
                100
            ]))),
            ScalarImpl::Struct(StructValue::new(vec![Some(ScalarImpl::Bool(true)); 100])),
        ),
        Case::new(
            "List of Bool (len = 100)",
            DataType::List {
                datatype: Box::new(DataType::Boolean),
            },
            ScalarImpl::List(ListValue::new(vec![Some(ScalarImpl::Bool(true)); 100])),
        ),
    ];

    let filter = env::var(ENV_CASE).unwrap_or_else(|_| "".to_string());
    let cases = cases
        .into_iter()
        .filter(|case| case.name.contains(&filter))
        .collect::<Vec<_>>();
    let bench_ser = !env::var(ENV_BENCH_SER)
        .unwrap_or_else(|_| "1".to_string())
        .eq("0");
    let bench_de = !env::var(ENV_BENCH_DE)
        .unwrap_or_else(|_| "1".to_string())
        .eq("0");

    if bench_ser {
        for case in &cases {
            // Bench key encoding.
            let encoded_len = key_serialization(&case.datum).len();
            println!("{} key encoded len: {}", case.name, encoded_len);
            c.bench_function(
                format!("bench {} (key encoding serialization)", case.name).as_str(),
                |bencher| bencher.iter(|| key_serialization(&case.datum)),
            );

            // Bench value encoding.
            let encoded_len = value_serialization(&case.datum).len();
            println!("{} value encoded len: {}", case.name, encoded_len);
            c.bench_function(
                format!("bench {} (value encoding serialization)", case.name).as_str(),
                |bencher| bencher.iter(|| value_serialization(&case.datum)),
            );
        }
    }

    if bench_de {
        for case in &cases {
            // Bench key encoding.
            let encode_result = key_serialization(&case.datum);
            c.bench_function(
                format!("bench {} (key encoding deserialization)", case.name).as_str(),
                |bencher| bencher.iter(|| key_deserialization(&case.ty, &encode_result)),
            );

            // Bench value encoding.
            let encode_result = value_serialization(&case.datum);
            c.bench_function(
                format!("bench {} (value encoding deserialization)", case.name).as_str(),
                |bencher| bencher.iter(|| value_deserialization(&case.ty, &encode_result)),
            );
        }
    }
}

// This benchmark compares encoding performance between key/value encoding.
//
// Some environment variables are available to select particular data types to ser/de.
// - `BENCH_SER`: Set to `0` to disable benchmarking serialization. Default to `1`.
// - `BENCH_DE`: Set to `0` to disable benchmarking deserialization. Default to `1`.
// - `CASE`: Filter the data types to bench. All the test cases whose name contains this variable
//   will be tested. It is case-sensitive.
criterion_group!(benches, bench_encoding);
criterion_main!(benches);
