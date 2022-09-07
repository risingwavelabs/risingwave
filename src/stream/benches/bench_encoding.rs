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
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{
    serialize_datum_into, Datum, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, ScalarImpl,
};
use risingwave_common::util::value_encoding::serialize_datum;

struct Case {
    name: String,
    datum: Datum,
}

impl Case {
    pub fn new(name: &str, scalar: ScalarImpl) -> Self {
        Self {
            name: name.to_string(),
            datum: Some(scalar),
        }
    }
}

// Memcomparable encoding. Return the encoded length.
fn key_encode(datum: &Datum) -> usize {
    let mut serializer = memcomparable::Serializer::new(vec![]);
    serialize_datum_into(datum, &mut serializer).unwrap();
    serializer.into_inner().len()
}

// Value encoding. Return the encoded length.
fn value_encode(datum: &Datum) -> usize {
    let mut buf = vec![];
    serialize_datum(datum, &mut buf);
    buf.len()
}

fn bench_encoding(c: &mut Criterion) {
    let cases = vec![
        Case::new("Int16", ScalarImpl::Int16(1)),
        Case::new("Int32", ScalarImpl::Int32(1)),
        Case::new("Int64", ScalarImpl::Int64(1)),
        Case::new("Float32", ScalarImpl::Float32(1.0.into())),
        Case::new("Float64", ScalarImpl::Float64(1.0.into())),
        Case::new("Bool", ScalarImpl::Bool(true)),
        Case::new("Decimal", ScalarImpl::Decimal("12.13".parse().unwrap())),
        Case::new("Interval", ScalarImpl::Interval(IntervalUnit::default())),
        Case::new(
            "NaiveDate",
            ScalarImpl::NaiveDate(NaiveDateWrapper::default()),
        ),
        Case::new(
            "NaiveDateTime",
            ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper::default()),
        ),
        Case::new(
            "NaiveTime",
            ScalarImpl::NaiveTime(NaiveTimeWrapper::default()),
        ),
        Case::new(
            "Utf8 (len = 10)",
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 10])),
        ),
        Case::new(
            "Utf8 (len = 1000)",
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 1000])),
        ),
        Case::new(
            "Utf8 (len = 10000)",
            ScalarImpl::Utf8(String::from_iter(vec!['a'; 100000])),
        ),
        // Use bool as the inner elem/field type to eliminate the performance gap in elem/field
        // encoding.
        Case::new(
            "Struct of Bool (len = 100)",
            ScalarImpl::Struct(StructValue::new(vec![Some(ScalarImpl::Bool(true)); 100])),
        ),
        Case::new(
            "List of Bool (len = 100)",
            ScalarImpl::List(ListValue::new(vec![Some(ScalarImpl::Bool(true)); 100])),
        ),
    ];

    for case in cases {
        // Bench key encoding.
        let encoded_len = key_encode(&case.datum);
        println!("{} key encoded len: {}", case.name, encoded_len);
        c.bench_function(
            format!("bench {} (key encoding)", case.name).as_str(),
            |bencher| bencher.iter(|| key_encode(&case.datum)),
        );

        // Bench value encoding.
        let encoded_len = value_encode(&case.datum);
        println!("{} value encoded len: {}", case.name, encoded_len);
        c.bench_function(
            format!("bench {} (value encoding)", case.name).as_str(),
            |bencher| bencher.iter(|| value_encode(&case.datum)),
        );
    }
}

// This benchmark compares encoding performance between key/value encoding.
criterion_group!(benches, bench_encoding);
criterion_main!(benches);
