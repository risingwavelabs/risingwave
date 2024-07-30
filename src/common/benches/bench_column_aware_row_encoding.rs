// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use risingwave_common::catalog::ColumnId;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Date, ScalarImpl};
use risingwave_common::util::value_encoding::column_aware_row_encoding::*;
use risingwave_common::util::value_encoding::*;

fn bench_column_aware_encoding_16_columns(c: &mut Criterion) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    // The schema is inspired by the TPC-H lineitem table
    let data_types = Arc::new([
        DataType::Int64,
        DataType::Int64,
        DataType::Int64,
        DataType::Int32,
        DataType::Decimal,
        DataType::Decimal,
        DataType::Decimal,
        DataType::Decimal,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Date,
        DataType::Date,
        DataType::Date,
        DataType::Varchar,
        DataType::Varchar,
        DataType::Varchar,
    ]);
    let row = OwnedRow::new(vec![
        Some(ScalarImpl::Int64(rng.gen())),
        Some(ScalarImpl::Int64(rng.gen())),
        Some(ScalarImpl::Int64(rng.gen())),
        Some(ScalarImpl::Int32(rng.gen())),
        Some(ScalarImpl::Decimal("1.0".parse().unwrap())),
        Some(ScalarImpl::Decimal("114.514".parse().unwrap())),
        None,
        Some(ScalarImpl::Decimal("0.08".parse().unwrap())),
        Some(ScalarImpl::Utf8("A".into())),
        Some(ScalarImpl::Utf8("B".into())),
        Some(ScalarImpl::Date(Date::from_ymd_uncheck(2024, 7, 1))),
        Some(ScalarImpl::Date(Date::from_ymd_uncheck(2024, 7, 2))),
        Some(ScalarImpl::Date(Date::from_ymd_uncheck(2024, 7, 3))),
        Some(ScalarImpl::Utf8("D".into())),
        None,
        Some(ScalarImpl::Utf8("No comments".into())),
    ]);

    let column_ids = (1..=data_types.len())
        .map(|i| ColumnId::from(i as i32))
        .collect::<Vec<_>>();

    c.bench_function("column_aware_row_encoding_16_columns_encode", |b| {
        let serializer = Serializer::new(&column_ids[..]);
        b.iter(|| {
            black_box(serializer.serialize(&row));
        });
    });

    let serializer = Serializer::new(&column_ids[..]);
    let encoded = serializer.serialize(&row);

    c.bench_function("column_aware_row_encoding_16_columns_decode", |b| {
        let deserializer =
            Deserializer::new(&column_ids[..], data_types.clone(), std::iter::empty());
        b.iter(|| {
            let result = deserializer.deserialize(&encoded).unwrap();
            black_box(result);
        });
    });
}

fn bench_column_aware_encoding_4_columns(c: &mut Criterion) {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    // The schema is inspired by the TPC-H nation table
    let data_types = Arc::new([
        DataType::Int32,
        DataType::Varchar,
        DataType::Int32,
        DataType::Varchar,
    ]);
    let row = OwnedRow::new(vec![
        Some(ScalarImpl::Int32(rng.gen())),
        Some(ScalarImpl::Utf8("United States".into())),
        Some(ScalarImpl::Int32(rng.gen())),
        Some(ScalarImpl::Utf8("No comments".into())),
    ]);

    let column_ids = (1..=data_types.len())
        .map(|i| ColumnId::from(i as i32))
        .collect::<Vec<_>>();

    c.bench_function("column_aware_row_encoding_4_columns_encode", |b| {
        let serializer = Serializer::new(&column_ids[..]);
        b.iter(|| {
            black_box(serializer.serialize(&row));
        });
    });

    let serializer = Serializer::new(&column_ids[..]);
    let encoded = serializer.serialize(&row);

    c.bench_function("column_aware_row_encoding_4_columns_decode", |b| {
        let deserializer =
            Deserializer::new(&column_ids[..], data_types.clone(), std::iter::empty());
        b.iter(|| {
            let result = deserializer.deserialize(&encoded).unwrap();
            black_box(result);
        });
    });
}

criterion_group!(
    benches,
    bench_column_aware_encoding_16_columns,
    bench_column_aware_encoding_4_columns,
);
criterion_main!(benches);
