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

use std::sync::Arc;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng};
use risingwave_common::catalog::ColumnId;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{
    DataType, Date, ListValue, MapType, MapValue, ScalarImpl, StructType, StructValue,
};
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
        Some(ScalarImpl::Int64(rng.random())),
        Some(ScalarImpl::Int64(rng.random())),
        Some(ScalarImpl::Int64(rng.random())),
        Some(ScalarImpl::Int32(rng.random())),
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
        let serializer = Serializer::new(&column_ids[..], data_types.iter().cloned());
        b.iter(|| {
            black_box(serializer.serialize(&row));
        });
    });

    let serializer = Serializer::new(&column_ids[..], data_types.iter().cloned());
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
        Some(ScalarImpl::Int32(rng.random())),
        Some(ScalarImpl::Utf8("United States".into())),
        Some(ScalarImpl::Int32(rng.random())),
        Some(ScalarImpl::Utf8("No comments".into())),
    ]);

    let column_ids = (1..=data_types.len())
        .map(|i| ColumnId::from(i as i32))
        .collect::<Vec<_>>();

    c.bench_function("column_aware_row_encoding_4_columns_encode", |b| {
        let serializer = Serializer::new(&column_ids[..], data_types.iter().cloned());
        b.iter(|| {
            black_box(serializer.serialize(&row));
        });
    });

    let serializer = Serializer::new(&column_ids[..], data_types.iter().cloned());
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

fn bench_column_aware_encoding_struct(c: &mut Criterion) {
    use ScalarImpl::*;

    // struct<f1 int, s struct<f2 int, f3 boolean>>
    let make = |alterable| {
        let mut inner_struct =
            StructType::new([("f2", DataType::Int32), ("f3", DataType::Boolean)]);
        if alterable {
            inner_struct = inner_struct.with_ids([ColumnId::new(11), ColumnId::new(12)]);
        }
        let inner_struct: DataType = inner_struct.into();
        let mut outer_struct = StructType::new([("f1", DataType::Int32), ("s", inner_struct)]);
        if alterable {
            outer_struct = outer_struct.with_ids([ColumnId::new(1), ColumnId::new(2)]);
        }
        let outer_struct: DataType = outer_struct.into();

        let inner_struct_value = StructValue::new(vec![Some(Int32(6)), Some(Bool(true))]);
        let outer_struct_value =
            StructValue::new(vec![Some(Int32(5)), Some(Struct(inner_struct_value))]);

        let serializer = Serializer::new(&[ColumnId::new(1)], [outer_struct.clone()]);
        let deserializer = Deserializer::new(
            &[ColumnId::new(1)],
            [outer_struct].into(),
            std::iter::empty(),
        );
        let row = OwnedRow::new(vec![Some(Struct(outer_struct_value.clone()))]);

        (serializer, deserializer, row)
    };

    for alterable in [false, true] {
        let id = if alterable {
            "column_aware_row_encoding_struct_alterable"
        } else {
            "column_aware_row_encoding_struct_unalterable"
        };

        let (serializer, deserializer, row) = make(alterable);

        c.bench_function(&format!("{id}_encode"), |b| {
            b.iter(|| {
                black_box(serializer.serialize(&row));
            });
        });

        let encoded = serializer.serialize(&row);
        c.bench_function(&format!("{id}_decode"), |b| {
            b.iter(|| {
                let result = deserializer.deserialize(&encoded).unwrap();
                black_box(result);
            });
        });
    }
}

fn bench_column_aware_encoding_composite(c: &mut Criterion) {
    use ScalarImpl::*;

    // struct<f1 int, map map(varchar, struct<f2 int, f3 boolean>[])>
    let make = |alterable| {
        let mut inner_struct =
            StructType::new([("f2", DataType::Int32), ("f3", DataType::Boolean)]);
        if alterable {
            inner_struct = inner_struct.with_ids([ColumnId::new(11), ColumnId::new(12)]);
        }
        let inner_struct: DataType = inner_struct.into();
        let list = DataType::List(Box::new(inner_struct.clone()));
        let map = MapType::from_kv(DataType::Varchar, list.clone()).into();
        let mut outer_struct = StructType::new([("f1", DataType::Int32), ("map", map)]);
        if alterable {
            outer_struct = outer_struct.with_ids([ColumnId::new(1), ColumnId::new(2)]);
        }
        let outer_struct: DataType = outer_struct.into();

        let inner_struct_value = StructValue::new(vec![Some(Int32(6)), Some(Bool(true))]);
        let list_value =
            ListValue::from_datum_iter(&inner_struct, [Some(Struct(inner_struct_value))]);
        let map_value = MapValue::try_from_kv(
            ListValue::from_datum_iter(&DataType::Varchar, [Some(Utf8("key".into()))]),
            ListValue::from_datum_iter(&list, [Some(List(list_value))]),
        )
        .unwrap();
        let outer_struct_value = StructValue::new(vec![Some(Int32(5)), Some(Map(map_value))]);

        let serializer = Serializer::new(&[ColumnId::new(1)], [outer_struct.clone()]);
        let deserializer = Deserializer::new(
            &[ColumnId::new(1)],
            [outer_struct].into(),
            std::iter::empty(),
        );
        let row = OwnedRow::new(vec![Some(Struct(outer_struct_value.clone()))]);

        (serializer, deserializer, row)
    };

    for alterable in [false, true] {
        let id = if alterable {
            "column_aware_row_encoding_composite_alterable"
        } else {
            "column_aware_row_encoding_composite_unalterable"
        };

        let (serializer, deserializer, row) = make(alterable);

        c.bench_function(&format!("{id}_encode"), |b| {
            b.iter(|| {
                black_box(serializer.serialize(&row));
            });
        });

        let encoded = serializer.serialize(&row);
        c.bench_function(&format!("{id}_decode"), |b| {
            b.iter(|| {
                let result = deserializer.deserialize(&encoded).unwrap();
                black_box(result);
            });
        });
    }
}

criterion_group!(
    benches,
    bench_column_aware_encoding_16_columns,
    bench_column_aware_encoding_4_columns,
    bench_column_aware_encoding_struct,
    bench_column_aware_encoding_composite,
);
criterion_main!(benches);
