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

use std::collections::BTreeMap;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::Result;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{
    BasicSerde, ValueRowDeserializer, ValueRowSerializer,
};
use risingwave_storage::value_serde::ValueRowSerdeNew;

struct Case {
    name: String,
    schema: Arc<[DataType]>,
    column_ids: Vec<ColumnId>,
    rows: Vec<OwnedRow>,
    needed_schema: Arc<[DataType]>,
    needed_ids: Vec<ColumnId>,
}

impl Case {
    pub fn new(
        name: &str,
        schema: Arc<[DataType]>,
        column_ids: Vec<ColumnId>,
        rows: Vec<OwnedRow>,
        needed_schema: Option<Arc<[DataType]>>,
        needed_ids: Option<Vec<ColumnId>>,
    ) -> Self {
        Self {
            name: name.to_string(),
            schema: schema.clone(),
            column_ids: column_ids.clone(),
            rows,
            needed_ids: needed_ids.unwrap_or(column_ids),
            needed_schema: needed_schema.unwrap_or(schema),
        }
    }
}

fn memcmp_encode(c: &Case) -> Vec<Vec<u8>> {
    let serde = OrderedRowSerde::new(
        c.schema.to_vec(),
        vec![OrderType::descending(); c.schema.len()],
    );
    let mut array = vec![];
    for row in &c.rows {
        let mut row_bytes = vec![];
        serde.serialize(row, &mut row_bytes);
        array.push(row_bytes);
    }
    array
}

fn basic_encode(c: &Case) -> Vec<Vec<u8>> {
    let mut array = vec![];
    for row in &c.rows {
        let row_encoding = row.value_serialize();
        array.push(row_encoding);
    }
    array
}

fn column_aware_encode(c: &Case) -> Vec<Vec<u8>> {
    let seralizer = ColumnAwareSerde::new(&c.column_ids, c.schema.clone(), std::iter::empty());
    let mut array = vec![];
    for row in &c.rows {
        let row_bytes = seralizer.serialize(row);
        array.push(row_bytes);
    }
    array
}

fn memcmp_decode(c: &Case, bytes: &Vec<Vec<u8>>) -> Result<Vec<Vec<Datum>>> {
    let serde = OrderedRowSerde::new(
        c.schema.to_vec(),
        vec![OrderType::descending(); c.schema.len()],
    );
    let mut res = vec![];
    if c.column_ids == c.needed_ids {
        for byte in bytes {
            let row = serde.deserialize(byte)?.into_inner();
            res.push(row);
        }
    } else {
        let column_id_to_index = c
            .column_ids
            .iter()
            .enumerate()
            .map(|(v, k)| (k, v))
            .collect::<BTreeMap<_, _>>();
        let needed_to_row = c
            .needed_ids
            .iter()
            .map(|id| (id, *column_id_to_index.get(id).unwrap_or(&65536)))
            .collect::<BTreeMap<_, _>>();

        for byte in bytes.iter().enumerate() {
            let row = serde.deserialize(byte.1)?.into_inner();
            let mut needed = vec![None; c.needed_ids.len()];
            for (i, c) in c.needed_ids.iter().enumerate() {
                let ri = *needed_to_row.get(c).unwrap();
                if ri != 65536 {
                    if let Some(v) = &row[ri] {
                        needed[i] = Some(v.clone());
                    }
                }
            }
            res.push(needed);
        }
    }

    Ok(res)
}

fn basic_decode(c: &Case, bytes: &Vec<Vec<u8>>) -> Result<Vec<Vec<Datum>>> {
    let deserializer = BasicSerde::new(&c.column_ids, c.schema.clone(), std::iter::empty());
    let mut res = vec![];
    if c.column_ids == c.needed_ids {
        for byte in bytes {
            let row = deserializer.deserialize(&byte[..])?;
            res.push(row);
        }
    } else {
        let column_id_to_index = c
            .column_ids
            .iter()
            .enumerate()
            .map(|(v, k)| (k, v))
            .collect::<BTreeMap<_, _>>();
        let needed_to_row = c
            .needed_ids
            .iter()
            .map(|id| (id, *column_id_to_index.get(id).unwrap_or(&65536)))
            .collect::<BTreeMap<_, _>>();
        for byte in bytes {
            let row = deserializer.deserialize(&byte[..])?;
            let mut needed = vec![None; c.needed_ids.len()];
            for (i, c) in c.needed_ids.iter().enumerate() {
                let ri = *needed_to_row.get(c).unwrap();
                if ri != 65536 {
                    if let Some(v) = &row[ri] {
                        needed[i] = Some(v.clone());
                    }
                }
            }
            res.push(needed);
        }
    }

    Ok(res)
}

fn column_aware_decode(c: &Case, bytes: &Vec<Vec<u8>>) -> Result<Vec<Vec<Datum>>> {
    let deserializer =
        ColumnAwareSerde::new(&c.needed_ids, c.needed_schema.clone(), std::iter::empty());
    let mut res = vec![];
    for byte in bytes {
        let row = deserializer.deserialize(byte)?;
        res.push(row);
    }
    Ok(res)
}

fn bench_row(c: &mut Criterion) {
    let cases = vec![
        Case::new(
            "Int16",
            Arc::new([DataType::Int16]),
            vec![ColumnId::new(0)],
            vec![OwnedRow::new(vec![Some(ScalarImpl::Int16(5))]); 100000],
            None,
            None,
        ),
        Case::new(
            "Int16 and String",
            Arc::new([DataType::Int16, DataType::Varchar]),
            vec![ColumnId::new(0), ColumnId::new(1)],
            vec![
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int16(5)),
                    Some(ScalarImpl::Utf8("abc".into()))
                ]);
                100000
            ],
            None,
            None,
        ),
        Case::new(
            "Int16 and String (Only need String)",
            Arc::new([DataType::Int16, DataType::Varchar]),
            vec![ColumnId::new(0), ColumnId::new(1)],
            vec![
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int16(5)),
                    Some(ScalarImpl::Utf8("abc".into()))
                ]);
                100000
            ],
            Some(Arc::new([DataType::Varchar])),
            Some(vec![ColumnId::new(1)]),
        ),
    ];
    for case in &cases {
        c.bench_function(
            format!("memcmp encoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| memcmp_encode(case)),
        );
        c.bench_function(
            format!("basic encoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| basic_encode(case)),
        );
        c.bench_function(
            format!("column aware encoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| column_aware_encode(case)),
        );
    }

    for case in &cases {
        let encode_result = memcmp_encode(case);
        c.bench_function(
            format!("memcmp decoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| memcmp_decode(case, &encode_result)),
        );
        let encode_result = basic_encode(case);
        c.bench_function(
            format!("basic decoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| basic_decode(case, &encode_result)),
        );
        let encode_result = column_aware_encode(case);
        c.bench_function(
            format!("column aware decoding on {}", case.name).as_str(),
            |bencher| bencher.iter(|| column_aware_decode(case, &encode_result)),
        );
    }
}

criterion_group!(benches, bench_row);
criterion_main!(benches);
