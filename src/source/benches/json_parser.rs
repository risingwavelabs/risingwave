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

use chrono::{NaiveDate, NaiveDateTime};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::DataType;
use risingwave_source::parser::JsonParser;
use risingwave_source::{SourceColumnDesc, SourceParser, SourceStreamChunkBuilder};

const NUM_RECORDS: usize = 1 << 18; // ~ 4 million

fn generate_json(rng: &mut impl Rng) -> String {
    format!("{{\"i32\":{},\"bool\":{},\"i16\":{},\"i64\":{},\"f32\":{},\"f64\":{},\"varchar\":\"{}\",\"date\":\"{}\",\"timestamp\":\"{}\"}}",
        rng.gen::<i32>(),
        rng.gen::<bool>(),
        rng.gen::<i16>(),
        rng.gen::<i64>(),
        rng.gen::<f32>(),
        rng.gen::<f64>(),
        rng.sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect::<String>(),
        NaiveDate::from_num_days_from_ce((rng.gen::<u32>() % (1 << 20)) as i32),
        {
            let datetime = NaiveDateTime::from_timestamp((rng.gen::<u32>() % (1u32 << 28)) as i64, 0);
            format!("{:?} {:?}", datetime.date(), datetime.time())
        }
    )
}

fn generate_all_json() -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let mut records = Vec::with_capacity(NUM_RECORDS);
    for _ in 0..NUM_RECORDS {
        records.push(generate_json(&mut rng).into_bytes());
    }
    records
}

fn get_descs() -> Vec<SourceColumnDesc> {
    vec![
        SourceColumnDesc {
            name: "i32".to_string(),
            data_type: DataType::Int32,
            column_id: ColumnId::from(0),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "bool".to_string(),
            data_type: DataType::Boolean,
            column_id: ColumnId::from(2),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "i16".to_string(),
            data_type: DataType::Int16,
            column_id: ColumnId::from(3),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "i64".to_string(),
            data_type: DataType::Int64,
            column_id: ColumnId::from(4),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "f32".to_string(),
            data_type: DataType::Float32,
            column_id: ColumnId::from(5),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "f64".to_string(),
            data_type: DataType::Float64,
            column_id: ColumnId::from(6),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "varchar".to_string(),
            data_type: DataType::Varchar,
            column_id: ColumnId::from(7),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "date".to_string(),
            data_type: DataType::Date,
            column_id: ColumnId::from(8),
            skip_parse: false,
            fields: vec![],
        },
        SourceColumnDesc {
            name: "timestamp".to_string(),
            data_type: DataType::Timestamp,
            column_id: ColumnId::from(9),
            skip_parse: false,
            fields: vec![],
        },
    ]
}

fn bench_json_parser(c: &mut Criterion) {
    let descs = get_descs();
    let parser = JsonParser {};
    let records = generate_all_json();
    c.bench_function("json_parser", |b| {
        b.iter(|| {
            let mut builder = SourceStreamChunkBuilder::with_capacity(descs.clone(), NUM_RECORDS);
            for record in &records {
                let writer = builder.row_writer();
                parser.parse(record, writer).unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_json_parser);
criterion_main!(benches);
