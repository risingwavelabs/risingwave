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

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::{DataType, Timestamp, NaiveDateWrapper};
use risingwave_connector::parser::{JsonParser, SourceStreamChunkBuilder};
use risingwave_connector::source::SourceColumnDesc;

const NUM_RECORDS: usize = 1 << 18; // ~ 250,000

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
        NaiveDateWrapper::from_num_days_from_ce_uncheck((rng.gen::<u32>() % (1 << 20)) as i32).0,
        {
            let datetime = Timestamp::from_timestamp_uncheck((rng.gen::<u32>() % (1u32 << 28)) as i64, 0).0;
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
        SourceColumnDesc::simple("i32", DataType::Int32, ColumnId::from(0)),
        SourceColumnDesc::simple("bool", DataType::Boolean, ColumnId::from(2)),
        SourceColumnDesc::simple("i16", DataType::Int16, ColumnId::from(3)),
        SourceColumnDesc::simple("i64", DataType::Int64, ColumnId::from(4)),
        SourceColumnDesc::simple("f32", DataType::Float32, ColumnId::from(5)),
        SourceColumnDesc::simple("f64", DataType::Float64, ColumnId::from(6)),
        SourceColumnDesc::simple("varchar", DataType::Varchar, ColumnId::from(7)),
        SourceColumnDesc::simple("date", DataType::Date, ColumnId::from(8)),
        SourceColumnDesc::simple("timestamp", DataType::Timestamp, ColumnId::from(9)),
    ]
}

fn bench_json_parser(c: &mut Criterion) {
    let descs = get_descs();
    let parser = JsonParser::new_for_test(descs.clone()).unwrap();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let records = generate_all_json();
    c.bench_function("json_parser", |b| {
        b.to_async(&rt).iter_batched(
            || records.clone(),
            |records| async {
                let mut builder =
                    SourceStreamChunkBuilder::with_capacity(descs.clone(), NUM_RECORDS);
                for record in records {
                    let writer = builder.row_writer();
                    parser.parse_inner(record, writer).await.unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_json_parser);
criterion_main!(benches);
