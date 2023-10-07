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
use futures::executor::block_on;
use paste::paste;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::{DataType, Date, Timestamp};
use risingwave_connector::parser::{DebeziumParser, JsonParser, SourceStreamChunkBuilder};
use risingwave_connector::source::SourceColumnDesc;

macro_rules! create_debezium_bench_helpers {
    ($op:ident, $op_sym:expr, $bench_function:expr) => {
        paste! {
            fn [<bench_debezium_json_parser_$op>](c: &mut Criterion) {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                // Generate records
                let mut rng = rand::thread_rng();
                let mut records = Vec::with_capacity(NUM_RECORDS);
                for _ in 0..NUM_RECORDS {
                    let json_row = generate_debezium_json_row(&mut rng, $op_sym);
                    records.push(Some(json_row.into_bytes()));
                }

                c.bench_function($bench_function, |b| {
                    b.to_async(&rt).iter_batched(
                        || (block_on(DebeziumParser::new_for_test(get_descs())).unwrap(), records.clone()) ,
                        | (mut parser, records) | async move {
                            let mut builder =
                                SourceStreamChunkBuilder::with_capacity(get_descs(), NUM_RECORDS);
                            for record in records {
                                let writer = builder.row_writer();
                                parser.parse_inner(None, record, writer).await.unwrap();
                            }
                        },
                        BatchSize::SmallInput,
                    )
                });
            }
        }
    };
}

create_debezium_bench_helpers!(create, "c", "bench_debezium_json_parser_create");
create_debezium_bench_helpers!(read, "r", "bench_debezium_json_parser_read");
create_debezium_bench_helpers!(update, "u", "bench_debezium_json_parser_update");
create_debezium_bench_helpers!(delete, "d", "bench_debezium_json_parser_delete");

const NUM_RECORDS: usize = 1 << 18; // ~ 250,000

fn generate_json_row(rng: &mut impl Rng) -> String {
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
        Date::from_num_days_from_ce_uncheck((rng.gen::<u32>() % (1 << 20)) as i32).0,
        {
            let datetime = Timestamp::from_timestamp_uncheck((rng.gen::<u32>() % (1u32 << 28)) as i64, 0).0;
            format!("{:?} {:?}", datetime.date(), datetime.time())
        }
    )
}

fn generate_json_rows() -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let mut records = Vec::with_capacity(NUM_RECORDS);
    for _ in 0..NUM_RECORDS {
        records.push(generate_json_row(&mut rng).into_bytes());
    }
    records
}

fn generate_debezium_json_row(rng: &mut impl Rng, change_event: &str) -> String {
    let source = r#"{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null}"#;
    let (before, after) = match change_event {
        "c" => ("null".to_string(), generate_json_row(rng)),
        "r" => ("null".to_string(), generate_json_row(rng)),
        "u" => (generate_json_row(rng), generate_json_row(rng)),
        "d" => (generate_json_row(rng), "null".to_string()),
        _ => unreachable!(),
    };
    format!("{{\"before\": {before}, \"after\": {after}, \"source\": {source}, \"op\": \"{change_event}\", \"ts_ms\":1639551564960, \"transaction\":null}}")
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
    let records = generate_json_rows();
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

criterion_group!(
    benches,
    bench_json_parser,
    bench_debezium_json_parser_create,
    bench_debezium_json_parser_read,
    bench_debezium_json_parser_update,
    bench_debezium_json_parser_delete
);
criterion_main!(benches);
