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

//! Benchmark for Debezium JSON records with `DebeziumParser`.

mod json_common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use json_common::*;
use paste::paste;
use rand::Rng;
use risingwave_connector::parser::{DebeziumParser, SourceStreamChunkBuilder};
use risingwave_connector::source::SourceCtrlOpts;

fn generate_debezium_json_row(rng: &mut impl Rng, change_event: &str) -> String {
    let source = r#"{"version":"1.7.1.Final","connector":"mysql","name":"dbserver1","ts_ms":1639547113601,"snapshot":"true","db":"inventory","sequence":null,"table":"products","server_id":0,"gtid":null,"file":"mysql-bin.000003","pos":156,"row":0,"thread":null,"query":null}"#;
    let (before, after) = match change_event {
        "c" => ("null".to_owned(), generate_json_row(rng)),
        "r" => ("null".to_owned(), generate_json_row(rng)),
        "u" => (generate_json_row(rng), generate_json_row(rng)),
        "d" => (generate_json_row(rng), "null".to_owned()),
        _ => unreachable!(),
    };
    format!("{{\"before\": {before}, \"after\": {after}, \"source\": {source}, \"op\": \"{change_event}\", \"ts_ms\":1639551564960, \"transaction\":null}}")
}

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
                                SourceStreamChunkBuilder::new(get_descs(), SourceCtrlOpts {
                                    chunk_size: NUM_RECORDS,
                                    split_txn: false,
                                });
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

criterion_group!(
    benches,
    bench_debezium_json_parser_create,
    bench_debezium_json_parser_read,
    bench_debezium_json_parser_update,
    bench_debezium_json_parser_delete
);
criterion_main!(benches);
