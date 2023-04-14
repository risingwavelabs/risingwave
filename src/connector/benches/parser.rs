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

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use maplit::hashmap;
use rand::Rng;
use risingwave_common::types::DataType;
use risingwave_connector::parser::{JsonParser, SourceStreamChunkBuilder};
use risingwave_connector::source::SourceColumnDesc;
use serde_json::json;
use tokio::runtime::Runtime;

fn gen_input(mode: &str, chunk_size: usize, chunk_num: usize) -> Vec<Vec<Vec<u8>>> {
    let mut input = Vec::with_capacity(chunk_num);
    for _ in 0..chunk_num {
        let mut input_inner = Vec::with_capacity(chunk_size);
        for _ in 0..chunk_size {
            input_inner.push(match mode {
                "match" => r#"{"alpha": 1, "bravo": 2, "charlie": 3, "delta": 4}"#
                    .as_bytes()
                    .to_vec(),
                "mismatch" => {
                    let convert_case = |s: &str| -> String {
                        let mut rng = rand::thread_rng();
                        let mut result = "".to_string();
                        for char in s.chars() {
                            if rng.gen_bool(0.5) {
                                result.push(char.to_uppercase().to_string().parse().unwrap());
                            } else {
                                result.push(char.to_lowercase().to_string().parse().unwrap());
                            }
                        }
                        result
                    };
                    let value = hashmap! {
                        convert_case("alpha") => json!(1),
                        convert_case("bravo") => json!(2),
                        convert_case("charlie") => json!(3),
                        convert_case("delta") => json!(4),
                    };
                    serde_json::to_string(&value).unwrap().as_bytes().to_vec()
                }
                _ => unreachable!(),
            });
        }
        input.push(input_inner);
    }
    input
}

fn create_parser(
    chunk_size: usize,
    chunk_num: usize,
    mode: &str,
) -> (JsonParser, Vec<SourceColumnDesc>, Vec<Vec<Vec<u8>>>) {
    let desc = vec![
        SourceColumnDesc::simple("alpha", DataType::Int16, 0.into()),
        SourceColumnDesc::simple("bravo", DataType::Int32, 1.into()),
        SourceColumnDesc::simple("charlie", DataType::Int64, 2.into()),
        SourceColumnDesc::simple("delta", DataType::Int256, 3.into()),
    ];
    let parser = JsonParser::new(desc.clone(), Default::default()).unwrap();
    let input = gen_input(mode, chunk_size, chunk_num);
    (parser, desc, input)
}

async fn parse(parser: JsonParser, column_desc: Vec<SourceColumnDesc>, input: Vec<Vec<Vec<u8>>>) {
    for input_inner in input {
        let mut builder =
            SourceStreamChunkBuilder::with_capacity(column_desc.clone(), input_inner.len());
        for payload in input_inner {
            let row_writer = builder.row_writer();
            parser.parse_inner(payload, row_writer).await.unwrap();
        }
        builder.finish();
    }
}

fn bench_parse_match_case(c: &mut Criterion) {
    const TOTAL_SIZE: usize = 1024 * 1024usize;
    let rt = Runtime::new().unwrap();

    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("ParseMatchCase", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = TOTAL_SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_parser(chunk_size, chunk_num, "match"),
                    |(parser, column_desc, input)| parse(parser, column_desc, input),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

fn bench_parse_not_match_case(c: &mut Criterion) {
    const TOTAL_SIZE: usize = 1024 * 1024usize;
    let rt = Runtime::new().unwrap();

    for chunk_size in &[32, 128, 512, 1024, 2048, 4096] {
        c.bench_with_input(
            BenchmarkId::new("ParseMatchCase", chunk_size),
            chunk_size,
            |b, &chunk_size| {
                let chunk_num = TOTAL_SIZE / chunk_size;
                b.to_async(&rt).iter_batched(
                    || create_parser(chunk_size, chunk_num, "mismatch"),
                    |(parser, column_desc, input)| parse(parser, column_desc, input),
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, bench_parse_match_case, bench_parse_not_match_case);
criterion_main!(benches);
