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

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use risingwave_common::types::DataType;
use risingwave_connector::parser::memory_pool::{GlobalMemoryPool, ParserMemoryPool};
use risingwave_connector::parser::{
    ByteStreamSourceParserImpl, CommonParserConfig, JsonProperties, ParserConfig, SourceColumnDesc,
    SpecificParserConfig,
};
use risingwave_connector::source::{SourceContext, SourceCtrlOpts};

// Test data for JSON parsing
const JSON_PAYLOADS: &[&[u8]] = &[
    br#"{"id": 1, "name": "Alice", "age": 30, "active": true, "score": 85.5}"#,
    br#"{"id": 2, "name": "Bob", "age": 25, "active": false, "score": 92.3}"#,
    br#"{"id": 3, "name": "Charlie", "age": 35, "active": true, "score": 78.9}"#,
    br#"{"id": 4, "name": "Diana", "age": 28, "active": true, "score": 95.1}"#,
    br#"{"id": 5, "name": "Eve", "age": 32, "active": false, "score": 81.7}"#,
];

// CSV test data
const CSV_PAYLOADS: &[&[u8]] = &[
    b"1,Alice,30,true,85.5",
    b"2,Bob,25,false,92.3",
    b"3,Charlie,35,true,78.9",
    b"4,Diana,28,true,95.1",
    b"5,Eve,32,false,81.7",
];

fn create_test_columns() -> Vec<SourceColumnDesc> {
    vec![
        SourceColumnDesc::simple("id", DataType::Int32, 0.into()),
        SourceColumnDesc::simple("name", DataType::Varchar, 1.into()),
        SourceColumnDesc::simple("age", DataType::Int32, 2.into()),
        SourceColumnDesc::simple("active", DataType::Boolean, 3.into()),
        SourceColumnDesc::simple("score", DataType::Float32, 4.into()),
    ]
}

fn bench_memory_pool_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool_creation");

    group.bench_function("new_parser_memory_pool", |b| {
        b.iter(|| ParserMemoryPool::new())
    });

    group.bench_function("global_memory_pool", |b| b.iter(|| GlobalMemoryPool::new()));

    group.finish();
}

fn bench_json_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_parsing");
    let columns = create_test_columns();

    // Benchmark without memory pooling
    group.bench_function("json_without_pool", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let parser = ByteStreamSourceParserImpl::create_for_test(ParserConfig {
                    common: CommonParserConfig {
                        rw_columns: columns.clone(),
                    },
                    specific: SpecificParserConfig {
                        encoding_config: JsonProperties::default().into(),
                        protocol_config: Default::default(),
                    },
                })
                .unwrap();

                for payload in JSON_PAYLOADS {
                    let chunk = parser.parse(vec![payload.to_vec()]).await;
                    black_box(chunk);
                }
            });
    });

    // Benchmark with memory pooling
    group.bench_function("json_with_pool", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let pool = GlobalMemoryPool::new();
                let _ = pool.get_pool(); // Initialize pool

                let parser = ByteStreamSourceParserImpl::create_for_test(ParserConfig {
                    common: CommonParserConfig {
                        rw_columns: columns.clone(),
                    },
                    specific: SpecificParserConfig {
                        encoding_config: JsonProperties::default().into(),
                        protocol_config: Default::default(),
                    },
                })
                .unwrap();

                for payload in JSON_PAYLOADS {
                    let chunk = parser.parse(vec![payload.to_vec()]).await;
                    black_box(chunk);
                }
            });
    });

    group.finish();
}

fn bench_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");
    let columns = create_test_columns();

    group.bench_function("csv_without_pool", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                use risingwave_connector::parser::{CsvParser, CsvProperties};

                let mut parser = CsvParser::new(
                    columns.clone(),
                    CsvProperties {
                        delimiter: b',',
                        has_header: false,
                    },
                    SourceContext::dummy().into(),
                )
                .unwrap();

                let mut builder = risingwave_connector::parser::SourceStreamChunkBuilder::new(
                    columns.clone(),
                    SourceCtrlOpts::for_test(),
                );

                for payload in CSV_PAYLOADS {
                    parser
                        .parse_inner(payload.to_vec(), builder.row_writer())
                        .await
                        .unwrap();
                }
            });
    });

    group.bench_function("csv_with_pool", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                use risingwave_connector::parser::{CsvParser, CsvProperties};

                let pool = GlobalMemoryPool::new();
                let _ = pool.get_pool(); // Initialize pool

                let mut parser = CsvParser::new(
                    columns.clone(),
                    CsvProperties {
                        delimiter: b',',
                        has_header: false,
                    },
                    SourceContext::dummy().into(),
                )
                .unwrap();

                let mut builder = risingwave_connector::parser::SourceStreamChunkBuilder::new(
                    columns.clone(),
                    SourceCtrlOpts::for_test(),
                );

                for payload in CSV_PAYLOADS {
                    parser
                        .parse_inner(payload.to_vec(), builder.row_writer())
                        .await
                        .unwrap();
                }
            });
    });

    group.finish();
}

fn bench_buffer_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_allocation");
    let test_data =
        br#"{"id": 1, "name": "Alice", "age": 30, "active": true, "score": 85.5}"#.to_vec();

    group.bench_function("vec_with_capacity", |b| {
        b.iter(|| {
            let mut buffer = Vec::with_capacity(test_data.len());
            buffer.extend_from_slice(test_data);
            black_box(buffer);
        });
    });

    group.bench_function("memory_pool_buffer", |b| {
        b.iter(|| {
            let pool = GlobalMemoryPool::new();
            let buffer = pool.get_pool().get_buffer(test_data.len());
            buffer.extend_from_slice(test_data);
            black_box(buffer);
        });
    });

    group.finish();
}

fn bench_string_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_allocation");
    let test_strings = &["Alice", "Bob", "Charlie", "Diana", "Eve"];

    group.bench_function("standard_string", |b| {
        b.iter(|| {
            for s in test_strings {
                let string = s.to_string();
                black_box(string);
            }
        });
    });

    group.bench_function("memory_pool_string", |b| {
        b.iter(|| {
            let pool = GlobalMemoryPool::new();
            for s in test_strings {
                let mut string = pool.get_pool().get_string();
                string.push_str(s);
                black_box(string.to_string());
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_memory_pool_creation,
    bench_json_parsing,
    bench_csv_parsing,
    bench_buffer_allocation,
    bench_string_allocation
);
criterion_main!(benches);
