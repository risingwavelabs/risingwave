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

//! Benchmark for JSON batch processing vs individual parsing

use criterion::{Criterion, criterion_group, criterion_main};
use risingwave_common::types::DataType;
use risingwave_connector::parser::JsonProperties;
use risingwave_connector::parser::batch_json_parser::BatchJsonAccessBuilder;
use risingwave_connector::parser::JsonAccessBuilder;
use risingwave_connector::parser::Access;
use risingwave_connector::source::SourceMeta;
use tokio::runtime::Runtime;
use risingwave_connector::parser::access_builder::AccessBuilder;
use risingwave_connector::parser::unified::AccessImpl;

/// Create test JSON payloads for benchmarking
fn create_test_payloads() -> (Vec<u8>, Vec<u8>) {
    // Single object format
    let single_payload = br#"{"id": 1, "name": "test", "value": 42.5}"#.to_vec();

    // Array format with same data
    let array_payload = br#"[
        {"id": 1, "name": "test1", "value": 42.5},
        {"id": 2, "name": "test2", "value": 43.5},
        {"id": 3, "name": "test3", "value": 44.5},
        {"id": 4, "name": "test4", "value": 45.5},
        {"id": 5, "name": "test5", "value": 46.5}
    ]"#
    .to_vec();

    (single_payload, array_payload)
}

/// Create larger payloads for scalability testing
fn create_large_payloads(batch_size: usize) -> (Vec<Vec<u8>>, Vec<u8>) {
    let mut individual_payloads = Vec::new();
    let mut array_objects = Vec::new();

    for i in 0..batch_size {
        let single = format!(
            r#"{{"id": {}, "name": "item_{}", "value": {}}}"#,
            i,
            i,
            i as f64 * 1.5
        );
        individual_payloads.push(single.as_bytes().to_vec());

        let array_obj = format!(
            r#"{{"id": {}, "name": "item_{}", "value": {}}}"#,
            i,
            i,
            i as f64 * 1.5
        );
        array_objects.push(array_obj);
    }

    let array_payload = format!("[{}]", array_objects.join(",")).as_bytes().to_vec();

    (individual_payloads, array_payload)
}

/// Benchmark individual JSON parsing (current approach)
fn bench_individual_parsing(c: &mut Criterion) {
    let config = JsonProperties::default();
    let mut builder = JsonAccessBuilder::new(config).unwrap();
    let runtime = Runtime::new().unwrap();

    let payload = br#"{"id": 1, "name": "test", "value": 42.5}"#.to_vec();

    let mut group = c.benchmark_group("json_parsing");

    group.bench_function("individual_parsing", |b| {
        b.iter(|| {
            let access = runtime.block_on(async {
                builder.generate_accessor(payload.clone(), &SourceMeta::Empty).await
            }).unwrap();
            let _ = access.access(&["id"], &DataType::Int32).unwrap();
            let _ = access.access(&["name"], &DataType::Varchar).unwrap();
            let _ = access.access(&["value"], &DataType::Float64).unwrap();
        })
    });

    group.finish();
}

/// Benchmark batch JSON array parsing
fn bench_batch_parsing(c: &mut Criterion) {
    let config = JsonProperties::default();
    let mut builder = BatchJsonAccessBuilder::new(config, true).unwrap();

    let (_, array_payload) = create_test_payloads();

    let mut group = c.benchmark_group("json_batch_parsing");

    group.bench_function("batch_array_parsing_5_objects", |b| {
        b.iter(|| {
            let accesses = builder.parse_to_batch(array_payload.clone()).unwrap();
            assert_eq!(accesses.len(), 5);

            for access in accesses {
                let _ = access.access(&["id"], &DataType::Int32).unwrap();
                let _ = access.access(&["name"], &DataType::Varchar).unwrap();
                let _ = access.access(&["value"], &DataType::Float64).unwrap();
            }
        })
    });

    group.finish();
}

/// Benchmark scalability comparison
fn bench_scalability_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_scalability");

    for batch_size in [1, 5, 10, 20, 50] {
        let config = JsonProperties::default();
        let mut batch_builder = BatchJsonAccessBuilder::new(config.clone(), true).unwrap();
        let mut individual_builder = JsonAccessBuilder::new(config.clone()).unwrap();
        let runtime = Runtime::new().unwrap();

        let (individual_payloads, array_payload) = create_large_payloads(batch_size);

        // Benchmark individual parsing
        group.bench_with_input(
            format!("individual_{}_objects", batch_size),
            &batch_size,
            |b, &_| {
                b.iter(|| {
                    runtime.block_on(async {
                        for payload in &individual_payloads {
                            let access = individual_builder
                                .generate_accessor(payload.clone(), &SourceMeta::Empty)
                                .await
                                .unwrap();
                            let _ = access.access(&["id"], &DataType::Int32).unwrap();
                        }
                    })
                })
            },
        );

        // Benchmark batch parsing
        group.bench_with_input(
            format!("batch_{}_objects", batch_size),
            &batch_size,
            |b, &_| {
                b.iter(|| {
                    let accesses = batch_builder.parse_to_batch(array_payload.clone()).unwrap();
                    assert_eq!(accesses.len(), batch_size);

                    for access in accesses {
                        if let AccessImpl::Json(json_access) = access {
                            let _ = json_access.access(&["id"], &DataType::Int32).unwrap();
                        }
                    }
                })
            },
        );
    }

    group.finish();
}

/// Benchmark memory usage patterns
fn bench_memory_efficiency(c: &mut Criterion) {
    let config = JsonProperties::default();
    let mut builder = BatchJsonAccessBuilder::new(config, true).unwrap();

    let small_array = br#"[{"id": 1}, {"id": 2}]"#.to_vec();
    let large_array = br#"[{"id": 1, "name": "test", "data": {"nested": {"deep": "value"}}}, {"id": 2, "name": "test2", "data": {"nested": {"deep": "value2"}}}]"#.to_vec();

    let mut group = c.benchmark_group("json_memory_efficiency");

    group.bench_function("small_array_2_objects", |b| {
        b.iter(|| {
            let accesses = builder.parse_to_batch(small_array.clone()).unwrap();
            assert_eq!(accesses.len(), 2);
        })
    });

    group.bench_function("large_array_2_objects", |b| {
        b.iter(|| {
            let accesses = builder.parse_to_batch(large_array.clone()).unwrap();
            assert_eq!(accesses.len(), 2);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_individual_parsing,
    bench_batch_parsing,
    bench_scalability_comparison,
    bench_memory_efficiency,
);
criterion_main!(benches);
