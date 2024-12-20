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

//! Benchmark for comparing the performance of parsing JSON records directly
//! through the `JsonParser` versus indirectly through the `PlainParser`.

mod json_common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use json_common::*;
use old_json_parser::JsonParser;
use risingwave_connector::parser::plain_parser::PlainParser;
use risingwave_connector::parser::{SourceStreamChunkBuilder, SpecificParserConfig};
use risingwave_connector::source::{SourceContext, SourceCtrlOpts};

// The original implementation used to parse JSON prior to #13707.
mod old_json_parser {
    use anyhow::Context as _;
    use itertools::{Either, Itertools as _};
    use risingwave_common::{bail, try_match_expand};
    use risingwave_connector::error::ConnectorResult;
    use risingwave_connector::parser::{
        Access as _, EncodingProperties, JsonAccess, SourceStreamChunkRowWriter,
    };
    use risingwave_connector::source::{SourceColumnDesc, SourceContextRef};

    use super::*;

    /// Parser for JSON format
    #[derive(Debug)]
    pub struct JsonParser {
        _rw_columns: Vec<SourceColumnDesc>,
        _source_ctx: SourceContextRef,
        // If schema registry is used, the starting index of payload is 5.
        payload_start_idx: usize,
    }

    impl JsonParser {
        pub fn new(
            props: SpecificParserConfig,
            rw_columns: Vec<SourceColumnDesc>,
            source_ctx: SourceContextRef,
        ) -> ConnectorResult<Self> {
            let json_config = try_match_expand!(props.encoding_config, EncodingProperties::Json)?;
            let payload_start_idx = if json_config.use_schema_registry {
                5
            } else {
                0
            };
            Ok(Self {
                _rw_columns: rw_columns,
                _source_ctx: source_ctx,
                payload_start_idx,
            })
        }

        #[allow(clippy::unused_async)]
        pub async fn parse_inner(
            &self,
            mut payload: Vec<u8>,
            mut writer: SourceStreamChunkRowWriter<'_>,
        ) -> ConnectorResult<()> {
            let value = simd_json::to_borrowed_value(&mut payload[self.payload_start_idx..])
                .context("failed to parse json payload")?;
            let values = if let simd_json::BorrowedValue::Array(arr) = value {
                Either::Left(arr.into_iter())
            } else {
                Either::Right(std::iter::once(value))
            };

            let mut errors = Vec::new();
            for value in values {
                let accessor = JsonAccess::new(value);
                match writer.do_insert(|column| accessor.access(&[&column.name], &column.data_type))
                {
                    Ok(_) => {}
                    Err(err) => errors.push(err),
                }
            }

            if errors.is_empty() {
                Ok(())
            } else {
                bail!(
                    "failed to parse {} row(s) in a single json message: {}",
                    errors.len(),
                    errors.iter().format(", ")
                );
            }
        }
    }
}

fn generate_json_rows() -> Vec<Vec<u8>> {
    let mut rng = rand::thread_rng();
    let mut records = Vec::with_capacity(NUM_RECORDS);
    for _ in 0..NUM_RECORDS {
        records.push(generate_json_row(&mut rng).into_bytes());
    }
    records
}

fn bench_plain_parser_and_json_parser(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let records = generate_json_rows();

    let mut group = c.benchmark_group("plain parser and json parser comparison");

    group.bench_function("plain_parser", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let parser = block_on(PlainParser::new(
                    SpecificParserConfig::DEFAULT_PLAIN_JSON,
                    get_descs(),
                    SourceContext::dummy().into(),
                ))
                .unwrap();
                (parser, records.clone())
            },
            |(mut parser, records)| async move {
                let mut builder = SourceStreamChunkBuilder::new(
                    get_descs(),
                    SourceCtrlOpts {
                        chunk_size: NUM_RECORDS,
                        split_txn: false,
                    },
                );
                for record in records {
                    let writer = builder.row_writer();
                    parser
                        .parse_inner(None, Some(record), writer)
                        .await
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("json_parser", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let parser = JsonParser::new(
                    SpecificParserConfig::DEFAULT_PLAIN_JSON,
                    get_descs(),
                    SourceContext::dummy().into(),
                )
                .unwrap();
                (parser, records.clone())
            },
            |(parser, records)| async move {
                let mut builder = SourceStreamChunkBuilder::new(
                    get_descs(),
                    SourceCtrlOpts {
                        chunk_size: NUM_RECORDS,
                        split_txn: false,
                    },
                );
                for record in records {
                    let writer = builder.row_writer();
                    parser.parse_inner(record, writer).await.unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_plain_parser_and_json_parser,);
criterion_main!(benches);
