// Copyright 2026 RisingWave Labs
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

use apache_avro::{Schema, from_avro_datum};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::executor::block_on;
use itertools::Itertools;
use risingwave_common::catalog::ColumnId;
use risingwave_connector::parser::plain_parser::PlainParser;
use risingwave_connector::parser::{
    AvroParserConfig, AvroProperties, EncodingProperties, ParseResult, ProtocolProperties,
    SchemaLocation, SourceStreamChunkBuilder, SourceStreamChunkRowWriter, SpecificParserConfig,
};
use risingwave_connector::source::{SourceColumnDesc, SourceContext, SourceCtrlOpts};
use risingwave_connector_codec::decoder::Access;
use risingwave_connector_codec::decoder::avro::{AvroAccess, AvroParseOptions};

const NUM_RECORDS: usize = 1 << 18;
const SIMPLE_RECORD_HEX: &str = "40800102127374725f76616c7565000000420000000000005040010000000100000001000000e80300000a0102030405";

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let hi = (chunk[0] as char).to_digit(16).unwrap();
        let lo = (chunk[1] as char).to_digit(16).unwrap();
        bytes.push(((hi << 4) | lo) as u8);
    }
    bytes
}

fn build_specific_config() -> SpecificParserConfig {
    SpecificParserConfig {
        encoding_config: EncodingProperties::Avro(AvroProperties {
            schema_location: SchemaLocation::File {
                url: format!(
                    "file://{}/src/test_data/simple-schema.avsc",
                    env!("CARGO_MANIFEST_DIR")
                ),
                aws_auth_props: None,
            },
            record_name: None,
            key_record_name: None,
            map_handling: None,
        }),
        protocol_config: ProtocolProperties::Plain,
    }
}

fn load_schema() -> Arc<Schema> {
    let schema = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/test_data/simple-schema.avsc"),
    )
    .unwrap();
    Arc::new(Schema::parse_str(&schema).unwrap())
}

fn build_descs(rt: &tokio::runtime::Runtime) -> Vec<SourceColumnDesc> {
    let specific = build_specific_config();
    let avro_props = specific.encoding_config.clone();
    rt.block_on(async {
        AvroParserConfig::new(avro_props)
            .await
            .unwrap()
            .map_to_columns()
            .unwrap()
            .into_iter()
            .enumerate()
            .map(|(idx, field)| {
                SourceColumnDesc::simple(field.name, field.data_type, ColumnId::new(idx as _))
            })
            .collect_vec()
    })
}

fn build_records() -> Vec<Vec<u8>> {
    let payload = decode_hex(SIMPLE_RECORD_HEX);
    std::iter::repeat_n(payload, NUM_RECORDS).collect()
}

fn bench_plain_avro_parser(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let records = build_records();
    let descs = build_descs(&rt);
    let schema = load_schema();
    let payload = decode_hex(SIMPLE_RECORD_HEX);

    c.bench_function("parse_avro_raw_datum_reader_schema_none", |b| {
        b.iter_batched(
            || payload.clone(),
            |payload| {
                let mut raw = payload.as_slice();
                let value = from_avro_datum(schema.as_ref(), &mut raw, None).unwrap();
                std::hint::black_box(value)
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("parse_avro_raw_datum_reader_schema_some", |b| {
        b.iter_batched(
            || payload.clone(),
            |payload| {
                let mut raw = payload.as_slice();
                let value =
                    from_avro_datum(schema.as_ref(), &mut raw, Some(schema.as_ref())).unwrap();
                std::hint::black_box(value)
            },
            BatchSize::SmallInput,
        )
    });

    c.bench_function("create_avro_parse_options", |b| {
        b.iter(|| {
            let options = AvroParseOptions::create(schema.as_ref());
            std::hint::black_box(options)
        })
    });

    c.bench_function("access_avro_all_columns", |b| {
        let mut raw = payload.as_slice();
        let value = from_avro_datum(schema.as_ref(), &mut raw, None).unwrap();
        let options = AvroParseOptions::create(schema.as_ref());
        let access = AvroAccess::new(&value, &options);
        b.iter(|| {
            for desc in &descs {
                std::hint::black_box(access.access(&[&desc.name], &desc.data_type).unwrap());
            }
        })
    });

    c.bench_function("materialize_avro_all_columns", |b| {
        let mut raw = payload.as_slice();
        let value = from_avro_datum(schema.as_ref(), &mut raw, None).unwrap();
        let options = AvroParseOptions::create(schema.as_ref());
        let access = AvroAccess::new(&value, &options);
        b.iter(|| {
            let mut builder = SourceStreamChunkBuilder::new(
                descs.clone(),
                SourceCtrlOpts {
                    chunk_size: 1,
                    split_txn: false,
                },
            );
            let mut writer: SourceStreamChunkRowWriter<'_> = builder.row_writer();
            writer
                .do_insert(|column| access.access(&[&column.name], &column.data_type))
                .unwrap();
            builder.finish_current_chunk();
            std::hint::black_box(builder.consume_ready_chunks().next().unwrap());
        })
    });

    c.bench_function("plain_avro_parser_simple_record", |b| {
        b.to_async(&rt).iter_batched(
            || {
                let parser = block_on(PlainParser::new(
                    build_specific_config(),
                    descs.clone(),
                    SourceContext::dummy().into(),
                ))
                .unwrap();
                (parser, records.clone(), descs.clone())
            },
            |(mut parser, records, descs)| async move {
                let mut builder = SourceStreamChunkBuilder::new(
                    descs,
                    SourceCtrlOpts {
                        chunk_size: NUM_RECORDS,
                        split_txn: false,
                    },
                );
                for record in records {
                    let writer: SourceStreamChunkRowWriter<'_> = builder.row_writer();
                    let result = parser
                        .parse_inner(None, Some(record), writer)
                        .await
                        .unwrap();
                    assert!(matches!(result, ParseResult::Rows));
                }
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench_plain_avro_parser);
criterion_main!(benches);
