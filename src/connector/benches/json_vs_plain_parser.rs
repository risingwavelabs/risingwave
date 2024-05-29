mod json_common;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::executor::block_on;
use json_common::*;
use risingwave_connector::parser::plain_parser::PlainParser;
use risingwave_connector::parser::{JsonParser, SourceStreamChunkBuilder, SpecificParserConfig};
use risingwave_connector::source::SourceContext;

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
                let mut builder = SourceStreamChunkBuilder::with_capacity(get_descs(), NUM_RECORDS);
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
            |(mut parser, records)| async move {
                let mut builder = SourceStreamChunkBuilder::with_capacity(get_descs(), NUM_RECORDS);
                for record in records {
                    let writer = builder.row_writer();
                    parser.parse_value(record, writer).await.unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_plain_parser_and_json_parser,);
criterion_main!(benches);
