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

#![feature(lazy_cell)]

use std::sync::LazyLock;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::catalog::ColumnId;
use risingwave_common::types::DataType;
use risingwave_connector::parser::{
    ByteStreamSourceParser, JsonParser, SourceParserIntoStreamExt, SpecificParserConfig,
};
use risingwave_connector::source::{
    BoxSourceStream, BoxSourceWithStateStream, SourceColumnDesc, SourceMessage, SourceMeta,
    StreamChunkWithState,
};
use tracing::Level;
use tracing_subscriber::prelude::*;

static BATCH: LazyLock<Vec<SourceMessage>> = LazyLock::new(make_batch);

fn make_batch() -> Vec<SourceMessage> {
    let mut generator = nexmark::EventGenerator::default()
        .with_type_filter(nexmark::event::EventType::Bid)
        .map(|e| match e {
            nexmark::event::Event::Bid(bid) => bid, // extract the bid event
            _ => unreachable!(),
        })
        .enumerate();

    let message_base = SourceMessage {
        split_id: "default".into(),
        key: None,
        payload: None,     // to be filled
        offset: "".into(), // to be filled
        meta: SourceMeta::Empty,
    };

    generator
        .by_ref()
        .take(1024)
        .map(|(i, e)| {
            let payload = serde_json::to_vec(&e).unwrap();
            SourceMessage {
                payload: Some(payload),
                offset: i.to_string(),
                ..message_base.clone()
            }
        })
        .collect_vec()
}

fn make_data_stream() -> BoxSourceStream {
    futures::future::ready(Ok(BATCH.clone()))
        .into_stream()
        .boxed()
}

fn make_parser() -> impl ByteStreamSourceParser {
    let columns = [
        ("auction", DataType::Int64),
        ("bidder", DataType::Int64),
        ("price", DataType::Int64),
        ("channel", DataType::Varchar),
        ("url", DataType::Varchar),
        ("date_time", DataType::Timestamp),
        ("extra", DataType::Varchar),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, (n, t))| SourceColumnDesc::simple(n, t, ColumnId::new(i as _)))
    .collect_vec();

    let props = SpecificParserConfig::DEFAULT_PLAIN_JSON;

    JsonParser::new(props, columns, Default::default()).unwrap()
}

fn make_stream_iter() -> impl Iterator<Item = StreamChunkWithState> {
    let mut stream: BoxSourceWithStateStream =
        make_parser().into_stream(make_data_stream()).boxed();

    std::iter::from_fn(move || {
        stream
            .try_next()
            .now_or_never() // there's actually no yield point
            .unwrap()
            .unwrap()
            .unwrap()
            .into()
    })
}

fn bench(c: &mut Criterion) {
    c.bench_function("parse_nexmark", |b| {
        b.iter_batched(
            make_stream_iter,
            |mut iter| iter.next().unwrap(),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("parse_nexmark_with_tracing", |b| {
        // Note: `From<S> for Dispatch` has global side effects. Moving this out of `bench_function`
        // does not work. Why?
        let dispatch: tracing::dispatcher::Dispatch = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer().with_filter(
                    tracing_subscriber::filter::Targets::new()
                        .with_target("risingwave_connector", Level::INFO),
                ),
            )
            .into();

        b.iter_batched(
            make_stream_iter,
            |mut iter| tracing::dispatcher::with_default(&dispatch, || iter.next().unwrap()),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
