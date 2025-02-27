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

//! Integration benchmark for parsing Nexmark events.
//!
//! To cover the code path in real-world scenarios, the parser is created through
//! `ByteStreamSourceParserImpl::create` based on the given configuration, rather
//! than depending on a specific internal implementation.

use std::sync::LazyLock;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::{DataType, StructType};
use risingwave_connector::parser::{
    ByteStreamSourceParserImpl, CommonParserConfig, ParserConfig, SpecificParserConfig,
};
use risingwave_connector::source::{
    BoxSourceChunkStream, BoxSourceMessageStream, SourceColumnDesc, SourceMessage, SourceMeta,
};
use tracing::Level;
use tracing_subscriber::prelude::*;

static BATCH: LazyLock<Vec<SourceMessage>> = LazyLock::new(|| make_batch(false));
static STRUCT_BATCH: LazyLock<Vec<SourceMessage>> = LazyLock::new(|| make_batch(true));

fn make_batch(use_struct: bool) -> Vec<SourceMessage> {
    let mut generator = nexmark::EventGenerator::default()
        .with_type_filter(nexmark::event::EventType::Bid)
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
        .take(16384)
        .map(|(i, event)| {
            let payload = if use_struct {
                serde_json::to_vec(&event).unwrap()
            } else {
                let nexmark::event::Event::Bid(bid) = event else {
                    unreachable!()
                };
                serde_json::to_vec(&bid).unwrap()
            };
            SourceMessage {
                payload: Some(payload),
                offset: i.to_string(),
                ..message_base.clone()
            }
        })
        .collect_vec()
}

fn make_data_stream(use_struct: bool) -> BoxSourceMessageStream {
    futures::future::ready(Ok(if use_struct {
        STRUCT_BATCH.clone()
    } else {
        BATCH.clone()
    }))
    .into_stream()
    .boxed()
}

fn make_parser(use_struct: bool) -> ByteStreamSourceParserImpl {
    let fields = vec![
        ("auction", DataType::Int64),
        ("bidder", DataType::Int64),
        ("price", DataType::Int64),
        ("channel", DataType::Varchar),
        ("url", DataType::Varchar),
        ("date_time", DataType::Timestamp),
        ("extra", DataType::Varchar),
    ];

    let rw_columns = if use_struct {
        let struct_type = StructType::new(fields).into();
        let struct_col = ColumnDesc::named("bid", 114514.into(), struct_type);
        vec![(&struct_col).into()]
    } else {
        fields
            .into_iter()
            .enumerate()
            .map(|(i, (n, t))| SourceColumnDesc::simple(n, t, ColumnId::new(i as _)))
            .collect_vec()
    };

    let config = ParserConfig {
        common: CommonParserConfig { rw_columns },
        specific: SpecificParserConfig::DEFAULT_PLAIN_JSON,
    };

    ByteStreamSourceParserImpl::create_for_test(config).unwrap()
}

fn make_stream_iter(use_struct: bool) -> impl Iterator<Item = StreamChunk> {
    let mut stream: BoxSourceChunkStream = make_parser(use_struct)
        .parse_stream(make_data_stream(use_struct))
        .boxed();

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
            || make_stream_iter(false),
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
            || make_stream_iter(false),
            |mut iter| tracing::dispatcher::with_default(&dispatch, || iter.next().unwrap()),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("parse_nexmark_struct_type", |b| {
        b.iter_batched(
            || make_stream_iter(true),
            |mut iter| iter.next().unwrap(),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
