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

//! Integration benchmark for parsing protobuf messages.
//!
//! To cover the code path in real-world scenarios, the parser is created through
//! `ByteStreamSourceParserImpl::create` based on the given configuration, rather
//! than depending on a specific internal implementation.

use std::collections::HashSet;
use std::sync::LazyLock;

use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use futures::{FutureExt, StreamExt, TryStreamExt};
use itertools::Itertools;
use prost_reflect::prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::ColumnId;
use risingwave_connector::parser::{
    ByteStreamSourceParserImpl, CommonParserConfig, EncodingProperties, ParserConfig,
    ProtobufParserConfig, ProtobufProperties, ProtocolProperties, SchemaLocation,
    SpecificParserConfig,
};
use risingwave_connector::source::{
    BoxSourceChunkStream, BoxSourceMessageStream, SourceColumnDesc, SourceMessage, SourceMeta,
};
use tokio::runtime::Runtime;
use tracing::Level;
use tracing_subscriber::prelude::*;

static BATCH: LazyLock<Vec<SourceMessage>> = LazyLock::new(|| make_batch());

fn load_protobuf_descriptor() -> prost_reflect::MessageDescriptor {
    let pool_path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("codec/tests/test_data/all-types.pb");
    let pool_bytes = std::fs::read(pool_path).unwrap();
    let pool = DescriptorPool::decode(pool_bytes.as_ref()).unwrap();
    pool.get_message_by_name("all_types.AllTypes").unwrap()
}

/// Generate a protobuf message with various field types populated
/// This creates realistic varied data for benchmarking
fn create_sample_protobuf_data(index: usize) -> Vec<u8> {
    let descriptor = load_protobuf_descriptor();

    // Create a sample AllTypes message with various field types populated
    let mut message = DynamicMessage::new(descriptor.clone());

    // Generate varied data based on the index to simulate real-world diversity
    let base_val = index as i64;

    // Basic scalar fields using prost_reflect::Value
    message.set_field_by_name(
        "double_field",
        Value::F64(123.456 + (base_val as f64 * 0.1)),
    );
    message.set_field_by_name("float_field", Value::F32(78.9 + (base_val as f32 * 0.01)));
    message.set_field_by_name("int32_field", Value::I32(42 + (base_val as i32)));
    message.set_field_by_name("int64_field", Value::I64(1234567890 + base_val));
    message.set_field_by_name("uint32_field", Value::U32(98765 + (base_val as u32)));
    message.set_field_by_name("uint64_field", Value::U64(9876543210 + (base_val as u64)));
    message.set_field_by_name("sint32_field", Value::I32(-12345 + (base_val as i32)));
    message.set_field_by_name("sint64_field", Value::I64(-987654321 + base_val));
    message.set_field_by_name("fixed32_field", Value::U32(1234 + (base_val as u32)));
    message.set_field_by_name("fixed64_field", Value::U64(5678 + (base_val as u64)));
    message.set_field_by_name("sfixed32_field", Value::I32(-56789 + (base_val as i32)));
    message.set_field_by_name("sfixed64_field", Value::I64(-123456 + base_val));
    message.set_field_by_name("bool_field", Value::Bool(base_val % 2 == 0));
    message.set_field_by_name(
        "string_field",
        Value::String(format!("Hello, RisingWave {}!", index)),
    );
    message.set_field_by_name(
        "bytes_field",
        Value::Bytes(format!("binary data {}", index).into_bytes().into()),
    );

    // Enum field (cycle through enum values)
    message.set_field_by_name("enum_field", Value::EnumNumber((base_val % 3) as i32));

    // Nested message
    if let Some(nested_field) = descriptor.get_field_by_name("nested_message_field") {
        if let Some(nested_descriptor) = nested_field.kind().as_message() {
            let mut nested_message = DynamicMessage::new(nested_descriptor.clone());
            nested_message.set_field_by_name("id", Value::I32(100 + (base_val as i32)));
            nested_message
                .set_field_by_name("name", Value::String(format!("nested_name_{}", index)));
            message.set_field_by_name("nested_message_field", Value::Message(nested_message));
        }
    }

    // Repeated field with varying length
    let repeated_count = (base_val % 10) + 1;
    let repeated_values = (0..repeated_count)
        .map(|i| Value::I32((base_val + i) as i32))
        .collect();
    message.set_field_by_name("repeated_int_field", Value::List(repeated_values));

    // Oneof field (alternate between different oneof options)
    match base_val % 3 {
        0 => message.set_field_by_name(
            "oneof_string",
            Value::String(format!("oneof_string_{}", index)),
        ),
        1 => message.set_field_by_name("oneof_int32", Value::I32(base_val as i32)),
        _ => message.set_field_by_name("oneof_enum", Value::EnumNumber(1)),
    }

    message.set_field_by_name(
        "map_field",
        Value::Map(std::collections::HashMap::<MapKey, Value>::from([
            (MapKey::String(format!("key1_{}", index)), Value::I32(1)),
            (MapKey::String(format!("key2_{}", index)), Value::I32(2)),
        ])),
    );

    message.encode_to_vec()
}

fn make_batch() -> Vec<SourceMessage> {
    let message_base = SourceMessage {
        split_id: "default".into(),
        key: None,
        payload: None,     // to be filled
        offset: "".into(), // to be filled
        meta: SourceMeta::Empty,
    };

    (0..1024)
        .map(|i| {
            // Generate a unique protobuf message for each index
            let payload = create_sample_protobuf_data(i);

            // For struct variant, the same data is used but parsed differently
            // This simulates more complex parsing scenarios where data is nested

            SourceMessage {
                payload: Some(payload),
                offset: i.to_string(),
                ..message_base.clone()
            }
        })
        .collect_vec()
}

fn make_data_stream() -> BoxSourceMessageStream {
    futures::future::ready(Ok(BATCH.clone()))
        .into_stream()
        .boxed()
}

fn make_parser(rt: &Runtime) -> ByteStreamSourceParserImpl {
    let _descriptor = load_protobuf_descriptor();

    let protobuf_properties = ProtobufProperties {
        schema_location: SchemaLocation::File {
            url: format!(
                "file://{}/codec/tests/test_data/all-types.pb",
                env!("CARGO_MANIFEST_DIR")
            ),
            aws_auth_props: None,
        },
        message_name: "all_types.AllTypes".to_string(),
        key_message_name: None,
        messages_as_jsonb: HashSet::from(["google.protobuf.Any".to_string()]),
    };

    let encoding_props = EncodingProperties::Protobuf(protobuf_properties);
    let column_fields = rt.block_on(async {
        ProtobufParserConfig::new(encoding_props.clone())
            .await
            .unwrap()
            .map_to_columns()
            .unwrap()
    });

    let rw_columns = column_fields
        .into_iter()
        .enumerate()
        .map(|(idx, field)| {
            SourceColumnDesc::simple(field.name, field.data_type, ColumnId::new(idx as _))
        })
        .collect_vec();
    let config = ParserConfig {
        common: CommonParserConfig { rw_columns },
        specific: SpecificParserConfig {
            encoding_config: encoding_props,
            protocol_config: ProtocolProperties::Plain,
        },
    };

    ByteStreamSourceParserImpl::create_for_test(config).expect("Failed to create parser")
}

fn make_stream_iter(rt: &Runtime) -> impl Iterator<Item = StreamChunk> {
    let mut stream: BoxSourceChunkStream = make_parser(rt).parse_stream(make_data_stream()).boxed();

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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    c.bench_function("parse_protobuf_all_types", |b| {
        b.iter_batched(
            || make_stream_iter(&rt),
            |mut iter| iter.next().unwrap(),
            BatchSize::SmallInput,
        )
    });

    c.bench_function("parse_protobuf_with_tracing", |b| {
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
            || make_stream_iter(&rt),
            |mut iter| tracing::dispatcher::with_default(&dispatch, || iter.next().unwrap()),
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
