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

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;

use super::*;
use crate::sink::catalog::{SinkEncode, SinkFormat, SinkType};
use crate::sink::{SinkImpl, build_sink, enforce_secret_sink, sink_is_exactly_once};

fn sink_param() -> SinkParam {
    SinkParam {
        sink_id: 1.into(),
        sink_name: "rabbitmq_test".into(),
        properties: BTreeMap::from([
            ("connector".into(), RABBITMQ_SINK.into()),
            ("url".into(), "amqp://localhost".into()),
            ("username".into(), "guest".into()),
            ("password".into(), "broker-secret".into()),
            ("exchange".into(), "".into()),
            ("routing_key".into(), "events".into()),
        ]),
        columns: vec![ColumnDesc::named("id", ColumnId::new(1), DataType::Int32)],
        downstream_pk: None,
        sink_type: SinkType::AppendOnly,
        ignore_delete: false,
        format_desc: Some(SinkFormatDesc {
            format: SinkFormat::AppendOnly,
            encode: SinkEncode::Json,
            options: BTreeMap::new(),
            secret_refs: BTreeMap::new(),
            key_encode: None,
            connection_id: None,
        }),
        db_name: "dev".into(),
        sink_from_name: "events".into(),
    }
}

#[tokio::test]
async fn registers_sink_without_legacy_type_option() {
    let param = sink_param();
    assert!(!sink_is_exactly_once(&param.properties).unwrap());
    let SinkImpl::RabbitMq(sink) = build_sink(param).unwrap() else {
        panic!("RabbitMQ sink is not registered");
    };
    sink.validate_unknown_fields().unwrap();
    assert_eq!(sink.config.r#type, SINK_TYPE_APPEND_ONLY);
    assert_eq!(sink.schema_subject, "events");
    assert_eq!(
        sink.build_encoder().await.unwrap().content_type(),
        "application/json"
    );
}

#[test]
fn rejects_non_append_only_input_and_conflicting_type() {
    for sink_type in [SinkType::Upsert, SinkType::Retract] {
        let mut param = sink_param();
        param.sink_type = sink_type;
        let error = build_sink(param).unwrap_err();
        assert!(error.to_string().contains("only supports append-only"));
    }
    let mut param = sink_param();
    param.properties.insert("type".into(), "upsert".into());
    assert!(
        build_sink(param)
            .unwrap_err()
            .to_string()
            .contains("only supports append-only")
    );
}

#[test]
fn requires_explicit_format_and_encoding() {
    let mut param = sink_param();
    param.format_desc = None;
    param.properties.insert("type".into(), "append-only".into());
    assert!(
        build_sink(param)
            .unwrap_err()
            .to_string()
            .contains("missing FORMAT ... ENCODE ...")
    );
}

#[test]
fn supports_planner_normalized_ignore_delete() {
    let mut param = sink_param();
    param.ignore_delete = true;
    param.properties.insert("routing_key".into(), "".into());
    let sink = RabbitMqSink::try_from(param).unwrap();
    assert_eq!(sink.schema_subject, "rabbitmq_test");
}

#[tokio::test]
async fn rejects_unknown_options_before_connecting() {
    let mut param = sink_param();
    param
        .properties
        .insert("routing_kye".into(), "events".into());
    let sink = RabbitMqSink::try_from(param).unwrap();
    assert!(
        sink.validate()
            .await
            .unwrap_err()
            .to_string()
            .contains("routing_kye")
    );
}

#[tokio::test]
async fn validates_encoding_before_connecting() {
    for (format, encode, key_encode, options, expected) in [
        (
            SinkFormat::Upsert,
            SinkEncode::Json,
            None,
            BTreeMap::new(),
            "only supports append-only",
        ),
        (
            SinkFormat::AppendOnly,
            SinkEncode::Avro,
            None,
            BTreeMap::new(),
            "encode unsupported",
        ),
        (
            SinkFormat::AppendOnly,
            SinkEncode::Json,
            Some(SinkEncode::Text),
            BTreeMap::new(),
            "does not support KEY ENCODE",
        ),
        (
            SinkFormat::AppendOnly,
            SinkEncode::Json,
            None,
            BTreeMap::from([("timestamptz.handling.mode".into(), "invalid".into())]),
            "unrecognized timestamptz.handling.mode",
        ),
        (
            SinkFormat::AppendOnly,
            SinkEncode::Json,
            None,
            BTreeMap::from([("jsonb.handling.mode".into(), "invalid".into())]),
            "unrecognized jsonb.handling.mode",
        ),
    ] {
        let mut param = sink_param();
        let desc = param.format_desc.as_mut().unwrap();
        desc.format = format;
        desc.encode = encode;
        desc.key_encode = key_encode;
        desc.options = options;
        let sink = RabbitMqSink::try_from(param).unwrap();
        let error = sink.validate().await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        let Err(error) = sink.new_log_sinker(SinkWriterParam::for_test()).await else {
            panic!("invalid encoding must not create a writer");
        };
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
async fn builds_protobuf_encoder_from_sink_parameters() {
    let mut param = sink_param();
    let desc = param.format_desc.as_mut().unwrap();
    desc.encode = SinkEncode::Protobuf;
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("codec/tests/test_data/all-types.pb");
    desc.options = BTreeMap::from([
        (
            "schema.location".into(),
            Url::from_file_path(path).unwrap().to_string(),
        ),
        ("message".into(), "all_types.AllTypes.NestedMessage".into()),
    ]);
    let sink = RabbitMqSink::try_from(param).unwrap();
    assert_eq!(
        sink.build_encoder().await.unwrap().content_type(),
        "application/x-protobuf"
    );
}

#[test]
fn enforces_password_secret_through_sink_dispatch() {
    let mut props = sink_param().properties;
    assert!(
        enforce_secret_sink(&props)
            .unwrap_err()
            .to_string()
            .contains("password")
    );
    // Resolved secret references are absent from the plaintext property keys.
    props.remove("password");
    enforce_secret_sink(&props).unwrap();
}

#[test]
fn debug_output_does_not_expose_resolved_secrets() {
    let mut param = sink_param();
    param
        .format_desc
        .as_mut()
        .unwrap()
        .options
        .insert("schema.registry.password".into(), "registry-secret".into());
    let debug = format!("{:?}", build_sink(param).unwrap());
    assert!(!debug.contains("broker-secret"));
    assert!(!debug.contains("registry-secret"));
}
