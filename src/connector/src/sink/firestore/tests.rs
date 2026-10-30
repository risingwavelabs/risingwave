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

use gcloud_sdk::google::firestore::v1::value::ValueType;
use gcloud_sdk::google::rpc::Status;
use risingwave_common::catalog::Field;
use risingwave_common::types::{JsonbVal, Scalar as _};

use super::*;

// The SDK uses real HTTP/2 sockets; madsim's tonic replacement has no server API.
// Keep encoding and validation tests in simulation, and run wire tests on Tokio.
#[cfg(not(madsim))]
mod grpc;

fn test_config() -> FirestoreConfig {
    FirestoreConfig {
        project_id: "demo-rw-firestore".to_owned(),
        collection: "sink-test".to_owned(),
        database_id: None,
        credentials: None,
        document_id_column: Some("id".to_owned()),
        max_batch_size: 1,
        max_future_send_nums: 2,
    }
}

fn test_schema() -> Schema {
    Schema::new(vec![
        Field::with_name(DataType::Varchar, "id"),
        Field::with_name(DataType::Int64, "value"),
    ])
}

#[test]
fn non_finite_floats_remain_doubles() -> Result<()> {
    for v in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 1.5] {
        let encoded = format_datum(Some(ScalarRefImpl::Float64(v.into())), &DataType::Float64)?;
        let Some(ValueType::DoubleValue(actual)) = encoded.value_type else {
            panic!("float changed its Firestore type");
        };
        assert!(actual.to_bits() == v.to_bits() || (actual.is_nan() && v.is_nan()));
        let encoded = format_datum(
            Some(ScalarRefImpl::Float32((v as f32).into())),
            &DataType::Float32,
        )?;
        assert!(matches!(
            encoded.value_type,
            Some(ValueType::DoubleValue(_))
        ));
    }
    Ok(())
}

#[test]
fn jsonb_preserves_nulls_and_integer_boundaries() -> Result<()> {
    let json: JsonbVal =
        r#"{"min":-9223372036854775808,"max":9223372036854775807,"a":[1,null,{"x":null}]}"#
            .parse()
            .unwrap();
    let encoded = format_datum(
        Some(ScalarRefImpl::Jsonb(json.as_scalar_ref())),
        &DataType::Jsonb,
    )?;
    let Some(ValueType::MapValue(map)) = encoded.value_type else {
        panic!()
    };
    assert_eq!(
        map.fields["min"].value_type,
        Some(ValueType::IntegerValue(i64::MIN))
    );
    assert_eq!(
        map.fields["max"].value_type,
        Some(ValueType::IntegerValue(i64::MAX))
    );
    let Some(ValueType::ArrayValue(array)) = &map.fields["a"].value_type else {
        panic!()
    };
    assert_eq!(array.values.len(), 3);
    assert_eq!(array.values[1].value_type, Some(ValueType::NullValue(0)));
    let Some(ValueType::MapValue(nested)) = &array.values[2].value_type else {
        panic!()
    };
    assert_eq!(nested.fields["x"].value_type, Some(ValueType::NullValue(0)));
    for text in ["9223372036854775808", "18446744073709551615"] {
        let json: JsonbVal = text.parse().unwrap();
        assert!(
            format_datum(
                Some(ScalarRefImpl::Jsonb(json.as_scalar_ref())),
                &DataType::Jsonb
            )
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn unsupported_json_shapes_fail_instead_of_becoming_strings() {
    for text in [
        "[[1]]".to_owned(),
        format!("{}0{}", r#"{"x":"#.repeat(21), "}".repeat(21)),
    ] {
        let json: JsonbVal = text.parse().unwrap();
        assert!(
            format_datum(
                Some(ScalarRefImpl::Jsonb(json.as_scalar_ref())),
                &DataType::Jsonb
            )
            .is_err()
        );
    }
    let text = format!("{}0{}", r#"{"x":"#.repeat(20), "}".repeat(20));
    let json: JsonbVal = text.parse().unwrap();
    assert!(
        format_datum(
            Some(ScalarRefImpl::Jsonb(json.as_scalar_ref())),
            &DataType::Jsonb
        )
        .is_ok()
    );
}

#[test]
fn invalid_config_is_rejected_before_connecting() {
    let mut config = test_config();
    config.document_id_column = Some("missing".to_owned());
    assert!(config.validate_schema(&test_schema(), &[0]).is_err());
    config.document_id_column = None;
    assert!(config.validate_schema(&test_schema(), &[]).is_err());
    config.max_future_send_nums = 0;
    assert!(config.validate_schema(&test_schema(), &[0]).is_err());
    config.max_future_send_nums = 1;
    for size in [0, 501] {
        config.max_batch_size = size;
        assert!(config.validate_schema(&test_schema(), &[0]).is_err());
    }
    assert!(validate_firestore_document_id(&"é".repeat(751)).is_err());
    assert!(validate_firestore_document_id("__reserved__").is_err());
}

#[test]
fn partial_or_incomplete_batch_ack_is_an_error() {
    let ok = Status::default();
    let failed = Status {
        code: 7,
        message: "injected".to_owned(),
        ..Default::default()
    };
    let document_ids = ["first".to_owned(), "second".to_owned()];
    assert!(check_batch_statuses(&[ok.clone(), failed], &document_ids).is_err());
    assert!(check_batch_statuses(std::slice::from_ref(&ok), &document_ids).is_err());
    assert!(check_batch_statuses(&[ok], &document_ids[..1]).is_ok());
}

#[test]
fn test_encode_pk_document_id_is_unambiguous_and_path_safe() -> Result<()> {
    let first = encode_pk_document_id(vec![
        JsonValue::String("a_b".to_owned()),
        JsonValue::String("c".to_owned()),
    ])?;
    let second = encode_pk_document_id(vec![
        JsonValue::String("a".to_owned()),
        JsonValue::String("b_c".to_owned()),
    ])?;
    let third = encode_pk_document_id(vec![JsonValue::Null])?;
    let fourth = encode_pk_document_id(vec![JsonValue::String("null".to_owned())])?;

    assert_ne!(first, second);
    assert_ne!(third, fourth);
    assert!(!first.contains('/'));
    assert!(!second.contains('/'));
    Ok(())
}

#[test]
fn test_validate_explicit_document_id() {
    assert!(validate_firestore_document_id("doc_1").is_ok());
    assert!(validate_firestore_document_id("a/b").is_err());
    assert!(validate_firestore_document_id("").is_err());
    assert!(validate_firestore_document_id(".").is_err());
    assert!(validate_firestore_document_id("..").is_err());
}

#[test]
fn test_format_int64_as_integer_value() -> Result<()> {
    for v in [i64::MIN, i64::MAX] {
        assert_eq!(
            format_datum(Some(ScalarRefImpl::Int64(v)), &DataType::Int64)?.value_type,
            Some(ValueType::IntegerValue(v))
        );
    }
    Ok(())
}
