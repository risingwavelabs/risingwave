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

#[rustfmt::skip]
#[allow(clippy::all)]
mod recursive;
#[rustfmt::skip]
#[allow(clippy::all)]
mod all_types;
use std::collections::HashMap;

use anyhow::Context;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use risingwave_connector_codec::common::protobuf::compile_pb;
use risingwave_connector_codec::decoder::protobuf::parser::*;
use risingwave_connector_codec::decoder::protobuf::ProtobufAccess;
use risingwave_connector_codec::decoder::Access;
use thiserror_ext::AsReport;

use crate::utils::*;

/// Refer to [crate level documentation](crate) for the ideas.
#[track_caller]
fn check(
    pb_schema: MessageDescriptor,
    pb_data: &[&[u8]],
    expected_risingwave_schema: expect_test::Expect,
    expected_risingwave_data: expect_test::Expect,
) {
    let rw_schema = pb_schema_to_column_descs(&pb_schema);

    if let Err(e) = rw_schema {
        expected_risingwave_schema.assert_eq(&e.to_report_string_pretty());
        expected_risingwave_data.assert_eq("");
        return;
    }

    let rw_schema = rw_schema
        .unwrap()
        .iter()
        .map(ColumnDesc::from)
        .collect_vec();
    expected_risingwave_schema.assert_eq(&format!(
        "{:#?}",
        rw_schema.iter().map(ColumnDescTestDisplay).collect_vec()
    ));

    let mut data_str = vec![];
    for data in pb_data {
        let access = ProtobufAccess::new(DynamicMessage::decode(pb_schema.clone(), *data).unwrap());
        let mut row = vec![];
        for col in &rw_schema {
            let rw_data = access.access(&[&col.name], &col.data_type);
            match rw_data {
                Ok(data) => row.push(format!("{:#?}", DatumCowTestDisplay(&data))),
                Err(e) => row.push(format!(
                    "~~~~\nError at column `{}`: {}\n~~~~",
                    col.name,
                    e.to_report_string()
                )),
            }
        }
        data_str.push(format!("{}", row.iter().format("\n")));
    }

    expected_risingwave_data.assert_eq(&format!(
        "{}",
        data_str
            .iter()
            .format("\n================================================================\n")
    ));
}

fn load_message_descriptor(
    file_name: &str,
    message_name: &str,
) -> anyhow::Result<MessageDescriptor> {
    let location = "tests/test_data/".to_owned() + file_name;
    let file_content = fs_err::read_to_string(&location).unwrap();

    let pool = if file_name.ends_with(".proto") {
        let fd_set = compile_pb((location.clone(), file_content), [])?;
        DescriptorPool::from_file_descriptor_set(fd_set)
    } else {
        DescriptorPool::decode(file_content.as_bytes())
    }
    .with_context(|| format!("cannot build descriptor pool from schema `{location}`"))?;

    pool.get_message_by_name(message_name).with_context(|| {
        format!(
            "cannot find message `{}` in schema `{}`",
            message_name, location,
        )
    })
}

#[test]
fn test_simple_schema() -> anyhow::Result<()> {
    // Id:      123,
    // Address: "test address",
    // City:    "test city",
    // Zipcode: 456,
    // Rate:    1.2345,
    // Date:    "2021-01-01"
    static PRE_GEN_PROTO_DATA: &[u8] = b"\x08\x7b\x12\x0c\x74\x65\x73\x74\x20\x61\x64\x64\x72\x65\x73\x73\x1a\x09\x74\x65\x73\x74\x20\x63\x69\x74\x79\x20\xc8\x03\x2d\x19\x04\x9e\x3f\x32\x0a\x32\x30\x32\x31\x2d\x30\x31\x2d\x30\x31";

    let message_descriptor =
        load_message_descriptor("simple-schema.proto", "test.TestRecord").unwrap();

    // validate the binary data is correct
    let value = DynamicMessage::decode(message_descriptor.clone(), PRE_GEN_PROTO_DATA).unwrap();
    expect![[r#"
        [
            I32(
                123,
            ),
            String(
                "test address",
            ),
            String(
                "test city",
            ),
            I64(
                456,
            ),
            F32(
                1.2345,
            ),
            String(
                "2021-01-01",
            ),
        ]
    "#]]
    .assert_debug_eq(&value.fields().map(|f| f.1).collect_vec());

    check(
        message_descriptor,
        &[PRE_GEN_PROTO_DATA],
        expect![[r#"
            [
                id(#1): Int32,
                address(#2): Varchar,
                city(#3): Varchar,
                zipcode(#4): Int64,
                rate(#5): Float32,
                date(#6): Varchar,
            ]"#]],
        expect![[r#"
            Owned(Int32(123))
            Borrowed(Utf8("test address"))
            Borrowed(Utf8("test city"))
            Owned(Int64(456))
            Owned(Float32(OrderedFloat(1.2345)))
            Borrowed(Utf8("2021-01-01"))"#]],
    );

    Ok(())
}

#[test]
fn test_complex_schema() -> anyhow::Result<()> {
    let message_descriptor = load_message_descriptor("complex-schema.proto", "test.User").unwrap();

    check(
        message_descriptor,
        &[],
        expect![[r#"
            [
                id(#1): Int32,
                code(#2): Varchar,
                timestamp(#3): Int64,
                xfas(#4): List(
                    Struct {
                        device_model_id: Int32,
                        device_make_id: Int32,
                        ip: Varchar,
                    },
                ), type_name: test.Xfa,
                contacts(#7): Struct {
                    emails: List(Varchar),
                    phones: List(Varchar),
                }, type_name: test.Contacts, field_descs: [emails(#5): List(Varchar), phones(#6): List(Varchar)],
                sex(#8): Varchar,
            ]"#]],
        expect![""],
    );

    Ok(())
}

#[test]
fn test_any_schema() -> anyhow::Result<()> {
    let message_descriptor = load_message_descriptor("any-schema.proto", "test.TestAny").unwrap();

    // id: 12345
    // name {
    // type_url: "type.googleapis.com/test.Int32Value"
    // value: "\010\322\376\006"
    // }
    // Unpacked Int32Value from Any: value: 114514
    static ANY_DATA_1: &[u8] = b"\x08\xb9\x60\x12\x2b\x0a\x23\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x49\x6e\x74\x33\x32\x56\x61\x6c\x75\x65\x12\x04\x08\xd2\xfe\x06";

    // "id": 12345,
    // "any_value": {
    //     "type_url": "type.googleapis.com/test.AnyValue",
    //     "value": {
    //         "any_value_1": {
    //             "type_url": "type.googleapis.com/test.StringValue",
    //             "value": "114514"
    //         },
    //         "any_value_2": {
    //             "type_url": "type.googleapis.com/test.Int32Value",
    //             "value": 114514
    //         }
    //     }
    // }
    static ANY_DATA_2: &[u8] = b"\x08\xb9\x60\x12\x84\x01\x0a\x21\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x41\x6e\x79\x56\x61\x6c\x75\x65\x12\x5f\x0a\x30\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x56\x61\x6c\x75\x65\x12\x08\x0a\x06\x31\x31\x34\x35\x31\x34\x12\x2b\x0a\x23\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x49\x6e\x74\x33\x32\x56\x61\x6c\x75\x65\x12\x04\x08\xd2\xfe\x06";

    // id: 12345
    // name {
    //    type_url: "type.googleapis.com/test.StringValue"
    //    value: "\n\010John Doe"
    // }
    static ANY_DATA_3: &[u8] = b"\x08\xb9\x60\x12\x32\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x56\x61\x6c\x75\x65\x12\x0a\x0a\x08\x4a\x6f\x68\x6e\x20\x44\x6f\x65";

    // // id: 12345
    // // any_value: {
    // //    type_url: "type.googleapis.com/test.StringXalue"
    // //    value: "\n\010John Doe"
    // // }
    static ANY_DATA_INVALID: &[u8] = b"\x08\xb9\x60\x12\x32\x0a\x24\x74\x79\x70\x65\x2e\x67\x6f\x6f\x67\x6c\x65\x61\x70\x69\x73\x2e\x63\x6f\x6d\x2f\x74\x65\x73\x74\x2e\x53\x74\x72\x69\x6e\x67\x58\x61\x6c\x75\x65\x12\x0a\x0a\x08\x4a\x6f\x68\x6e\x20\x44\x6f\x65";

    // validate the binary data is correct
    {
        let value1 = DynamicMessage::decode(message_descriptor.clone(), ANY_DATA_1).unwrap();
        expect![[r#"
        [
            I32(
                12345,
            ),
            Message(
                DynamicMessage {
                    desc: MessageDescriptor {
                        name: "Any",
                        full_name: "google.protobuf.Any",
                        is_map_entry: false,
                        fields: [
                            FieldDescriptor {
                                name: "type_url",
                                full_name: "google.protobuf.Any.type_url",
                                json_name: "typeUrl",
                                number: 1,
                                kind: string,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                            FieldDescriptor {
                                name: "value",
                                full_name: "google.protobuf.Any.value",
                                json_name: "value",
                                number: 2,
                                kind: bytes,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                        ],
                        oneofs: [],
                    },
                    fields: DynamicMessageFieldSet {
                        fields: {
                            1: Value(
                                String(
                                    "type.googleapis.com/test.Int32Value",
                                ),
                            ),
                            2: Value(
                                Bytes(
                                    b"\x08\xd2\xfe\x06",
                                ),
                            ),
                        },
                    },
                },
            ),
        ]
    "#]]
        .assert_debug_eq(&value1.fields().map(|f| f.1).collect_vec());

        let value2 = DynamicMessage::decode(message_descriptor.clone(), ANY_DATA_2).unwrap();
        expect![[r#"
        [
            I32(
                12345,
            ),
            Message(
                DynamicMessage {
                    desc: MessageDescriptor {
                        name: "Any",
                        full_name: "google.protobuf.Any",
                        is_map_entry: false,
                        fields: [
                            FieldDescriptor {
                                name: "type_url",
                                full_name: "google.protobuf.Any.type_url",
                                json_name: "typeUrl",
                                number: 1,
                                kind: string,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                            FieldDescriptor {
                                name: "value",
                                full_name: "google.protobuf.Any.value",
                                json_name: "value",
                                number: 2,
                                kind: bytes,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                        ],
                        oneofs: [],
                    },
                    fields: DynamicMessageFieldSet {
                        fields: {
                            1: Value(
                                String(
                                    "type.googleapis.com/test.AnyValue",
                                ),
                            ),
                            2: Value(
                                Bytes(
                                    b"\n0\n$type.googleapis.com/test.StringValue\x12\x08\n\x06114514\x12+\n#type.googleapis.com/test.Int32Value\x12\x04\x08\xd2\xfe\x06",
                                ),
                            ),
                        },
                    },
                },
            ),
        ]
    "#]]
    .assert_debug_eq(&value2.fields().map(|f| f.1).collect_vec());

        let value3 = DynamicMessage::decode(message_descriptor.clone(), ANY_DATA_INVALID).unwrap();
        expect![[r#"
        [
            I32(
                12345,
            ),
            Message(
                DynamicMessage {
                    desc: MessageDescriptor {
                        name: "Any",
                        full_name: "google.protobuf.Any",
                        is_map_entry: false,
                        fields: [
                            FieldDescriptor {
                                name: "type_url",
                                full_name: "google.protobuf.Any.type_url",
                                json_name: "typeUrl",
                                number: 1,
                                kind: string,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                            FieldDescriptor {
                                name: "value",
                                full_name: "google.protobuf.Any.value",
                                json_name: "value",
                                number: 2,
                                kind: bytes,
                                cardinality: Optional,
                                containing_oneof: None,
                                default_value: None,
                                is_group: false,
                                is_list: false,
                                is_map: false,
                                is_packed: false,
                                supports_presence: false,
                            },
                        ],
                        oneofs: [],
                    },
                    fields: DynamicMessageFieldSet {
                        fields: {
                            1: Value(
                                String(
                                    "type.googleapis.com/test.StringXalue",
                                ),
                            ),
                            2: Value(
                                Bytes(
                                    b"\n\x08John Doe",
                                ),
                            ),
                        },
                    },
                },
            ),
        ]
    "#]]
        .assert_debug_eq(&value3.fields().map(|f| f.1).collect_vec());
    }

    check(
        message_descriptor,
        &[ANY_DATA_1, ANY_DATA_2, ANY_DATA_3, ANY_DATA_INVALID],
        expect![[r#"
            [
                id(#1): Int32,
                any_value(#4): Jsonb, type_name: google.protobuf.Any, field_descs: [type_url(#2): Varchar, value(#3): Bytea],
            ]"#]],
        expect![[r#"
            Owned(Int32(12345))
            Owned(Jsonb({
                "@type": "type.googleapis.com/test.Int32Value",
                "value": Number(114514),
            }))
            ================================================================
            Owned(Int32(12345))
            Owned(Jsonb({
                "@type": "type.googleapis.com/test.AnyValue",
                "anyValue1": {
                    "@type": "type.googleapis.com/test.StringValue",
                    "value": "114514",
                },
                "anyValue2": {
                    "@type": "type.googleapis.com/test.Int32Value",
                    "value": Number(114514),
                },
            }))
            ================================================================
            Owned(Int32(12345))
            Owned(Jsonb({
                "@type": "type.googleapis.com/test.StringValue",
                "value": "John Doe",
            }))
            ================================================================
            Owned(Int32(12345))
            ~~~~
            Error at column `any_value`: Fail to convert protobuf Any into jsonb: message 'test.StringXalue' not found
            ~~~~"#]],
    );

    Ok(())
}

#[test]
fn test_all_types() -> anyhow::Result<()> {
    use self::all_types::all_types::*;
    use self::all_types::*;

    let message_descriptor =
        load_message_descriptor("all-types.proto", "all_types.AllTypes").unwrap();

    let data = {
        AllTypes {
            double_field: 1.2345,
            float_field: 1.2345,
            int32_field: 42,
            int64_field: 1234567890,
            uint32_field: 98765,
            uint64_field: 9876543210,
            sint32_field: -12345,
            sint64_field: -987654321,
            fixed32_field: 1234,
            fixed64_field: 5678,
            sfixed32_field: -56789,
            sfixed64_field: -123456,
            bool_field: true,
            string_field: "Hello, Prost!".to_owned(),
            bytes_field: b"byte data".to_vec(),
            enum_field: EnumType::Option1 as i32,
            nested_message_field: Some(NestedMessage {
                id: 100,
                name: "Nested".to_owned(),
            }),
            repeated_int_field: vec![1, 2, 3, 4, 5],
            map_field: HashMap::from_iter([
                ("key1".to_owned(), 1),
                ("key2".to_owned(), 2),
                ("key3".to_owned(), 3),
            ]),
            timestamp_field: Some(::prost_types::Timestamp {
                seconds: 1630927032,
                nanos: 500000000,
            }),
            duration_field: Some(::prost_types::Duration {
                seconds: 60,
                nanos: 500000000,
            }),
            any_field: Some(::prost_types::Any {
                type_url: "type.googleapis.com/my_custom_type".to_owned(),
                value: b"My custom data".to_vec(),
            }),
            int32_value_field: Some(42),
            string_value_field: Some("Hello, Wrapper!".to_owned()),
            example_oneof: Some(ExampleOneof::OneofInt32(123)),
            map_struct_field: HashMap::from_iter([
                (
                    "key1".to_owned(),
                    NestedMessage {
                        id: 1,
                        name: "A".to_owned(),
                    },
                ),
                (
                    "key2".to_owned(),
                    NestedMessage {
                        id: 2,
                        name: "B".to_owned(),
                    },
                ),
            ]),
            map_enum_field: HashMap::from_iter([
                (1, EnumType::Option1 as i32),
                (2, EnumType::Option2 as i32),
            ]),
        }
    };
    let mut data_bytes = Vec::new();
    data.encode(&mut data_bytes).unwrap();

    check(
        message_descriptor,
        &[&data_bytes],
        expect![[r#"
            [
                double_field(#1): Float64,
                float_field(#2): Float32,
                int32_field(#3): Int32,
                int64_field(#4): Int64,
                uint32_field(#5): Int64,
                uint64_field(#6): Decimal,
                sint32_field(#7): Int32,
                sint64_field(#8): Int64,
                fixed32_field(#9): Int64,
                fixed64_field(#10): Decimal,
                sfixed32_field(#11): Int32,
                sfixed64_field(#12): Int64,
                bool_field(#13): Boolean,
                string_field(#14): Varchar,
                bytes_field(#15): Bytea,
                enum_field(#16): Varchar,
                nested_message_field(#19): Struct {
                    id: Int32,
                    name: Varchar,
                }, type_name: all_types.AllTypes.NestedMessage, field_descs: [id(#17): Int32, name(#18): Varchar],
                repeated_int_field(#20): List(Int32),
                oneof_string(#21): Varchar,
                oneof_int32(#22): Int32,
                oneof_enum(#23): Varchar,
                map_field(#26): Map(Varchar,Int32), type_name: all_types.AllTypes.MapFieldEntry, field_descs: [key(#24): Varchar, value(#25): Int32],
                timestamp_field(#29): Struct {
                    seconds: Int64,
                    nanos: Int32,
                }, type_name: google.protobuf.Timestamp, field_descs: [seconds(#27): Int64, nanos(#28): Int32],
                duration_field(#32): Struct {
                    seconds: Int64,
                    nanos: Int32,
                }, type_name: google.protobuf.Duration, field_descs: [seconds(#30): Int64, nanos(#31): Int32],
                any_field(#35): Jsonb, type_name: google.protobuf.Any, field_descs: [type_url(#33): Varchar, value(#34): Bytea],
                int32_value_field(#37): Struct { value: Int32 }, type_name: google.protobuf.Int32Value, field_descs: [value(#36): Int32],
                string_value_field(#39): Struct { value: Varchar }, type_name: google.protobuf.StringValue, field_descs: [value(#38): Varchar],
                map_struct_field(#44): Map(Varchar,Struct { id: Int32, name: Varchar }), type_name: all_types.AllTypes.MapStructFieldEntry, field_descs: [key(#40): Varchar, value(#43): Struct {
                    id: Int32,
                    name: Varchar,
                }, type_name: all_types.AllTypes.NestedMessage, field_descs: [id(#41): Int32, name(#42): Varchar]],
                map_enum_field(#47): Map(Int32,Varchar), type_name: all_types.AllTypes.MapEnumFieldEntry, field_descs: [key(#45): Int32, value(#46): Varchar],
            ]"#]],
        expect![[r#"
            Owned(Float64(OrderedFloat(1.2345)))
            Owned(Float32(OrderedFloat(1.2345)))
            Owned(Int32(42))
            Owned(Int64(1234567890))
            Owned(Int64(98765))
            Owned(Decimal(Normalized(9876543210)))
            Owned(Int32(-12345))
            Owned(Int64(-987654321))
            Owned(Int64(1234))
            Owned(Decimal(Normalized(5678)))
            Owned(Int32(-56789))
            Owned(Int64(-123456))
            Owned(Bool(true))
            Borrowed(Utf8("Hello, Prost!"))
            Borrowed(Bytea([98, 121, 116, 101, 32, 100, 97, 116, 97]))
            Owned(Utf8("OPTION1"))
            Owned(StructValue(
                Int32(100),
                Utf8("Nested"),
            ))
            Owned([
                Int32(1),
                Int32(2),
                Int32(3),
                Int32(4),
                Int32(5),
            ])
            Owned(Utf8(""))
            Owned(Int32(123))
            Owned(Utf8("DEFAULT"))
            Owned([
                StructValue(
                    Utf8("key1"),
                    Int32(1),
                ),
                StructValue(
                    Utf8("key2"),
                    Int32(2),
                ),
                StructValue(
                    Utf8("key3"),
                    Int32(3),
                ),
            ])
            Owned(StructValue(
                Int64(1630927032),
                Int32(500000000),
            ))
            Owned(StructValue(
                Int64(60),
                Int32(500000000),
            ))
            ~~~~
            Error at column `any_field`: Fail to convert protobuf Any into jsonb: message 'my_custom_type' not found
            ~~~~
            Owned(StructValue(Int32(42)))
            Owned(StructValue(Utf8("Hello, Wrapper!")))
            Owned([
                StructValue(
                    Utf8("key1"),
                    StructValue(
                        Int32(1),
                        Utf8("A"),
                    ),
                ),
                StructValue(
                    Utf8("key2"),
                    StructValue(
                        Int32(2),
                        Utf8("B"),
                    ),
                ),
            ])
            Owned([
                StructValue(
                    Int32(1),
                    Utf8("OPTION1"),
                ),
                StructValue(
                    Int32(2),
                    Utf8("OPTION2"),
                ),
            ])"#]],
    );

    Ok(())
}

#[test]
fn test_recursive() -> anyhow::Result<()> {
    let message_descriptor =
        load_message_descriptor("recursive.proto", "recursive.ComplexRecursiveMessage").unwrap();

    check(
        message_descriptor,
        &[],
        expect![[r#"
            failed to map protobuf type

            Caused by:
              circular reference detected: parent(recursive.ComplexRecursiveMessage.parent)->siblings(recursive.ComplexRecursiveMessage.Parent.siblings), conflict with parent(recursive.ComplexRecursiveMessage.parent), kind recursive.ComplexRecursiveMessage.Parent
        "#]],
        expect![""],
    );

    Ok(())
}
