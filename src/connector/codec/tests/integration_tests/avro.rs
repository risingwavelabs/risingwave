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

use anyhow::Context;
use apache_avro::from_avro_datum;
use risingwave_connector_codec::AvroSchema;
use risingwave_connector_codec::decoder::Access;
use risingwave_connector_codec::decoder::avro::{
    AvroAccess, AvroParseOptions, MapHandling, ResolvedAvroSchema, avro_schema_to_fields,
};
use thiserror_ext::AsReport;

use crate::utils::*;

/// Refer to `AvroAccessBuilder::parse_avro_value` for how Avro data looks like.
enum TestDataEncoding {
    /// Each data is a JSON encoded Avro value.
    ///
    /// Refer to: <https://avro.apache.org/docs/1.11.1/specification/#json-encoding>
    ///
    /// Specially, it handles `union` variants, and differentiates `bytes` from `string`.
    ///
    /// TODO: Not supported yet, because `apache_avro` doesn't support decoding JSON encoded avro..
    #[allow(dead_code)]
    Json,
    /// Each data is a binary encoded Avro value, converted to a hex string.
    ///
    /// Tool convert Avro JSON to hex string: <https://xxchan-vercel-playground.vercel.app/avro>
    HexBinary,
}

struct Config {
    /// TODO: For one test data, we can test all possible config options.
    map_handling: Option<MapHandling>,
    data_encoding: TestDataEncoding,
}

fn avro_schema_str_to_risingwave_schema(
    avro_schema: &str,
    config: &Config,
) -> anyhow::Result<(ResolvedAvroSchema, Vec<Field>)> {
    // manually implement some logic in AvroParserConfig::map_to_columns
    let avro_schema = AvroSchema::parse_str(avro_schema).context("failed to parse Avro schema")?;
    let resolved_schema =
        ResolvedAvroSchema::create(avro_schema.into()).context("failed to resolve Avro schema")?;

    let rw_schema = avro_schema_to_fields(&resolved_schema.original_schema, config.map_handling)
        .context("failed to convert Avro schema to RisingWave schema")?;
    Ok((resolved_schema, rw_schema))
}

/// Refer to [crate level documentation](crate) for the ideas.
///
/// ## Arguments
/// - `avro_schema`: Avro schema in JSON format.
/// - `avro_data`: list of Avro data. Refer to [`TestDataEncoding`] for the format.
#[track_caller]
fn check(
    avro_schema: &str,
    avro_data: &[&str],
    config: Config,
    expected_risingwave_schema: expect_test::Expect,
    expected_risingwave_data: expect_test::Expect,
) {
    let (resolved_schema, rw_schema) =
        match avro_schema_str_to_risingwave_schema(avro_schema, &config) {
            Ok(res) => res,
            Err(e) => {
                expected_risingwave_schema.assert_eq(&format!("{}", e.as_report()));
                expected_risingwave_data.assert_eq("");
                return;
            }
        };
    expected_risingwave_schema.assert_eq(&format!(
        "{:#?}",
        rw_schema.iter().map(FieldTestDisplay).collect_vec()
    ));

    // manually implement some logic in AvroAccessBuilder, and some in PlainParser::parse_inner
    let mut data_str = vec![];
    for data in avro_data {
        let parser = AvroParseOptions::create(&resolved_schema.original_schema);

        match config.data_encoding {
            TestDataEncoding::Json => todo!(),
            TestDataEncoding::HexBinary => {
                let data = hex::decode(data).expect("failed to decode hex string");
                let avro_data =
                    from_avro_datum(&resolved_schema.original_schema, &mut data.as_slice(), None)
                        .expect("failed to parse Avro data");
                let access = AvroAccess::new(&avro_data, parser);

                let mut row = vec![];
                for col in &rw_schema {
                    let rw_data = access
                        .access(&[&col.name], &col.data_type)
                        .expect("failed to access");
                    row.push(format!("{:#?}", DatumCowTestDisplay(&rw_data)));
                }
                data_str.push(format!("{}", row.iter().format("\n")));
            }
        }
    }
    expected_risingwave_data.assert_eq(&format!("{}", data_str.iter().format("\n----\n")));
}

// This corresponds to legacy `e2e_test/source_legacy/basic/scripts/test_data/avro_simple_schema_bin.1`. TODO: remove that file.
#[test]
fn test_simple() {
    check(
        r#"
{
  "name": "test_student",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int",
      "default": 0
    },
    {
      "name": "sequence_id",
      "type": "long",
      "default": 0
    },
    {
      "name": "name",
      "type": ["null", "string"]
    },
    {
      "name": "score",
      "type": "float",
      "default": 0.0
    },
    {
      "name": "avg_score",
      "type": "double",
      "default": 0.0
    },
    {
      "name": "is_lasted",
      "type": "boolean",
      "default": false
    },
    {
      "name": "entrance_date",
      "type": "int",
      "logicalType": "date",
      "default": 0
    },
    {
      "name": "birthday",
      "type": "long",
      "logicalType": "timestamp-millis",
      "default": 0
    },
    {
      "name": "anniversary",
      "type": "long",
      "logicalType": "timestamp-micros",
      "default": 0
    },
    {
      "name": "passed",
      "type": {
        "name": "interval",
        "type": "fixed",
        "size": 12
      },
      "logicalType": "duration"
    },
    {
      "name": "bytes",
      "type": "bytes",
      "default": ""
    }
  ]
}
        "#,
        &[
            // {"id":32,"sequence_id":64,"name":{"string":"str_value"},"score":32.0,"avg_score":64.0,"is_lasted":true,"entrance_date":0,"birthday":0,"anniversary":0,"passed":"\u0001\u0000\u0000\u0000\u0001\u0000\u0000\u0000\u00E8\u0003\u0000\u0000","bytes":"\u0001\u0002\u0003\u0004\u0005"}
            "40800102127374725f76616c7565000000420000000000005040010000000100000001000000e80300000a0102030405",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                id: Int32,
                sequence_id: Int64,
                name: Varchar,
                score: Float32,
                avg_score: Float64,
                is_lasted: Boolean,
                entrance_date: Date,
                birthday: Timestamptz,
                anniversary: Timestamptz,
                passed: Interval,
                bytes: Bytea,
            ]"#]],
        expect![[r#"
            Owned(Int32(32))
            Owned(Int64(64))
            Borrowed(Utf8("str_value"))
            Owned(Float32(OrderedFloat(32.0)))
            Owned(Float64(OrderedFloat(64.0)))
            Owned(Bool(true))
            Owned(Date(Date(1970-01-01)))
            Owned(Timestamptz(Timestamptz(0)))
            Owned(Timestamptz(Timestamptz(0)))
            Owned(Interval(Interval { months: 1, days: 1, usecs: 1000000 }))
            Borrowed(Bytea([1, 2, 3, 4, 5]))"#]],
    )
}

#[test]
fn test_nullable_union() {
    check(
        r#"
{
  "name": "test_student",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int",
      "default": 0
    },
    {
      "name": "age",
      "type": ["null", "int"]
    },
    {
      "name": "sequence_id",
      "type": ["null", "long"]
    },
    {
      "name": "name",
      "type": ["null", "string"],
      "default": null
    },
    {
      "name": "score",
      "type": [ "float", "null" ],
      "default": 1.0
    },
    {
      "name": "avg_score",
      "type": ["null", "double"]
    },
    {
      "name": "is_lasted",
      "type": ["null", "boolean"]
    },
    {
      "name": "entrance_date",
      "type": [
        "null",
        {
          "type": "int",
          "logicalType": "date",
          "arg.properties": {
            "range": {
              "min": 1,
              "max": 19374
            }
          }
        }
      ],
      "default": null
    },
    {
      "name": "birthday",
      "type": [
        "null",
          {
            "type": "long",
            "logicalType": "timestamp-millis",
            "arg.properties": {
              "range": {
                "min": 1,
                "max": 1673970376213
              }
            }
          }
      ],
      "default": null
    },
    {
      "name": "anniversary",
      "type": [
        "null",
        {
          "type" : "long",
          "logicalType": "timestamp-micros",
          "arg.properties": {
            "range": {
              "min": 1,
              "max": 1673970376213000
            }
          }
        }
      ],
      "default": null
    }
  ]
}
        "#,
        &[
            // {
            //   "id": 5,
            //   "age": null,
            //   "sequence_id": null,
            //   "name": null,
            //   "score": null,
            //   "avg_score": null,
            //   "is_lasted": null,
            //   "entrance_date": null,
            //   "birthday": null,
            //   "anniversary": null
            // }
            "0a000000020000000000",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                id: Int32,
                age: Int32,
                sequence_id: Int64,
                name: Varchar,
                score: Float32,
                avg_score: Float64,
                is_lasted: Boolean,
                entrance_date: Date,
                birthday: Timestamptz,
                anniversary: Timestamptz,
            ]"#]],
        expect![[r#"
            Owned(Int32(5))
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(null)"#]],
    )
}

/// From `e2e_test/source_inline/kafka/avro/upsert_avro_json`
#[test]
fn test_1() {
    check(
        r#"
{
  "type": "record",
  "name": "OBJ_ATTRIBUTE_VALUE",
  "namespace": "CPLM",
  "fields": [
    {
      "name": "op_type",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "CLASS_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ITEM_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ATTR_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ATTR_VALUE",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "ORG_ID",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "UNIT_INFO",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "UPD_TIME",
      "type": [
        "null",
        "string"
      ],
      "default": null
    },
    {
      "name": "DEC_VAL",
      "type": [
        {
          "type": "bytes",
          "logicalType": "decimal",
          "precision": 10,
          "scale": 2
        },
        "null"
      ],
      "default": "\u00ff"
    },
    {
      "name": "REFERRED",
      "type": [
        "null",
        {
          "type": "record",
          "name": "REFERRED_TYPE",
          "fields": [
            {
              "name": "a",
              "type": "string"
            }
          ]
        }
      ],
      "default": null
    },
    {
      "name": "REF",
      "type": [
        "null",
        "REFERRED_TYPE"
      ],
      "default": null
    },
    {
      "name": "uuid",
      "type": [
        "null",
        {
          "type": "string",
          "logicalType": "uuid"
        }
      ],
      "default": null
    },
    {
      "name": "rate",
      "type": "double",
      "default": "NaN"
    }
  ],
  "connect.name": "CPLM.OBJ_ATTRIBUTE_VALUE"
}
"#,
        &[
            // {"op_type": {"string": "update"}, "ID": {"string": "id1"}, "CLASS_ID": {"string": "1"}, "ITEM_ID": {"string": "6768"}, "ATTR_ID": {"string": "6970"}, "ATTR_VALUE": {"string": "value9"}, "ORG_ID": {"string": "7172"}, "UNIT_INFO": {"string": "info9"}, "UPD_TIME": {"string": "2021-05-18T07:59:58.714Z"}, "DEC_VAL": {"bytes": "\u0002\u0054\u000b\u00e3\u00ff"}}
            "020c7570646174650206696431020231020836373638020836393730020c76616c756539020837313732020a696e666f390230323032312d30352d31385430373a35393a35382e3731345a000a02540be3ff000000000000000000f87f",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                op_type: Varchar,
                ID: Varchar,
                CLASS_ID: Varchar,
                ITEM_ID: Varchar,
                ATTR_ID: Varchar,
                ATTR_VALUE: Varchar,
                ORG_ID: Varchar,
                UNIT_INFO: Varchar,
                UPD_TIME: Varchar,
                DEC_VAL: Decimal,
                REFERRED: Struct { a: Varchar },
                REF: Struct { a: Varchar },
                uuid: Varchar,
                rate: Float64,
            ]"#]],
        expect![[r#"
            Borrowed(Utf8("update"))
            Borrowed(Utf8("id1"))
            Borrowed(Utf8("1"))
            Borrowed(Utf8("6768"))
            Borrowed(Utf8("6970"))
            Borrowed(Utf8("value9"))
            Borrowed(Utf8("7172"))
            Borrowed(Utf8("info9"))
            Borrowed(Utf8("2021-05-18T07:59:58.714Z"))
            Owned(Decimal(Normalized(99999999.99)))
            Owned(null)
            Owned(null)
            Owned(null)
            Owned(Float64(OrderedFloat(NaN)))"#]],
    );
}

#[test]
fn test_union() {
    // A basic test
    check(
        r#"
{
  "type": "record",
  "name": "Root",
  "fields": [
    {
      "name": "unionType",
      "type": ["int", "string"]
    },
    {
      "name": "unionTypeComplex",
      "type": [
        "null",
        {"type": "record", "name": "Email","fields": [{"name":"inner","type":"string"}]},
        {"type": "record", "name": "Fax","fields": [{"name":"inner","type":"int"}]},
        {"type": "record", "name": "Sms","fields": [{"name":"inner","type":"int"}]}
      ]
    },
    {
      "name": "nullableString",
      "type": ["null", "string"]
    }
  ]
}
    "#,
        &[
            // {
            //   "unionType": {"int": 114514},
            //   "unionTypeComplex": {"Sms": {"inner":6}},
            //   "nullableString": null
            // }
            "00a4fd0d060c00",
            // {
            //   "unionType": {"int": 114514},
            //   "unionTypeComplex": {"Fax": {"inner":6}},
            //   "nullableString": null
            // }
            "00a4fd0d040c00",
            // {
            //   "unionType": {"string": "oops"},
            //   "unionTypeComplex": null,
            //   "nullableString": {"string": "hello"}
            // }
            "02086f6f707300020a68656c6c6f",
            // {
            //   "unionType": {"string": "oops"},
            //   "unionTypeComplex": {"Email": {"inner":"a@b.c"}},
            //   "nullableString": null
            // }
            "02086f6f7073020a6140622e6300",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        // FIXME: why the struct type doesn't have field_descs? https://github.com/risingwavelabs/risingwave/issues/17128
        expect![[r#"
            failed to convert Avro schema to RisingWave schema: failed to convert Avro union to struct: Feature is not yet implemented: Avro named type used in Union type: Record(RecordSchema { name: Name { name: "Email", namespace: None }, aliases: None, doc: None, fields: [RecordField { name: "inner", doc: None, aliases: None, default: None, schema: String, order: Ascending, position: 0, custom_attributes: {} }], lookup: {"inner": 0}, attributes: {} })
            Tracking issue: https://github.com/risingwavelabs/risingwave/issues/17632"#]],
        expect![""],
    );

    // logicalType is currently rejected
    // https://github.com/risingwavelabs/risingwave/issues/17616
    check(
        r#"
{
"type": "record",
"name": "Root",
"fields": [
  {
    "name": "unionLogical",
    "type": ["int", {"type":"int", "logicalType": "date"}]
  }
]
}
  "#,
        &[],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            failed to convert Avro schema to RisingWave schema: failed to convert Avro union to struct: Feature is not yet implemented: Avro logicalType used in Union type: Date
            Tracking issue: https://github.com/risingwavelabs/risingwave/issues/17616"#]],
        expect![""],
    );

    // test named type. Consider namespace.
    // https://avro.apache.org/docs/1.11.1/specification/_print/#names
    // List of things to take care:
    // - Record fields and enum symbols DO NOT have namespace.
    // - If the name specified contains a dot, then it is assumed to be a fullname, and any namespace also specified is IGNORED.
    // - If a name doesn't have its own namespace, it will look for its most tightly enclosing named schema.
    check(
        r#"
{
    "type": "record",
    "name": "Root",
    "namespace": "RootNamespace",
    "fields": [
        {
            "name": "littleFieldToMakeNestingLooksBetter",
            "type": ["null","int"], "default": null
        },
        {
            "name": "recordField",
            "type": ["null", "int", {
                "type": "record",
                "name": "my.name.spaced.record",
                "namespace": "when.name.contains.dot.namespace.is.ignored",
                "fields": [
                    {"name": "hello", "type": {"type": "int", "default": 1}},
                    {"name": "world", "type": {"type": "double", "default": 1}}
                ]
            }],
            "default": null
        },
        {
            "name": "enumField",
            "type": ["null", "int", {
                "type": "enum",
                "name": "myEnum",
                "namespace": "my.namespace",
                "symbols": ["A", "B", "C", "D"]
            }],
            "default": null
        },
        {
            "name": "anotherEnumFieldUsingRootNamespace",
            "type": ["null", "int", {
                "type": "enum",
                "name": "myEnum",
                "symbols": ["A", "B", "C", "D"]
            }],
            "default": null
        }
    ]
}
"#,
        &[
            // {
            //   "enumField":{"my.namespace.myEnum":"A"},
            //   "anotherEnumFieldUsingRootNamespace":{"RootNamespace.myEnum": "D"}
            // }
            "000004000406",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            failed to convert Avro schema to RisingWave schema: failed to convert Avro union to struct: Feature is not yet implemented: Avro named type used in Union type: Record(RecordSchema { name: Name { name: "record", namespace: Some("my.name.spaced") }, aliases: None, doc: None, fields: [RecordField { name: "hello", doc: None, aliases: None, default: None, schema: Int, order: Ascending, position: 0, custom_attributes: {} }, RecordField { name: "world", doc: None, aliases: None, default: None, schema: Double, order: Ascending, position: 1, custom_attributes: {} }], lookup: {"hello": 0, "world": 1}, attributes: {} })
            Tracking issue: https://github.com/risingwavelabs/risingwave/issues/17632"#]],
        expect![""],
    );

    // This is provided by a user https://github.com/risingwavelabs/risingwave/issues/16273#issuecomment-2051480710
    check(
        r#"
{
  "namespace": "com.abc.efg.mqtt",
  "name": "also.DataMessage",
  "type": "record",
  "fields": [
      {
          "name": "metrics",
          "type": {
              "type": "array",
              "items": {
                  "name": "also_data_metric",
                  "type": "record",
                  "fields": [
                      {
                          "name": "id",
                          "type": "string"
                      },
                      {
                          "name": "name",
                          "type": "string"
                      },
                      {
                          "name": "norm_name",
                          "type": [
                              "null",
                              "string"
                          ],
                          "default": null
                      },
                      {
                          "name": "uom",
                          "type": [
                              "null",
                              "string"
                          ],
                          "default": null
                      },
                      {
                          "name": "data",
                          "type": {
                              "type": "array",
                              "items": {
                                  "name": "dataItem",
                                  "type": "record",
                                  "fields": [
                                      {
                                          "name": "ts",
                                          "type": "string",
                                          "doc": "Timestamp of the metric."
                                      },
                                      {
                                          "name": "value",
                                          "type": [
                                              "null",
                                              "boolean",
                                              "double",
                                              "string"
                                          ],
                                          "doc": "Value of the metric."
                                      }
                                  ]
                              }
                          },
                          "doc": "The data message"
                      }
                  ],
                  "doc": "A metric object"
              }
          },
          "doc": "A list of metrics."
      }
  ]
}
          "#,
        &[
            // {
            //   "metrics": [
            //     {"id":"foo", "name":"a", "data": [] }
            //   ]
            // }
            "0206666f6f026100000000",
            // {
            //   "metrics": [
            //     {"id":"foo", "name":"a", "norm_name": null, "uom": {"string":"c"}, "data": [{"ts":"1", "value":null}, {"ts":"2", "value": {"boolean": true }}] }
            //   ]
            // }
            "0206666f6f02610002026304023100023202010000",
        ],
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                metrics: List(
                    Struct {
                        id: Varchar,
                        name: Varchar,
                        norm_name: Varchar,
                        uom: Varchar,
                        data: List(
                            Struct {
                                ts: Varchar,
                                value: Struct {
                                    boolean: Boolean,
                                    double: Float64,
                                    string: Varchar,
                                },
                            },
                        ),
                    },
                ),
            ]"#]],
        expect![[r#"
            Owned([
                StructValue(
                    Utf8("foo"),
                    Utf8("a"),
                    null,
                    null,
                    [],
                ),
            ])
            ----
            Owned([
                StructValue(
                    Utf8("foo"),
                    Utf8("a"),
                    null,
                    Utf8("c"),
                    [
                        StructValue(
                            Utf8("1"),
                            null,
                        ),
                        StructValue(
                            Utf8("2"),
                            StructValue(
                                Bool(true),
                                null,
                                null,
                            ),
                        ),
                    ],
                ),
            ])"#]],
    );
}

#[test]
fn test_map() {
    let schema = r#"
{
    "type": "record",
    "namespace": "com.redpanda.examples.avro",
    "name": "ClickEvent",
    "fields": [
        {
            "name": "map_str",
            "type": {
                "type": "map",
                "values": "string"
            },
            "default": {}
        },
        {
            "name": "map_map_int",
            "type": {
                "type": "map",
                "values": {
                    "type": "map",
                    "values": "int"
                }
            }
        }
    ]
}
    "#;

    let data = &[
        // {"map_str": {"a":"1","b":"2"}, "map_map_int": {"m1": {"a":1,"b":2}, "m2": {"c":3,"d":4}}}
        "0402610278026202790004046d310402610202620400046d32040263060264080000",
        // {"map_map_int": {}}
        "0000",
    ];

    check(
        schema,
        data,
        Config {
            map_handling: None,
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                map_str: Map(Varchar,Varchar),
                map_map_int: Map(Varchar,Map(Varchar,Int32)),
            ]"#]],
        expect![[r#"
            Owned([
                StructValue(
                    Utf8("a"),
                    Utf8("x"),
                ),
                StructValue(
                    Utf8("b"),
                    Utf8("y"),
                ),
            ])
            Owned([
                StructValue(
                    Utf8("m1"),
                    [
                        StructValue(
                            Utf8("a"),
                            Int32(1),
                        ),
                        StructValue(
                            Utf8("b"),
                            Int32(2),
                        ),
                    ],
                ),
                StructValue(
                    Utf8("m2"),
                    [
                        StructValue(
                            Utf8("c"),
                            Int32(3),
                        ),
                        StructValue(
                            Utf8("d"),
                            Int32(4),
                        ),
                    ],
                ),
            ])
            ----
            Owned([])
            Owned([])"#]],
    );

    check(
        schema,
        data,
        Config {
            map_handling: Some(MapHandling::Jsonb),
            data_encoding: TestDataEncoding::HexBinary,
        },
        expect![[r#"
            [
                map_str: Jsonb,
                map_map_int: Jsonb,
            ]"#]],
        expect![[r#"
            Owned(Jsonb({"a": "x", "b": "y"}))
            Owned(Jsonb({"m1": {"a": 1, "b": 2}, "m2": {"c": 3, "d": 4}}))
            ----
            Owned(Jsonb({}))
            Owned(Jsonb({}))"#]],
    );
}
