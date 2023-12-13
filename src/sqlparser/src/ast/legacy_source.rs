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

//! Content of this file can be deleted once we stop supporting `create source` syntax v1.
//! New features shall NOT touch this file.

use std::fmt;

use itertools::Itertools as _;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::ast::{
    AstString, AstVec, ConnectorSchema, Encode, Format, Ident, ObjectName, ParseTo, SqlOption,
    Value,
};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::{impl_fmt_display, impl_parse_to};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CompatibleSourceSchema {
    RowFormat(SourceSchema),
    V2(ConnectorSchema),
}

impl fmt::Display for CompatibleSourceSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => {
                write!(f, "{}", inner)
            }
            CompatibleSourceSchema::V2(inner) => {
                write!(f, "{}", inner)
            }
        }
    }
}

impl CompatibleSourceSchema {
    pub(crate) fn into_v2(self) -> ConnectorSchema {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => inner.into_source_schema_v2(),
            CompatibleSourceSchema::V2(inner) => inner,
        }
    }
}

impl From<ConnectorSchema> for CompatibleSourceSchema {
    fn from(value: ConnectorSchema) -> Self {
        Self::V2(value)
    }
}

pub fn parse_source_schema(p: &mut Parser) -> Result<CompatibleSourceSchema, ParserError> {
    if let Some(schema_v2) = p.parse_schema()? {
        Ok(CompatibleSourceSchema::V2(schema_v2))
    } else if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
        && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
    {
        p.expect_keyword(Keyword::ROW)?;
        p.expect_keyword(Keyword::FORMAT)?;
        let id = p.parse_identifier()?;
        let value = id.value.to_ascii_uppercase();
        let schema = match &value[..] {
            "JSON" => SourceSchema::Json,
            "UPSERT_JSON" => SourceSchema::UpsertJson,
            "PROTOBUF" => {
                impl_parse_to!(protobuf_schema: ProtobufSchema, p);
                SourceSchema::Protobuf(protobuf_schema)
            }
            "DEBEZIUM_JSON" => SourceSchema::DebeziumJson,
            "DEBEZIUM_MONGO_JSON" => SourceSchema::DebeziumMongoJson,
            "AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                SourceSchema::Avro(avro_schema)
            }
            "UPSERT_AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                SourceSchema::UpsertAvro(avro_schema)
            }
            "MAXWELL" => SourceSchema::Maxwell,
            "CANAL_JSON" => SourceSchema::CanalJson,
            "CSV" => {
                impl_parse_to!(csv_info: CsvInfo, p);
                SourceSchema::Csv(csv_info)
            }
            "NATIVE" => SourceSchema::Native, // used internally by schema change
            "DEBEZIUM_AVRO" => {
                impl_parse_to!(avro_schema: DebeziumAvroSchema, p);
                SourceSchema::DebeziumAvro(avro_schema)
            }
            "BYTES" => SourceSchema::Bytes,
            _ => {
                return Err(ParserError::ParserError(
                    "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO \
                    | AVRO | UPSERT_AVRO | MAXWELL | CANAL_JSON | BYTES | NATIVE after ROW FORMAT"
                        .to_string(),
                ))
            }
        };
        Ok(CompatibleSourceSchema::RowFormat(schema))
    } else {
        p.expected("description of the format", p.peek_token())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SourceSchema {
    Protobuf(ProtobufSchema),
    // Keyword::PROTOBUF ProtobufSchema
    Json,         // Keyword::JSON
    DebeziumJson, // Keyword::DEBEZIUM_JSON
    DebeziumMongoJson,
    UpsertJson,             // Keyword::UPSERT_JSON
    Avro(AvroSchema),       // Keyword::AVRO
    UpsertAvro(AvroSchema), // Keyword::UpsertAVRO
    Maxwell,                // Keyword::MAXWELL
    CanalJson,              // Keyword::CANAL_JSON
    Csv(CsvInfo),           // Keyword::CSV
    Native,
    DebeziumAvro(DebeziumAvroSchema), // Keyword::DEBEZIUM_AVRO
    Bytes,
}

impl SourceSchema {
    pub fn into_source_schema_v2(self) -> ConnectorSchema {
        let (format, row_encode) = match self {
            SourceSchema::Protobuf(_) => (Format::Plain, Encode::Protobuf),
            SourceSchema::Json => (Format::Plain, Encode::Json),
            SourceSchema::DebeziumJson => (Format::Debezium, Encode::Json),
            SourceSchema::DebeziumMongoJson => (Format::DebeziumMongo, Encode::Json),
            SourceSchema::UpsertJson => (Format::Upsert, Encode::Json),
            SourceSchema::Avro(_) => (Format::Plain, Encode::Avro),
            SourceSchema::UpsertAvro(_) => (Format::Upsert, Encode::Avro),
            SourceSchema::Maxwell => (Format::Maxwell, Encode::Json),
            SourceSchema::CanalJson => (Format::Canal, Encode::Json),
            SourceSchema::Csv(_) => (Format::Plain, Encode::Csv),
            SourceSchema::DebeziumAvro(_) => (Format::Debezium, Encode::Avro),
            SourceSchema::Bytes => (Format::Plain, Encode::Bytes),
            SourceSchema::Native => (Format::Native, Encode::Native),
        };

        let row_options = match self {
            SourceSchema::Protobuf(schema) => {
                let mut options = vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "message".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.message_name.0),
                }];
                if schema.use_schema_registry {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    });
                } else {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    })
                }
                options
            }
            SourceSchema::Avro(schema) | SourceSchema::UpsertAvro(schema) => {
                if schema.use_schema_registry {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    }]
                } else {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0),
                    }]
                }
            }
            SourceSchema::DebeziumAvro(schema) => {
                vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "schema.registry".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.row_schema_location.0),
                }]
            }
            SourceSchema::Csv(schema) => {
                vec![
                    SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "delimiter".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(
                            String::from_utf8_lossy(&[schema.delimiter]).into(),
                        ),
                    },
                    SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "without_header".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(if schema.has_header {
                            "false".into()
                        } else {
                            "true".into()
                        }),
                    },
                ]
            }
            _ => vec![],
        };

        ConnectorSchema {
            format,
            row_encode,
            row_options,
        }
    }
}

impl fmt::Display for SourceSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROW FORMAT ")?;
        match self {
            SourceSchema::Protobuf(protobuf_schema) => write!(f, "PROTOBUF {}", protobuf_schema),
            SourceSchema::Json => write!(f, "JSON"),
            SourceSchema::UpsertJson => write!(f, "UPSERT_JSON"),
            SourceSchema::Maxwell => write!(f, "MAXWELL"),
            SourceSchema::DebeziumJson => write!(f, "DEBEZIUM_JSON"),
            SourceSchema::DebeziumMongoJson => write!(f, "DEBEZIUM_MONGO_JSON"),
            SourceSchema::Avro(avro_schema) => write!(f, "AVRO {}", avro_schema),
            SourceSchema::UpsertAvro(avro_schema) => write!(f, "UPSERT_AVRO {}", avro_schema),
            SourceSchema::CanalJson => write!(f, "CANAL_JSON"),
            SourceSchema::Csv(csv_info) => write!(f, "CSV {}", csv_info),
            SourceSchema::Native => write!(f, "NATIVE"),
            SourceSchema::DebeziumAvro(avro_schema) => write!(f, "DEBEZIUM_AVRO {}", avro_schema),
            SourceSchema::Bytes => write!(f, "BYTES"),
        }
    }
}

// sql_grammar!(ProtobufSchema {
//     [Keyword::MESSAGE],
//     message_name: AstString,
//     [Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION],
//     row_schema_location: AstString,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ProtobufSchema {
    pub message_name: AstString,
    pub row_schema_location: AstString,
    pub use_schema_registry: bool,
}

impl ParseTo for ProtobufSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::MESSAGE], p);
        impl_parse_to!(message_name: AstString, p);
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], p);
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            message_name,
            row_schema_location,
            use_schema_registry,
        })
    }
}

impl fmt::Display for ProtobufSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!([Keyword::MESSAGE], v);
        impl_fmt_display!(message_name, v, self);
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], v, self);
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(AvroSchema {
//     [Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION, [Keyword::CONFLUENT, Keyword::SCHEMA,
// Keyword::REGISTRY]],     row_schema_location: AstString,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AvroSchema {
    pub row_schema_location: AstString,
    pub use_schema_registry: bool,
}
impl ParseTo for AvroSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], p);
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            row_schema_location,
            use_schema_registry,
        })
    }
}

impl fmt::Display for AvroSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(use_schema_registry => [Keyword::CONFLUENT, Keyword::SCHEMA, Keyword::REGISTRY], v, self);
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DebeziumAvroSchema {
    pub row_schema_location: AstString,
}

impl fmt::Display for DebeziumAvroSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(
            [
                Keyword::ROW,
                Keyword::SCHEMA,
                Keyword::LOCATION,
                Keyword::CONFLUENT,
                Keyword::SCHEMA,
                Keyword::REGISTRY
            ],
            v
        );
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for DebeziumAvroSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(
            [
                Keyword::ROW,
                Keyword::SCHEMA,
                Keyword::LOCATION,
                Keyword::CONFLUENT,
                Keyword::SCHEMA,
                Keyword::REGISTRY
            ],
            p
        );
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            row_schema_location,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CsvInfo {
    pub delimiter: u8,
    pub has_header: bool,
}

pub fn get_delimiter(chars: &str) -> Result<u8, ParserError> {
    match chars {
        "," => Ok(b','),   // comma
        "\t" => Ok(b'\t'), // tab
        other => Err(ParserError::ParserError(format!(
            "The delimiter should be one of ',', E'\\t', but got {:?}",
            other
        ))),
    }
}

impl ParseTo for CsvInfo {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(without_header => [Keyword::WITHOUT, Keyword::HEADER], p);
        impl_parse_to!([Keyword::DELIMITED, Keyword::BY], p);
        impl_parse_to!(delimiter: AstString, p);
        let delimiter = get_delimiter(delimiter.0.as_str())?;
        Ok(Self {
            delimiter,
            has_header: !without_header,
        })
    }
}

impl fmt::Display for CsvInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        if !self.has_header {
            v.push(format!(
                "{}",
                AstVec([Keyword::WITHOUT, Keyword::HEADER].to_vec())
            ));
        }
        impl_fmt_display!(delimiter, v, self);
        v.iter().join(" ").fmt(f)
    }
}
