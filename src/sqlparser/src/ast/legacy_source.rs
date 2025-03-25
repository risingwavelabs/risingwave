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

//! Content of this file can be deleted once we stop supporting `create source` syntax v1.
//! New features shall NOT touch this file.

use std::fmt;

use itertools::Itertools as _;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use winnow::ModalResult;

use crate::ast::{
    AstString, Encode, Format, FormatEncodeOptions, Ident, ObjectName, ParseTo, SqlOption, Value,
    display_separated,
};
use crate::keywords::Keyword;
use crate::parser::{Parser, StrError};
use crate::{impl_fmt_display, impl_parse_to, parser_err};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CompatibleFormatEncode {
    RowFormat(LegacyRowFormat),
    V2(FormatEncodeOptions),
}

impl fmt::Display for CompatibleFormatEncode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompatibleFormatEncode::RowFormat(inner) => {
                write!(f, "{}", inner)
            }
            CompatibleFormatEncode::V2(inner) => {
                write!(f, "{}", inner)
            }
        }
    }
}

impl CompatibleFormatEncode {
    pub(crate) fn into_v2(self) -> FormatEncodeOptions {
        match self {
            CompatibleFormatEncode::RowFormat(inner) => inner.into_format_encode_v2(),
            CompatibleFormatEncode::V2(inner) => inner,
        }
    }
}

impl From<FormatEncodeOptions> for CompatibleFormatEncode {
    fn from(value: FormatEncodeOptions) -> Self {
        Self::V2(value)
    }
}

pub fn parse_format_encode(p: &mut Parser<'_>) -> ModalResult<CompatibleFormatEncode> {
    if let Some(schema_v2) = p.parse_schema()? {
        if schema_v2.key_encode.is_some() {
            parser_err!("key encode clause is not supported in source schema");
        }
        Ok(CompatibleFormatEncode::V2(schema_v2))
    } else if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
        && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
    {
        p.expect_keyword(Keyword::ROW)?;
        p.expect_keyword(Keyword::FORMAT)?;
        let id = p.parse_identifier()?;
        let value = id.value.to_ascii_uppercase();
        let schema = match &value[..] {
            "JSON" => LegacyRowFormat::Json,
            "UPSERT_JSON" => LegacyRowFormat::UpsertJson,
            "PROTOBUF" => {
                impl_parse_to!(protobuf_schema: ProtobufSchema, p);
                LegacyRowFormat::Protobuf(protobuf_schema)
            }
            "DEBEZIUM_JSON" => LegacyRowFormat::DebeziumJson,
            "DEBEZIUM_MONGO_JSON" => LegacyRowFormat::DebeziumMongoJson,
            "AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                LegacyRowFormat::Avro(avro_schema)
            }
            "UPSERT_AVRO" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                LegacyRowFormat::UpsertAvro(avro_schema)
            }
            "MAXWELL" => LegacyRowFormat::Maxwell,
            "CANAL_JSON" => LegacyRowFormat::CanalJson,
            "CSV" => {
                impl_parse_to!(csv_info: CsvInfo, p);
                LegacyRowFormat::Csv(csv_info)
            }
            "NATIVE" => LegacyRowFormat::Native, // used internally by schema change
            "DEBEZIUM_AVRO" => {
                impl_parse_to!(avro_schema: DebeziumAvroSchema, p);
                LegacyRowFormat::DebeziumAvro(avro_schema)
            }
            "BYTES" => LegacyRowFormat::Bytes,
            _ => {
                parser_err!(
                    "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO \
                    | AVRO | UPSERT_AVRO | MAXWELL | CANAL_JSON | BYTES | NATIVE after ROW FORMAT"
                );
            }
        };
        Ok(CompatibleFormatEncode::RowFormat(schema))
    } else {
        p.expected("description of the format")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LegacyRowFormat {
    Protobuf(ProtobufSchema), // Keyword::PROTOBUF ProtobufSchema
    Json,                     // Keyword::JSON
    DebeziumJson,             // Keyword::DEBEZIUM_JSON
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

impl LegacyRowFormat {
    pub fn into_format_encode_v2(self) -> FormatEncodeOptions {
        let (format, row_encode) = match self {
            LegacyRowFormat::Protobuf(_) => (Format::Plain, Encode::Protobuf),
            LegacyRowFormat::Json => (Format::Plain, Encode::Json),
            LegacyRowFormat::DebeziumJson => (Format::Debezium, Encode::Json),
            LegacyRowFormat::DebeziumMongoJson => (Format::DebeziumMongo, Encode::Json),
            LegacyRowFormat::UpsertJson => (Format::Upsert, Encode::Json),
            LegacyRowFormat::Avro(_) => (Format::Plain, Encode::Avro),
            LegacyRowFormat::UpsertAvro(_) => (Format::Upsert, Encode::Avro),
            LegacyRowFormat::Maxwell => (Format::Maxwell, Encode::Json),
            LegacyRowFormat::CanalJson => (Format::Canal, Encode::Json),
            LegacyRowFormat::Csv(_) => (Format::Plain, Encode::Csv),
            LegacyRowFormat::DebeziumAvro(_) => (Format::Debezium, Encode::Avro),
            LegacyRowFormat::Bytes => (Format::Plain, Encode::Bytes),
            LegacyRowFormat::Native => (Format::Native, Encode::Native),
        };

        let row_options = match self {
            LegacyRowFormat::Protobuf(schema) => {
                let mut options = vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "message".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.message_name.0).into(),
                }];
                if schema.use_schema_registry {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0).into(),
                    });
                } else {
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0).into(),
                    })
                }
                options
            }
            LegacyRowFormat::Avro(schema) | LegacyRowFormat::UpsertAvro(schema) => {
                if schema.use_schema_registry {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.registry".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0).into(),
                    }]
                } else {
                    vec![SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "schema.location".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(schema.row_schema_location.0).into(),
                    }]
                }
            }
            LegacyRowFormat::DebeziumAvro(schema) => {
                vec![SqlOption {
                    name: ObjectName(vec![Ident {
                        value: "schema.registry".into(),
                        quote_style: None,
                    }]),
                    value: Value::SingleQuotedString(schema.row_schema_location.0).into(),
                }]
            }
            LegacyRowFormat::Csv(schema) => {
                vec![
                    SqlOption {
                        name: ObjectName(vec![Ident {
                            value: "delimiter".into(),
                            quote_style: None,
                        }]),
                        value: Value::SingleQuotedString(
                            String::from_utf8_lossy(&[schema.delimiter]).into(),
                        )
                        .into(),
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
                        })
                        .into(),
                    },
                ]
            }
            _ => vec![],
        };

        FormatEncodeOptions {
            format,
            row_encode,
            row_options,
            key_encode: None,
        }
    }
}

impl fmt::Display for LegacyRowFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ROW FORMAT ")?;
        match self {
            LegacyRowFormat::Protobuf(protobuf_schema) => {
                write!(f, "PROTOBUF {}", protobuf_schema)
            }
            LegacyRowFormat::Json => write!(f, "JSON"),
            LegacyRowFormat::UpsertJson => write!(f, "UPSERT_JSON"),
            LegacyRowFormat::Maxwell => write!(f, "MAXWELL"),
            LegacyRowFormat::DebeziumJson => write!(f, "DEBEZIUM_JSON"),
            LegacyRowFormat::DebeziumMongoJson => write!(f, "DEBEZIUM_MONGO_JSON"),
            LegacyRowFormat::Avro(avro_schema) => write!(f, "AVRO {}", avro_schema),
            LegacyRowFormat::UpsertAvro(avro_schema) => write!(f, "UPSERT_AVRO {}", avro_schema),
            LegacyRowFormat::CanalJson => write!(f, "CANAL_JSON"),
            LegacyRowFormat::Csv(csv_info) => write!(f, "CSV {}", csv_info),
            LegacyRowFormat::Native => write!(f, "NATIVE"),
            LegacyRowFormat::DebeziumAvro(avro_schema) => {
                write!(f, "DEBEZIUM_AVRO {}", avro_schema)
            }
            LegacyRowFormat::Bytes => write!(f, "BYTES"),
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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

pub fn get_delimiter(chars: &str) -> Result<u8, StrError> {
    match chars {
        "," => Ok(b','),   // comma
        ";" => Ok(b';'),   // semicolon
        "\t" => Ok(b'\t'), // tab
        other => Err(StrError(format!(
            "The delimiter should be one of ',', ';', E'\\t', but got {other:?}",
        ))),
    }
}

impl ParseTo for CsvInfo {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
        if !self.has_header {
            write!(f, "WITHOUT HEADER ")?;
        }
        write!(
            f,
            "DELIMITED BY {}",
            AstString((self.delimiter as char).to_string())
        )?;
        Ok(())
    }
}
