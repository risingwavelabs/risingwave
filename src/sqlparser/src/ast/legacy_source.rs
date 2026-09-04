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
use winnow::ModalResult;

use crate::ast::{
    AstString, Encode, Format, FormatEncodeOptions, Ident, ObjectName, ParseTo, SqlOption, Value,
    display_separated,
};
use crate::keywords::Keyword;
use crate::parser::{Parser, StrError};
use crate::{impl_fmt_display, impl_parse_to, parser_err};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let value = id.real_value();
        let schema = match &value[..] {
            "avro" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                LegacyRowFormat::Avro(avro_schema)
            }
            "upsert_avro" => {
                impl_parse_to!(avro_schema: AvroSchema, p);
                LegacyRowFormat::UpsertAvro(avro_schema)
            }
            "native" => LegacyRowFormat::Native, // used internally by schema change
            "bytes" => LegacyRowFormat::Bytes,
            _ => {
                parser_err!(
                    "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO \
                    | AVRO | UPSERT_AVRO | BYTES | NATIVE after ROW FORMAT"
                );
            }
        };
        Ok(CompatibleFormatEncode::RowFormat(schema))
    } else {
        p.expected("description of the format")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LegacyRowFormat {
    Avro(AvroSchema),       // Keyword::AVRO
    UpsertAvro(AvroSchema), // Keyword::UpsertAVRO
    Native,
    Bytes,
}

impl LegacyRowFormat {
    pub fn into_format_encode_v2(self) -> FormatEncodeOptions {
        let (format, row_encode) = match self {
            LegacyRowFormat::Avro(_) => (Format::Plain, Encode::Avro),
            LegacyRowFormat::UpsertAvro(_) => (Format::Upsert, Encode::Avro),
            LegacyRowFormat::Bytes => (Format::Plain, Encode::Bytes),
            LegacyRowFormat::Native => (Format::Native, Encode::Native),
        };

        let row_options = match self {
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
            LegacyRowFormat::Avro(avro_schema) => write!(f, "AVRO {}", avro_schema),
            LegacyRowFormat::UpsertAvro(avro_schema) => write!(f, "UPSERT_AVRO {}", avro_schema),
            LegacyRowFormat::Native => write!(f, "NATIVE"),
            LegacyRowFormat::Bytes => write!(f, "BYTES"),
        }
    }
}

// sql_grammar!(AvroSchema {
//     [Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION, [Keyword::CONFLUENT, Keyword::SCHEMA,
// Keyword::REGISTRY]],     row_schema_location: AstString,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
