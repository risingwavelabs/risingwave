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

use core::fmt;
use std::fmt::Write;

use itertools::Itertools;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::ddl::SourceWatermark;
use super::{EmitMode, Ident, ObjectType, Query, Value};
use crate::ast::{
    display_comma_separated, display_separated, ColumnDef, ObjectName, SqlOption, TableConstraint,
};
use crate::keywords::Keyword;
use crate::parser::{IsOptional, Parser, ParserError, UPSTREAM_SOURCE_KEY};
use crate::tokenizer::Token;

/// Consumes token from the parser into an AST node.
pub trait ParseTo: Sized {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError>;
}

macro_rules! impl_parse_to {
    () => {};
    ($field:ident : $field_type:ty, $parser:ident) => {
        let $field = <$field_type>::parse_to($parser)?;
    };
    ($field:ident => [$($arr:tt)+], $parser:ident) => {
        let $field = $parser.parse_keywords(&[$($arr)+]);
    };
    ([$($arr:tt)+], $parser:ident) => {
        $parser.expect_keywords(&[$($arr)+])?;
    };
}

macro_rules! impl_fmt_display {
    () => {};
    ($field:ident, $v:ident, $self:ident) => {{
        let s = format!("{}", $self.$field);
        if !s.is_empty() {
            $v.push(s);
        }
    }};
    ($field:ident => [$($arr:tt)+], $v:ident, $self:ident) => {
        if $self.$field {
            $v.push(format!("{}", AstVec([$($arr)+].to_vec())));
        }
    };
    ([$($arr:tt)+], $v:ident) => {
        $v.push(format!("{}", AstVec([$($arr)+].to_vec())));
    };
}

// sql_grammar!(CreateSourceStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     source_name: Ident,
//     with_properties: AstOption<WithProperties>,
//     [Keyword::ROW, Keyword::FORMAT],
//     source_schema: SourceSchema,
//     [Keyword::WATERMARK, Keyword::FOR] column [Keyword::AS] <expr>
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSourceStatement {
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub source_name: ObjectName,
    pub with_properties: WithProperties,
    pub source_schema: CompatibleSourceSchema,
    pub source_watermarks: Vec<SourceWatermark>,
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Format {
    Native,
    Debezium,      // Keyword::DEBEZIUM
    DebeziumMongo, // Keyword::DEBEZIUM_MONGO
    Maxwell,       // Keyword::MAXWELL
    Canal,         // Keyword::CANAL
    Upsert,        // Keyword::UPSERT
    Plain,         // Keyword::PLAIN
}

// TODO: unify with `from_keyword`
impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Format::Native => "NATIVE",
                Format::Debezium => "DEBEZIUM",
                Format::DebeziumMongo => "DEBEZIUM_MONGO",
                Format::Maxwell => "MAXWELL",
                Format::Canal => "CANAL",
                Format::Upsert => "UPSERT",
                Format::Plain => "PLAIN",
            }
        )
    }
}

impl Format {
    pub fn from_keyword(s: &str) -> Result<Self, ParserError> {
        Ok(match s {
            "DEBEZIUM" => Format::Debezium,
            "DEBEZIUM_MONGO" => Format::DebeziumMongo,
            "MAXWELL" => Format::Maxwell,
            "CANAL" => Format::Canal,
            "PLAIN" => Format::Plain,
            "UPSERT" => Format::Upsert,
            "NATIVE" => Format::Native, // used internally for schema change
            _ => {
                return Err(ParserError::ParserError(
                    "expected CANAL | PROTOBUF | DEBEZIUM | MAXWELL | PLAIN | NATIVE after FORMAT"
                        .to_string(),
                ))
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Encode {
    Avro,     // Keyword::Avro
    Csv,      // Keyword::CSV
    Protobuf, // Keyword::PROTOBUF
    Json,     // Keyword::JSON
    Bytes,    // Keyword::BYTES
    Native,
    Template,
}

// TODO: unify with `from_keyword`
impl fmt::Display for Encode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Encode::Avro => "AVRO",
                Encode::Csv => "CSV",
                Encode::Protobuf => "PROTOBUF",
                Encode::Json => "JSON",
                Encode::Bytes => "BYTES",
                Encode::Native => "NATIVE",
                Encode::Template => "TEMPLATE",
            }
        )
    }
}

impl Encode {
    pub fn from_keyword(s: &str) -> Result<Self, ParserError> {
        Ok(match s {
            "AVRO" => Encode::Avro,
            "BYTES" => Encode::Bytes,
            "CSV" => Encode::Csv,
            "PROTOBUF" => Encode::Protobuf,
            "JSON" => Encode::Json,
            "TEMPLATE" => Encode::Template,
            "NATIVE" => Encode::Native, // used internally for schema change
            _ => return Err(ParserError::ParserError(
                "expected AVRO | BYTES | CSV | PROTOBUF | JSON | NATIVE | TEMPLATE after Encode"
                    .to_string(),
            )),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ConnectorSchema {
    pub format: Format,
    pub row_encode: Encode,
    pub row_options: Vec<SqlOption>,
}

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
    pub fn into_source_schema_v2(self) -> (ConnectorSchema, Option<String>) {
        match self {
            CompatibleSourceSchema::RowFormat(inner) => (
                inner.into_source_schema_v2(),
                Some("RisingWave will stop supporting the syntax \"ROW FORMAT\" in future versions, which will be changed to \"FORMAT ... ENCODE ...\" syntax.".to_string())),
            CompatibleSourceSchema::V2(inner) => (inner, None),
        }
    }
}

impl From<ConnectorSchema> for CompatibleSourceSchema {
    fn from(value: ConnectorSchema) -> Self {
        Self::V2(value)
    }
}

fn parse_source_schema(p: &mut Parser) -> Result<CompatibleSourceSchema, ParserError> {
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
        Err(ParserError::ParserError(
            "expect description of the format".to_string(),
        ))
    }
}

impl Parser {
    /// Peek the next tokens to see if it is `FORMAT` or `ROW FORMAT` (for compatibility).
    fn peek_source_schema_format(&mut self) -> bool {
        (self.peek_nth_any_of_keywords(0, &[Keyword::ROW])
            && self.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])) // ROW FORMAT
            || self.peek_nth_any_of_keywords(0, &[Keyword::FORMAT]) // FORMAT
    }

    /// Parse the source schema. The behavior depends on the `connector` type.
    pub fn parse_source_schema_with_connector(
        &mut self,
        connector: &str,
        cdc_source_job: bool,
    ) -> Result<CompatibleSourceSchema, ParserError> {
        // row format for cdc source must be debezium json
        // row format for nexmark source must be native
        // default row format for datagen source is native
        if connector.contains("-cdc") {
            let expected = if cdc_source_job {
                ConnectorSchema::plain_json()
            } else {
                ConnectorSchema::debezium_json()
            };
            if self.peek_source_schema_format() {
                let schema = parse_source_schema(self)?.into_source_schema_v2().0;
                if schema != expected {
                    return Err(ParserError::ParserError(format!(
                        "Row format for CDC connectors should be \
                         either omitted or set to `{expected}`",
                    )));
                }
            }
            Ok(expected.into())
        } else if connector.contains("nexmark") {
            let expected = ConnectorSchema::native();
            if self.peek_source_schema_format() {
                let schema = parse_source_schema(self)?.into_source_schema_v2().0;
                if schema != expected {
                    return Err(ParserError::ParserError(format!(
                        "Row format for nexmark connectors should be \
                         either omitted or set to `{expected}`",
                    )));
                }
            }
            Ok(expected.into())
        } else if connector.contains("datagen") {
            Ok(if self.peek_source_schema_format() {
                parse_source_schema(self)?
            } else {
                ConnectorSchema::native().into()
            })
        } else {
            Ok(parse_source_schema(self)?)
        }
    }

    /// Parse `FORMAT ... ENCODE ... (...)` in `CREATE SOURCE` and `CREATE SINK`.
    pub fn parse_schema(&mut self) -> Result<Option<ConnectorSchema>, ParserError> {
        if !self.parse_keyword(Keyword::FORMAT) {
            return Ok(None);
        }

        let id = self.parse_identifier()?;
        let s = id.value.to_ascii_uppercase();
        let format = Format::from_keyword(&s)?;
        self.expect_keyword(Keyword::ENCODE)?;
        let id = self.parse_identifier()?;
        let s = id.value.to_ascii_uppercase();
        let row_encode = Encode::from_keyword(&s)?;
        let row_options = self.parse_options()?;

        Ok(Some(ConnectorSchema {
            format,
            row_encode,
            row_options,
        }))
    }
}

impl ConnectorSchema {
    pub const fn plain_json() -> Self {
        ConnectorSchema {
            format: Format::Plain,
            row_encode: Encode::Json,
            row_options: Vec::new(),
        }
    }

    /// Create a new source schema with `Debezium` format and `Json` encoding.
    pub const fn debezium_json() -> Self {
        ConnectorSchema {
            format: Format::Debezium,
            row_encode: Encode::Json,
            row_options: Vec::new(),
        }
    }

    /// Create a new source schema with `Native` format and encoding.
    pub const fn native() -> Self {
        ConnectorSchema {
            format: Format::Native,
            row_encode: Encode::Native,
            row_options: Vec::new(),
        }
    }

    pub fn row_options(&self) -> &[SqlOption] {
        self.row_options.as_ref()
    }
}

impl fmt::Display for ConnectorSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FORMAT {} ENCODE {}", self.format, self.row_encode)?;

        if !self.row_options().is_empty() {
            write!(f, " ({})", display_comma_separated(self.row_options()))
        } else {
            Ok(())
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

impl ParseTo for CreateSourceStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(source_name: ObjectName, p);

        // parse columns
        let (columns, constraints, source_watermarks) = p.parse_columns_with_watermark()?;

        let with_options = p.parse_with_properties()?;
        let option = with_options
            .iter()
            .find(|&opt| opt.name.real_value() == UPSTREAM_SOURCE_KEY);
        let connector: String = option.map(|opt| opt.value.to_string()).unwrap_or_default();
        // The format of cdc source job is fixed to `FORMAT PLAIN ENCODE JSON`
        let cdc_source_job =
            connector.contains("-cdc") && columns.is_empty() && constraints.is_empty();
        // row format for nexmark source must be native
        // default row format for datagen source is native
        let source_schema = p.parse_source_schema_with_connector(&connector, cdc_source_job)?;

        Ok(Self {
            if_not_exists,
            columns,
            constraints,
            source_name,
            with_properties: WithProperties(with_options),
            source_schema,
            source_watermarks,
        })
    }
}

pub(super) fn fmt_create_items(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
    watermarks: &[SourceWatermark],
) -> std::result::Result<String, fmt::Error> {
    let mut items = String::new();
    let has_items = !columns.is_empty() || !constraints.is_empty() || !watermarks.is_empty();
    has_items.then(|| write!(&mut items, "("));
    write!(&mut items, "{}", display_comma_separated(columns))?;
    if !columns.is_empty() && (!constraints.is_empty() || !watermarks.is_empty()) {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(constraints))?;
    if !columns.is_empty() && !constraints.is_empty() && !watermarks.is_empty() {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(watermarks))?;
    has_items.then(|| write!(&mut items, ")"));
    Ok(items)
}

impl fmt::Display for CreateSourceStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(source_name, v, self);

        let items = fmt_create_items(&self.columns, &self.constraints, &self.source_watermarks)?;
        if !items.is_empty() {
            v.push(items);
        }

        impl_fmt_display!(with_properties, v, self);
        impl_fmt_display!(source_schema, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CreateSink {
    From(ObjectName),
    AsQuery(Box<Query>),
}

impl fmt::Display for CreateSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::From(mv) => write!(f, "FROM {}", mv),
            Self::AsQuery(query) => write!(f, "AS {}", query),
        }
    }
}

// sql_grammar!(CreateSinkStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     sink_name: Ident,
//     [Keyword::FROM],
//     materialized_view: Ident,
//     with_properties: AstOption<WithProperties>,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSinkStatement {
    pub if_not_exists: bool,
    pub sink_name: ObjectName,
    pub with_properties: WithProperties,
    pub sink_from: CreateSink,
    pub columns: Vec<Ident>,
    pub emit_mode: Option<EmitMode>,
    pub sink_schema: Option<ConnectorSchema>,
}

impl ParseTo for CreateSinkStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(sink_name: ObjectName, p);

        let columns = p.parse_parenthesized_column_list(IsOptional::Optional)?;

        let sink_from = if p.parse_keyword(Keyword::FROM) {
            impl_parse_to!(from_name: ObjectName, p);
            CreateSink::From(from_name)
        } else if p.parse_keyword(Keyword::AS) {
            let query = Box::new(p.parse_query()?);
            CreateSink::AsQuery(query)
        } else {
            p.expected("FROM or AS after CREATE SINK sink_name", p.peek_token())?
        };

        let emit_mode = p.parse_emit_mode()?;

        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            return Err(ParserError::ParserError(
                "sink properties not provided".to_string(),
            ));
        }

        let sink_schema = p.parse_schema()?;

        Ok(Self {
            if_not_exists,
            sink_name,
            with_properties,
            sink_from,
            columns,
            emit_mode,
            sink_schema,
        })
    }
}

impl fmt::Display for CreateSinkStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(sink_name, v, self);
        impl_fmt_display!(sink_from, v, self);
        if let Some(ref emit_mode) = self.emit_mode {
            v.push(format!("EMIT {}", emit_mode));
        }
        impl_fmt_display!(with_properties, v, self);
        if let Some(schema) = &self.sink_schema {
            v.push(format!("{}", schema));
        }
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(CreateConnectionStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     connection_name: Ident,
//     with_properties: AstOption<WithProperties>,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateConnectionStatement {
    pub if_not_exists: bool,
    pub connection_name: ObjectName,
    pub with_properties: WithProperties,
}

impl ParseTo for CreateConnectionStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(connection_name: ObjectName, p);
        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            return Err(ParserError::ParserError(
                "connection properties not provided".to_string(),
            ));
        }

        Ok(Self {
            if_not_exists,
            connection_name,
            with_properties,
        })
    }
}

impl fmt::Display for CreateConnectionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(connection_name, v, self);
        impl_fmt_display!(with_properties, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AstVec<T>(pub Vec<T>);

impl<T: fmt::Display> fmt::Display for AstVec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WithProperties(pub Vec<SqlOption>);

impl ParseTo for WithProperties {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        Ok(Self(
            parser.parse_options_with_preceding_keyword(Keyword::WITH)?,
        ))
    }
}

impl fmt::Display for WithProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "WITH ({})", display_comma_separated(self.0.as_slice()))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RowSchemaLocation {
    pub value: AstString,
}

impl ParseTo for RowSchemaLocation {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(value: AstString, p);
        Ok(Self { value })
    }
}

impl fmt::Display for RowSchemaLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v = vec![];
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(value, v, self);
        v.iter().join(" ").fmt(f)
    }
}

/// String literal. The difference with String is that it is displayed with
/// single-quotes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AstString(pub String);

impl ParseTo for AstString {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        Ok(Self(parser.parse_literal_string()?))
    }
}

impl fmt::Display for AstString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

/// This trait is used to replace `Option` because `fmt::Display` can not be implemented for
/// `Option<T>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AstOption<T> {
    /// No value
    None,
    /// Some value `T`
    Some(T),
}

impl<T: ParseTo> ParseTo for AstOption<T> {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        match T::parse_to(parser) {
            Ok(t) => Ok(AstOption::Some(t)),
            Err(_) => Ok(AstOption::None),
        }
    }
}

impl<T: fmt::Display> fmt::Display for AstOption<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            AstOption::Some(t) => t.fmt(f),
            AstOption::None => Ok(()),
        }
    }
}

impl<T> From<AstOption<T>> for Option<T> {
    fn from(val: AstOption<T>) -> Self {
        match val {
            AstOption::Some(t) => Some(t),
            AstOption::None => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateUserStatement {
    pub user_name: ObjectName,
    pub with_options: UserOptions,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AlterUserStatement {
    pub user_name: ObjectName,
    pub mode: AlterUserMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AlterUserMode {
    Options(UserOptions),
    Rename(ObjectName),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum UserOption {
    SuperUser,
    NoSuperUser,
    CreateDB,
    NoCreateDB,
    CreateUser,
    NoCreateUser,
    Login,
    NoLogin,
    EncryptedPassword(AstString),
    Password(Option<AstString>),
}

impl fmt::Display for UserOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UserOption::SuperUser => write!(f, "SUPERUSER"),
            UserOption::NoSuperUser => write!(f, "NOSUPERUSER"),
            UserOption::CreateDB => write!(f, "CREATEDB"),
            UserOption::NoCreateDB => write!(f, "NOCREATEDB"),
            UserOption::CreateUser => write!(f, "CREATEUSER"),
            UserOption::NoCreateUser => write!(f, "NOCREATEUSER"),
            UserOption::Login => write!(f, "LOGIN"),
            UserOption::NoLogin => write!(f, "NOLOGIN"),
            UserOption::EncryptedPassword(p) => write!(f, "ENCRYPTED PASSWORD {}", p),
            UserOption::Password(None) => write!(f, "PASSWORD NULL"),
            UserOption::Password(Some(p)) => write!(f, "PASSWORD {}", p),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct UserOptions(pub Vec<UserOption>);

#[derive(Default)]
struct UserOptionsBuilder {
    super_user: Option<UserOption>,
    create_db: Option<UserOption>,
    create_user: Option<UserOption>,
    login: Option<UserOption>,
    password: Option<UserOption>,
}

impl UserOptionsBuilder {
    fn build(self) -> UserOptions {
        let mut options = vec![];
        if let Some(option) = self.super_user {
            options.push(option);
        }
        if let Some(option) = self.create_db {
            options.push(option);
        }
        if let Some(option) = self.create_user {
            options.push(option);
        }
        if let Some(option) = self.login {
            options.push(option);
        }
        if let Some(option) = self.password {
            options.push(option);
        }
        UserOptions(options)
    }
}

impl ParseTo for UserOptions {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        let mut builder = UserOptionsBuilder::default();
        let add_option = |item: &mut Option<UserOption>, user_option| {
            let old_value = item.replace(user_option);
            if old_value.is_some() {
                Err(ParserError::ParserError(
                    "conflicting or redundant options".to_string(),
                ))
            } else {
                Ok(())
            }
        };
        let _ = parser.parse_keyword(Keyword::WITH);
        loop {
            let token = parser.peek_token();
            if token == Token::EOF || token == Token::SemiColon {
                break;
            }

            if let Token::Word(ref w) = token.token {
                parser.next_token();
                let (item_mut_ref, user_option) = match w.keyword {
                    Keyword::SUPERUSER => (&mut builder.super_user, UserOption::SuperUser),
                    Keyword::NOSUPERUSER => (&mut builder.super_user, UserOption::NoSuperUser),
                    Keyword::CREATEDB => (&mut builder.create_db, UserOption::CreateDB),
                    Keyword::NOCREATEDB => (&mut builder.create_db, UserOption::NoCreateDB),
                    Keyword::CREATEUSER => (&mut builder.create_user, UserOption::CreateUser),
                    Keyword::NOCREATEUSER => (&mut builder.create_user, UserOption::NoCreateUser),
                    Keyword::LOGIN => (&mut builder.login, UserOption::Login),
                    Keyword::NOLOGIN => (&mut builder.login, UserOption::NoLogin),
                    Keyword::PASSWORD => {
                        if parser.parse_keyword(Keyword::NULL) {
                            (&mut builder.password, UserOption::Password(None))
                        } else {
                            (
                                &mut builder.password,
                                UserOption::Password(Some(AstString::parse_to(parser)?)),
                            )
                        }
                    }
                    Keyword::ENCRYPTED => {
                        parser.expect_keyword(Keyword::PASSWORD)?;
                        (
                            &mut builder.password,
                            UserOption::EncryptedPassword(AstString::parse_to(parser)?),
                        )
                    }
                    _ => {
                        parser.expected(
                            "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN \
                            | NOLOGIN | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL",
                            token,
                        )?;
                        unreachable!()
                    }
                };
                add_option(item_mut_ref, user_option)?;
            } else {
                parser.expected(
                    "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN | NOLOGIN \
                        | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL",
                    token,
                )?
            }
        }
        Ok(builder.build())
    }
}

impl fmt::Display for UserOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "WITH {}", display_separated(self.0.as_slice(), " "))
        } else {
            Ok(())
        }
    }
}

impl ParseTo for CreateUserStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(user_name: ObjectName, p);
        impl_parse_to!(with_options: UserOptions, p);

        Ok(CreateUserStatement {
            user_name,
            with_options,
        })
    }
}

impl fmt::Display for CreateUserStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(user_name, v, self);
        impl_fmt_display!(with_options, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl fmt::Display for AlterUserMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlterUserMode::Options(options) => {
                write!(f, "{}", options)
            }
            AlterUserMode::Rename(new_name) => {
                write!(f, "RENAME TO {}", new_name)
            }
        }
    }
}

impl fmt::Display for AlterUserStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(user_name, v, self);
        impl_fmt_display!(mode, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for AlterUserStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(user_name: ObjectName, p);
        impl_parse_to!(mode: AlterUserMode, p);

        Ok(AlterUserStatement { user_name, mode })
    }
}

impl ParseTo for AlterUserMode {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        if p.parse_keyword(Keyword::RENAME) {
            p.expect_keyword(Keyword::TO)?;
            impl_parse_to!(new_name: ObjectName, p);
            Ok(AlterUserMode::Rename(new_name))
        } else {
            impl_parse_to!(with_options: UserOptions, p);
            Ok(AlterUserMode::Options(with_options))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DropStatement {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// Object to drop.
    pub object_name: ObjectName,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub drop_mode: AstOption<DropMode>,
}

// sql_grammar!(DropStatement {
//     object_type: ObjectType,
//     if_exists => [Keyword::IF, Keyword::EXISTS],
//     name: ObjectName,
//     drop_mode: AstOption<DropMode>,
// });
impl ParseTo for DropStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(object_type: ObjectType, p);
        impl_parse_to!(if_exists => [Keyword::IF, Keyword::EXISTS], p);
        let object_name = p.parse_object_name()?;
        impl_parse_to!(drop_mode: AstOption<DropMode>, p);
        Ok(Self {
            object_type,
            if_exists,
            object_name,
            drop_mode,
        })
    }
}

impl fmt::Display for DropStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(object_type, v, self);
        impl_fmt_display!(if_exists => [Keyword::IF, Keyword::EXISTS], v, self);
        impl_fmt_display!(object_name, v, self);
        impl_fmt_display!(drop_mode, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DropMode {
    Cascade,
    Restrict,
}

impl ParseTo for DropMode {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        let drop_mode = if parser.parse_keyword(Keyword::CASCADE) {
            DropMode::Cascade
        } else if parser.parse_keyword(Keyword::RESTRICT) {
            DropMode::Restrict
        } else {
            return parser.expected("CASCADE | RESTRICT", parser.peek_token());
        };
        Ok(drop_mode)
    }
}

impl fmt::Display for DropMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DropMode::Cascade => "CASCADE",
            DropMode::Restrict => "RESTRICT",
        })
    }
}
