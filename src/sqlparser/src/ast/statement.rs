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
use super::{Ident, ObjectType, Query};
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
    pub source_schema: SourceSchema,
    pub source_watermarks: Vec<SourceWatermark>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SourceSchema {
    Protobuf(ProtobufSchema),
    // Keyword::PROTOBUF ProtobufSchema
    Json,                   // Keyword::JSON
    DebeziumJson,           // Keyword::DEBEZIUM_JSON
    UpsertJson,             // Keyword::UPSERT_JSON
    Avro(AvroSchema),       // Keyword::AVRO
    UpsertAvro(AvroSchema), // Keyword::UpsertAVRO
    Maxwell,                // Keyword::MAXWELL
    CanalJson,              // Keyword::CANAL_JSON
    Csv(CsvInfo),           // Keyword::CSV
    Native,
    DebeziumAvro(DebeziumAvroSchema), // Keyword::DEBEZIUM_AVRO
}

impl ParseTo for SourceSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        let schema = if p.parse_keywords(&[Keyword::JSON]) {
            SourceSchema::Json
        } else if p.parse_keywords(&[Keyword::UPSERT_JSON]) {
            SourceSchema::UpsertJson
        } else if p.parse_keywords(&[Keyword::PROTOBUF]) {
            impl_parse_to!(protobuf_schema: ProtobufSchema, p);
            SourceSchema::Protobuf(protobuf_schema)
        } else if p.parse_keywords(&[Keyword::DEBEZIUM_JSON]) {
            SourceSchema::DebeziumJson
        } else if p.parse_keywords(&[Keyword::AVRO]) {
            impl_parse_to!(avro_schema: AvroSchema, p);
            SourceSchema::Avro(avro_schema)
        } else if p.parse_keywords(&[Keyword::UPSERT_AVRO]) {
            impl_parse_to!(avro_schema: AvroSchema, p);
            SourceSchema::UpsertAvro(avro_schema)
        } else if p.parse_keywords(&[Keyword::MAXWELL]) {
            SourceSchema::Maxwell
        } else if p.parse_keywords(&[Keyword::CANAL_JSON]) {
            SourceSchema::CanalJson
        } else if p.parse_keywords(&[Keyword::CSV]) {
            impl_parse_to!(csv_info: CsvInfo, p);
            SourceSchema::Csv(csv_info)
        } else if p.parse_keywords(&[Keyword::DEBEZIUM_AVRO]) {
            impl_parse_to!(avro_schema: DebeziumAvroSchema, p);
            SourceSchema::DebeziumAvro(avro_schema)
        } else {
            return Err(ParserError::ParserError(
                "expected JSON | UPSERT_JSON | PROTOBUF | DEBEZIUM_JSON | DEBEZIUM_AVRO | AVRO | UPSERT_AVRO | MAXWELL | CANAL_JSON after ROW FORMAT".to_string(),
            ));
        };
        Ok(schema)
    }
}

impl fmt::Display for SourceSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SourceSchema::Protobuf(protobuf_schema) => write!(f, "PROTOBUF {}", protobuf_schema),
            SourceSchema::Json => write!(f, "JSON"),
            SourceSchema::UpsertJson => write!(f, "UPSERT JSON"),
            SourceSchema::Maxwell => write!(f, "MAXWELL"),
            SourceSchema::DebeziumJson => write!(f, "DEBEZIUM JSON"),
            SourceSchema::Avro(avro_schema) => write!(f, "AVRO {}", avro_schema),
            SourceSchema::UpsertAvro(avro_schema) => write!(f, "UPSERT AVRO {}", avro_schema),
            SourceSchema::CanalJson => write!(f, "CANAL JSON"),
            SourceSchema::Csv(csv_info) => write!(f, "CSV {}", csv_info),
            SourceSchema::Native => write!(f, "NATIVE"),
            SourceSchema::DebeziumAvro(avro_schema) => write!(f, "DEBEZIUM AVRO {}", avro_schema),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CsvInfo {
    pub delimiter: u8,
    pub has_header: bool,
}

impl ParseTo for CsvInfo {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(without_header => [Keyword::WITHOUT, Keyword::HEADER], p);
        impl_parse_to!([Keyword::DELIMITED, Keyword::BY], p);
        impl_parse_to!(delimiter: AstString, p);
        let mut chars = delimiter.0.chars().collect_vec();
        if chars.len() != 1 {
            return Err(ParserError::ParserError(format!(
                "The delimiter should be a char, but got {:?}",
                chars
            )));
        }
        let delimiter = chars.remove(0) as u8;
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

        impl_parse_to!(with_properties: WithProperties, p);
        let option = with_properties
            .0
            .iter()
            .find(|&opt| opt.name.real_value() == UPSTREAM_SOURCE_KEY);
        let connector: String = option.map(|opt| opt.value.to_string()).unwrap_or_default();
        // row format for cdc source must be debezium json
        // row format for nexmark source must be native
        // default row format for datagen source is native
        let source_schema = if connector.contains("-cdc") {
            if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
            {
                return Err(ParserError::ParserError("Row format for cdc connectors should not be set here because it is limited to debezium json".to_string()));
            }
            SourceSchema::DebeziumJson
        } else if connector.contains("nexmark") {
            if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
            {
                return Err(ParserError::ParserError("Row format for nexmark connectors should not be set here because it is limited to internal native format".to_string()));
            }
            SourceSchema::Native
        } else if connector.contains("datagen") {
            if p.peek_nth_any_of_keywords(0, &[Keyword::ROW])
                && p.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])
            {
                impl_parse_to!([Keyword::ROW, Keyword::FORMAT], p);
                SourceSchema::parse_to(p)?
            } else {
                SourceSchema::Native
            }
        } else {
            impl_parse_to!([Keyword::ROW, Keyword::FORMAT], p);
            SourceSchema::parse_to(p)?
        };

        Ok(Self {
            if_not_exists,
            columns,
            constraints,
            source_name,
            with_properties,
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
        impl_fmt_display!([Keyword::ROW, Keyword::FORMAT], v);
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

        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            return Err(ParserError::ParserError(
                "sink properties not provided".to_string(),
            ));
        }

        Ok(Self {
            if_not_exists,
            sink_name,
            with_properties,
            sink_from,
            columns,
        })
    }
}

impl fmt::Display for CreateSinkStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(sink_name, v, self);
        impl_fmt_display!(sink_from, v, self);
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
        Ok(Self(parser.parse_options(Keyword::WITH)?))
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

            if let Token::Word(ref w) = token {
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
