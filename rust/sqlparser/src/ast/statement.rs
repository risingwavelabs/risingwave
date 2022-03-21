// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use core::fmt;
use std::collections::HashMap;

use itertools::Itertools;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::ObjectType;
use crate::ast::{display_comma_separated, Ident, SqlOption};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

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
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSourceStatement {
    pub if_not_exists: bool,
    // Source name.
    pub source_name: Ident,
    pub with_properties: WithProperties,
    pub source_schema: SourceSchema,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SourceSchema {
    Protobuf(ProtobufSchema), // Keyword::PROTOBUF ProtobufSchema
    Json,                     // Keyword::JSON
}

impl ParseTo for SourceSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        let schema = if p.parse_keywords(&[Keyword::JSON]) {
            SourceSchema::Json
        } else if p.parse_keywords(&[Keyword::PROTOBUF]) {
            impl_parse_to!(protobuf_schema: ProtobufSchema, p);
            SourceSchema::Protobuf(protobuf_schema)
        } else {
            return Err(ParserError::ParserError(
                "expected JSON | PROTOBUF after ROW FORMAT".to_string(),
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
}

impl ParseTo for ProtobufSchema {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!([Keyword::MESSAGE], p);
        impl_parse_to!(message_name: AstString, p);
        impl_parse_to!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], p);
        impl_parse_to!(row_schema_location: AstString, p);
        Ok(Self {
            message_name,
            row_schema_location,
        })
    }
}

impl fmt::Display for ProtobufSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!([Keyword::MESSAGE], v);
        impl_fmt_display!(message_name, v, self);
        impl_fmt_display!([Keyword::ROW, Keyword::SCHEMA, Keyword::LOCATION], v);
        impl_fmt_display!(row_schema_location, v, self);
        v.iter().join(" ").fmt(f)
    }
}

impl ParseTo for CreateSourceStatement {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],    p);
        impl_parse_to!(source_name: Ident, p);
        impl_parse_to!(with_properties: WithProperties, p);
        impl_parse_to!([Keyword::ROW, Keyword::FORMAT], p);
        impl_parse_to!(source_schema: SourceSchema, p);
        Ok(Self {
            if_not_exists,
            source_name,
            with_properties,
            source_schema,
        })
    }
}

impl fmt::Display for CreateSourceStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(source_name, v, self);
        impl_fmt_display!(with_properties, v, self);
        impl_fmt_display!([Keyword::ROW, Keyword::FORMAT], v);
        impl_fmt_display!(source_schema, v, self);
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

impl From<WithProperties> for HashMap<String, String> {
    fn from(props: WithProperties) -> Self {
        props
            .0
            .into_iter()
            .map(|x| (x.name.value, format!("{}", x.value)))
            .collect()
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DropStatement {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// Object to drop.
    pub name: Ident,
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
        impl_parse_to!(name: Ident, p);
        impl_parse_to!(drop_mode: AstOption<DropMode>, p);
        Ok(Self {
            object_type,
            if_exists,
            name,
            drop_mode,
        })
    }
}

impl fmt::Display for DropStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(object_type, v, self);
        impl_fmt_display!(if_exists => [Keyword::IF, Keyword::EXISTS], v, self);
        impl_fmt_display!(name, v, self);
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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            DropMode::Cascade => "CASCADE",
            DropMode::Restrict => "RESTRICT",
        })
    }
}
