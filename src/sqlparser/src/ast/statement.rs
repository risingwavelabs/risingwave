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

use core::fmt;
use core::fmt::Formatter;
use std::fmt::Write;

use itertools::Itertools;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use winnow::ModalResult;

use super::ddl::SourceWatermark;
use super::legacy_source::{CompatibleFormatEncode, parse_format_encode};
use super::{EmitMode, Ident, ObjectType, Query, Value};
use crate::ast::{
    ColumnDef, ObjectName, SqlOption, TableConstraint, display_comma_separated, display_separated,
};
use crate::keywords::Keyword;
use crate::parser::{IncludeOption, IsOptional, Parser};
use crate::parser_err;
use crate::parser_v2::literal_u32;
use crate::tokenizer::Token;

/// Consumes token from the parser into an AST node.
pub trait ParseTo: Sized {
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self>;
}

#[macro_export]
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

#[macro_export]
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
            $v.push(format!("{}", display_separated(&[$($arr)+], " ")));
        }
    };
    ([$($arr:tt)+], $v:ident) => {
        $v.push(format!("{}", display_separated(&[$($arr)+], " ")));
    };
}

// sql_grammar!(CreateSourceStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     source_name: Ident,
//     with_properties: AstOption<WithProperties>,
//     [Keyword::ROW, Keyword::FORMAT],
//     format_encode: SourceSchema,
//     [Keyword::WATERMARK, Keyword::FOR] column [Keyword::AS] <expr>
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSourceStatement {
    pub temporary: bool,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    // The wildchar position in columns defined in sql. Only exist when using external schema.
    pub wildcard_idx: Option<usize>,
    pub constraints: Vec<TableConstraint>,
    pub source_name: ObjectName,
    pub with_properties: WithProperties,
    pub format_encode: CompatibleFormatEncode,
    pub source_watermarks: Vec<SourceWatermark>,
    pub include_column_options: IncludeOption,
}

/// FORMAT means how to get the operation(Insert/Delete) from the input.
///
/// Check `CONNECTORS_COMPATIBLE_FORMATS` for what `FORMAT ... ENCODE ...` combinations are allowed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Format {
    /// The format is the same with RisingWave's internal representation.
    /// Used internally for schema change
    Native,
    /// for self-explanatory sources like iceberg, they have their own format, and should not be specified by user.
    None,
    // Keyword::DEBEZIUM
    Debezium,
    // Keyword::DEBEZIUM_MONGO
    DebeziumMongo,
    // Keyword::MAXWELL
    Maxwell,
    // Keyword::CANAL
    Canal,
    // Keyword::UPSERT
    Upsert,
    // Keyword::PLAIN
    Plain,
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
                Format::None => "NONE",
            }
        )
    }
}

impl Format {
    pub fn from_keyword(s: &str) -> ModalResult<Self> {
        Ok(match s {
            "DEBEZIUM" => Format::Debezium,
            "DEBEZIUM_MONGO" => Format::DebeziumMongo,
            "MAXWELL" => Format::Maxwell,
            "CANAL" => Format::Canal,
            "PLAIN" => Format::Plain,
            "UPSERT" => Format::Upsert,
            "NATIVE" => Format::Native,
            "NONE" => Format::None,
            _ => parser_err!(
                "expected CANAL | PROTOBUF | DEBEZIUM | MAXWELL | PLAIN | NATIVE | NONE after FORMAT"
            ),
        })
    }
}

/// Check `CONNECTORS_COMPATIBLE_FORMATS` for what `FORMAT ... ENCODE ...` combinations are allowed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Encode {
    Avro,     // Keyword::Avro
    Csv,      // Keyword::CSV
    Protobuf, // Keyword::PROTOBUF
    Json,     // Keyword::JSON
    Bytes,    // Keyword::BYTES
    /// for self-explanatory sources like iceberg, they have their own format, and should not be specified by user.
    None,
    Text, // Keyword::TEXT
    /// The encode is the same with RisingWave's internal representation.
    /// Used internally for schema change
    Native,
    Template,
    Parquet,
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
                Encode::None => "NONE",
                Encode::Parquet => "PARQUET",
                Encode::Text => "TEXT",
            }
        )
    }
}

impl Encode {
    pub fn from_keyword(s: &str) -> ModalResult<Self> {
        Ok(match s {
            "AVRO" => Encode::Avro,
            "TEXT" => Encode::Text,
            "BYTES" => Encode::Bytes,
            "CSV" => Encode::Csv,
            "PROTOBUF" => Encode::Protobuf,
            "JSON" => Encode::Json,
            "TEMPLATE" => Encode::Template,
            "PARQUET" => Encode::Parquet,
            "NATIVE" => Encode::Native,
            "NONE" => Encode::None,
            _ => parser_err!(
                "expected AVRO | BYTES | CSV | PROTOBUF | JSON | NATIVE | TEMPLATE | PARQUET | NONE after Encode"
            ),
        })
    }
}

/// `FORMAT ... ENCODE ... [(a=b, ...)] [KEY ENCODE ...]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FormatEncodeOptions {
    pub format: Format,
    pub row_encode: Encode,
    pub row_options: Vec<SqlOption>,

    pub key_encode: Option<Encode>,
}

impl Parser<'_> {
    /// Peek the next tokens to see if it is `FORMAT` or `ROW FORMAT` (for compatibility).
    fn peek_format_encode_format(&mut self) -> bool {
        (self.peek_nth_any_of_keywords(0, &[Keyword::ROW])
            && self.peek_nth_any_of_keywords(1, &[Keyword::FORMAT])) // ROW FORMAT
            || self.peek_nth_any_of_keywords(0, &[Keyword::FORMAT]) // FORMAT
    }

    /// Parse the source schema. The behavior depends on the `connector` type.
    pub fn parse_format_encode_with_connector(
        &mut self,
        connector: &str,
        cdc_source_job: bool,
    ) -> ModalResult<CompatibleFormatEncode> {
        // row format for cdc source must be debezium json
        // row format for nexmark source must be native
        // default row format for datagen source is native
        // FIXME: parse input `connector` to enum type instead using string here
        if connector.contains("-cdc") {
            let expected = if cdc_source_job {
                FormatEncodeOptions::plain_json()
            } else if connector.contains("mongodb") {
                FormatEncodeOptions::debezium_mongo_json()
            } else {
                FormatEncodeOptions::debezium_json()
            };

            if self.peek_format_encode_format() {
                let schema = parse_format_encode(self)?.into_v2();
                if schema != expected {
                    parser_err!(
                        "Row format for CDC connectors should be \
                         either omitted or set to `{expected}`",
                    );
                }
            }
            Ok(expected.into())
        } else if connector.contains("nexmark") {
            let expected = FormatEncodeOptions::native();
            if self.peek_format_encode_format() {
                let schema = parse_format_encode(self)?.into_v2();
                if schema != expected {
                    parser_err!(
                        "Row format for nexmark connectors should be \
                         either omitted or set to `{expected}`",
                    );
                }
            }
            Ok(expected.into())
        } else if connector.contains("datagen") {
            Ok(if self.peek_format_encode_format() {
                parse_format_encode(self)?
            } else {
                FormatEncodeOptions::native().into()
            })
        } else if connector.contains("iceberg") {
            let expected = FormatEncodeOptions::none();
            if self.peek_format_encode_format() {
                let schema = parse_format_encode(self)?.into_v2();
                if schema != expected {
                    parser_err!(
                        "Row format for iceberg connectors should be \
                         either omitted or set to `{expected}`",
                    );
                }
            }
            Ok(expected.into())
        } else if connector.contains("webhook") {
            parser_err!(
                "Source with webhook connector is not supported. \
                 Please use the `CREATE TABLE ... WITH ...` statement instead.",
            );
        } else {
            Ok(parse_format_encode(self)?)
        }
    }

    /// Parse `FORMAT ... ENCODE ... (...)`.
    pub fn parse_schema(&mut self) -> ModalResult<Option<FormatEncodeOptions>> {
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

        let key_encode = if self.parse_keywords(&[Keyword::KEY, Keyword::ENCODE]) {
            Some(Encode::from_keyword(
                self.parse_identifier()?.value.to_ascii_uppercase().as_str(),
            )?)
        } else {
            None
        };

        Ok(Some(FormatEncodeOptions {
            format,
            row_encode,
            row_options,
            key_encode,
        }))
    }
}

impl FormatEncodeOptions {
    pub const fn plain_json() -> Self {
        FormatEncodeOptions {
            format: Format::Plain,
            row_encode: Encode::Json,
            row_options: Vec::new(),
            key_encode: None,
        }
    }

    /// Create a new source schema with `Debezium` format and `Json` encoding.
    pub const fn debezium_json() -> Self {
        FormatEncodeOptions {
            format: Format::Debezium,
            row_encode: Encode::Json,
            row_options: Vec::new(),
            key_encode: None,
        }
    }

    pub const fn debezium_mongo_json() -> Self {
        FormatEncodeOptions {
            format: Format::DebeziumMongo,
            row_encode: Encode::Json,
            row_options: Vec::new(),
            key_encode: None,
        }
    }

    /// Create a new source schema with `Native` format and encoding.
    pub const fn native() -> Self {
        FormatEncodeOptions {
            format: Format::Native,
            row_encode: Encode::Native,
            row_options: Vec::new(),
            key_encode: None,
        }
    }

    /// Create a new source schema with `None` format and encoding.
    /// Used for self-explanatory source like iceberg.
    pub const fn none() -> Self {
        FormatEncodeOptions {
            format: Format::None,
            row_encode: Encode::None,
            row_options: Vec::new(),
            key_encode: None,
        }
    }

    pub fn row_options(&self) -> &[SqlOption] {
        self.row_options.as_ref()
    }
}

impl fmt::Display for FormatEncodeOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FORMAT {} ENCODE {}", self.format, self.row_encode)?;

        if !self.row_options().is_empty() {
            write!(f, " ({})", display_comma_separated(self.row_options()))?;
        }

        if let Some(key_encode) = &self.key_encode {
            write!(f, " KEY ENCODE {}", key_encode)?;
        }

        Ok(())
    }
}

pub(super) fn fmt_create_items(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
    watermarks: &[SourceWatermark],
    wildcard_idx: Option<usize>,
) -> std::result::Result<String, fmt::Error> {
    let mut items = String::new();
    let has_items = !columns.is_empty()
        || !constraints.is_empty()
        || !watermarks.is_empty()
        || wildcard_idx.is_some();
    has_items.then(|| write!(&mut items, "("));

    if let Some(wildcard_idx) = wildcard_idx {
        let (columns_l, columns_r) = columns.split_at(wildcard_idx);
        write!(&mut items, "{}", display_comma_separated(columns_l))?;
        if !columns_l.is_empty() {
            write!(&mut items, ", ")?;
        }
        write!(&mut items, "{}", Token::Mul)?;
        if !columns_r.is_empty() {
            write!(&mut items, ", ")?;
        }
        write!(&mut items, "{}", display_comma_separated(columns_r))?;
    } else {
        write!(&mut items, "{}", display_comma_separated(columns))?;
    }
    let mut leading_items = !columns.is_empty() || wildcard_idx.is_some();

    if leading_items && !constraints.is_empty() {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(constraints))?;
    leading_items |= !constraints.is_empty();

    if leading_items && !watermarks.is_empty() {
        write!(&mut items, ", ")?;
    }
    write!(&mut items, "{}", display_comma_separated(watermarks))?;
    // uncomment this when adding more sections below
    // leading_items |= !watermarks.is_empty();

    has_items.then(|| write!(&mut items, ")"));
    Ok(items)
}

impl fmt::Display for CreateSourceStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(source_name, v, self);

        let items = fmt_create_items(
            &self.columns,
            &self.constraints,
            &self.source_watermarks,
            self.wildcard_idx,
        )?;
        if !items.is_empty() {
            v.push(items);
        }

        for item in &self.include_column_options {
            v.push(format!("{}", item));
        }

        // skip format_encode for cdc source
        let is_cdc_source = self.with_properties.0.iter().any(|option| {
            option.name.real_value().eq_ignore_ascii_case("connector")
                && option.value.to_string().contains("cdc")
        });

        impl_fmt_display!(with_properties, v, self);
        if !is_cdc_source {
            impl_fmt_display!(format_encode, v, self);
        }
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

    // only used when creating sink into a table
    // insert to specific columns of the target table
    pub columns: Vec<Ident>,
    pub emit_mode: Option<EmitMode>,
    pub sink_schema: Option<FormatEncodeOptions>,
    pub into_table_name: Option<ObjectName>,
}

impl ParseTo for CreateSinkStatement {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(sink_name: ObjectName, p);

        let mut target_spec_columns = Vec::new();
        let into_table_name = if p.parse_keyword(Keyword::INTO) {
            impl_parse_to!(into_table_name: ObjectName, p);

            // we only allow specify columns when creating sink into a table
            target_spec_columns = p.parse_parenthesized_column_list(IsOptional::Optional)?;
            Some(into_table_name)
        } else {
            None
        };

        let sink_from = if p.parse_keyword(Keyword::FROM) {
            impl_parse_to!(from_name: ObjectName, p);
            CreateSink::From(from_name)
        } else if p.parse_keyword(Keyword::AS) {
            let query = Box::new(p.parse_query()?);
            CreateSink::AsQuery(query)
        } else {
            p.expected("FROM or AS after CREATE SINK sink_name")?
        };

        let emit_mode: Option<EmitMode> = p.parse_emit_mode()?;

        // This check cannot be put into the `WithProperties::parse_to`, since other
        // statements may not need the with properties.
        if !p.peek_nth_any_of_keywords(0, &[Keyword::WITH]) && into_table_name.is_none() {
            p.expected("WITH")?
        }
        impl_parse_to!(with_properties: WithProperties, p);

        if with_properties.0.is_empty() && into_table_name.is_none() {
            parser_err!("sink properties not provided");
        }

        let sink_schema = p.parse_schema()?;

        Ok(Self {
            if_not_exists,
            sink_name,
            with_properties,
            sink_from,
            columns: target_spec_columns,
            emit_mode,
            sink_schema,
            into_table_name,
        })
    }
}

impl fmt::Display for CreateSinkStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(sink_name, v, self);
        if let Some(into_table) = &self.into_table_name {
            impl_fmt_display!([Keyword::INTO], v);
            impl_fmt_display!([into_table], v);
            if !self.columns.is_empty() {
                v.push(format!("({})", display_comma_separated(&self.columns)));
            }
        }
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

// sql_grammar!(CreateSubscriptionStatement {
//     if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS],
//     subscription_name: Ident,
//     [Keyword::FROM],
//     materialized_view: Ident,
//     with_properties: AstOption<WithProperties>,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSubscriptionStatement {
    pub if_not_exists: bool,
    pub subscription_name: ObjectName,
    pub with_properties: WithProperties,
    pub subscription_from: ObjectName,
    // pub emit_mode: Option<EmitMode>,
}

impl ParseTo for CreateSubscriptionStatement {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(subscription_name: ObjectName, p);

        let subscription_from = if p.parse_keyword(Keyword::FROM) {
            impl_parse_to!(from_name: ObjectName, p);
            from_name
        } else {
            p.expected("FROM after CREATE SUBSCRIPTION subscription_name")?
        };

        // let emit_mode = p.parse_emit_mode()?;

        // This check cannot be put into the `WithProperties::parse_to`, since other
        // statements may not need the with properties.
        if !p.peek_nth_any_of_keywords(0, &[Keyword::WITH]) {
            p.expected("WITH")?
        }
        impl_parse_to!(with_properties: WithProperties, p);

        if with_properties.0.is_empty() {
            parser_err!("subscription properties not provided");
        }

        Ok(Self {
            if_not_exists,
            subscription_name,
            with_properties,
            subscription_from,
        })
    }
}

impl fmt::Display for CreateSubscriptionStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(subscription_name, v, self);
        v.push(format!("FROM {}", self.subscription_from));
        impl_fmt_display!(with_properties, v, self);
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DeclareCursor {
    Query(Box<Query>),
    Subscription(ObjectName, Since),
}

impl fmt::Display for DeclareCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        match self {
            DeclareCursor::Query(query) => v.push(format!("{}", query.as_ref())),
            DeclareCursor::Subscription(name, since) => {
                v.push(format!("{}", name));
                v.push(format!("{:?}", since));
            }
        }
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(DeclareCursorStatement {
//     cursor_name: Ident,
//     [Keyword::SUBSCRIPTION]
//     [Keyword::CURSOR],
//     [Keyword::FOR],
//     subscription: Ident or query: Query,
//     [Keyword::SINCE],
//     rw_timestamp: Ident,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DeclareCursorStatement {
    pub cursor_name: Ident,
    pub declare_cursor: DeclareCursor,
}

impl ParseTo for DeclareCursorStatement {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        let cursor_name = p.parse_identifier_non_reserved()?;

        let declare_cursor = if !p.parse_keyword(Keyword::SUBSCRIPTION) {
            p.expect_keyword(Keyword::CURSOR)?;
            p.expect_keyword(Keyword::FOR)?;
            DeclareCursor::Query(Box::new(p.parse_query()?))
        } else {
            p.expect_keyword(Keyword::CURSOR)?;
            p.expect_keyword(Keyword::FOR)?;
            let cursor_for_name = p.parse_object_name()?;
            let rw_timestamp = p.parse_since()?;
            DeclareCursor::Subscription(cursor_for_name, rw_timestamp)
        };

        Ok(Self {
            cursor_name,
            declare_cursor,
        })
    }
}

impl fmt::Display for DeclareCursorStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(cursor_name, v, self);
        match &self.declare_cursor {
            DeclareCursor::Query(_) => {
                v.push("CURSOR FOR ".to_owned());
            }
            DeclareCursor::Subscription { .. } => {
                v.push("SUBSCRIPTION CURSOR FOR ".to_owned());
            }
        }
        impl_fmt_display!(declare_cursor, v, self);
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(FetchCursorStatement {
//     cursor_name: Ident,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FetchCursorStatement {
    pub cursor_name: Ident,
    pub count: u32,
    pub with_properties: WithProperties,
}

impl ParseTo for FetchCursorStatement {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        let count = if p.parse_keyword(Keyword::NEXT) {
            1
        } else {
            literal_u32(p)?
        };
        p.expect_keyword(Keyword::FROM)?;
        let cursor_name = p.parse_identifier_non_reserved()?;
        impl_parse_to!(with_properties: WithProperties, p);

        Ok(Self {
            cursor_name,
            count,
            with_properties,
        })
    }
}

impl fmt::Display for FetchCursorStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        if self.count == 1 {
            v.push("NEXT ".to_owned());
        } else {
            impl_fmt_display!(count, v, self);
        }
        v.push("FROM ".to_owned());
        impl_fmt_display!(cursor_name, v, self);
        v.iter().join(" ").fmt(f)
    }
}

// sql_grammar!(CloseCursorStatement {
//     cursor_name: Ident,
// });
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CloseCursorStatement {
    pub cursor_name: Option<Ident>,
}

impl ParseTo for CloseCursorStatement {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        let cursor_name = if p.parse_keyword(Keyword::ALL) {
            None
        } else {
            Some(p.parse_identifier_non_reserved()?)
        };

        Ok(Self { cursor_name })
    }
}

impl fmt::Display for CloseCursorStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        if let Some(cursor_name) = &self.cursor_name {
            v.push(format!("{}", cursor_name));
        } else {
            v.push("ALL".to_owned());
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], p);
        impl_parse_to!(connection_name: ObjectName, p);
        impl_parse_to!(with_properties: WithProperties, p);
        if with_properties.0.is_empty() {
            parser_err!("connection properties not provided");
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
pub struct CreateSecretStatement {
    pub if_not_exists: bool,
    pub secret_name: ObjectName,
    pub credential: Value,
    pub with_properties: WithProperties,
}

impl ParseTo for CreateSecretStatement {
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
        impl_parse_to!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], parser);
        impl_parse_to!(secret_name: ObjectName, parser);
        impl_parse_to!(with_properties: WithProperties, parser);
        let mut credential = Value::Null;
        if parser.parse_keyword(Keyword::AS) {
            credential = parser.ensure_parse_value()?;
        }
        Ok(Self {
            if_not_exists,
            secret_name,
            credential,
            with_properties,
        })
    }
}

impl fmt::Display for CreateSecretStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut v: Vec<String> = vec![];
        impl_fmt_display!(if_not_exists => [Keyword::IF, Keyword::NOT, Keyword::EXISTS], v, self);
        impl_fmt_display!(secret_name, v, self);
        impl_fmt_display!(with_properties, v, self);
        if self.credential != Value::Null {
            v.push("AS".to_owned());
            impl_fmt_display!(credential, v, self);
        }
        v.iter().join(" ").fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WithProperties(pub Vec<SqlOption>);

impl ParseTo for WithProperties {
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
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
pub enum Since {
    TimestampMsNum(u64),
    ProcessTime,
    Begin,
    Full,
}

impl fmt::Display for Since {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Since::*;
        match self {
            TimestampMsNum(ts) => write!(f, " SINCE {}", ts),
            ProcessTime => write!(f, " SINCE PROCTIME()"),
            Begin => write!(f, " SINCE BEGIN()"),
            Full => write!(f, " FULL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RowSchemaLocation {
    pub value: AstString,
}

impl ParseTo for RowSchemaLocation {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
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
    OAuth(Vec<SqlOption>),
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
            UserOption::OAuth(options) => {
                write!(f, "({})", display_comma_separated(options.as_slice()))
            }
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
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
        let mut builder = UserOptionsBuilder::default();
        let add_option = |item: &mut Option<UserOption>, user_option| {
            let old_value = item.replace(user_option);
            if old_value.is_some() {
                parser_err!("conflicting or redundant options");
            }
            Ok(())
        };
        let _ = parser.parse_keyword(Keyword::WITH);
        loop {
            let token = parser.peek_token();
            if token == Token::EOF || token == Token::SemiColon {
                break;
            }

            if let Token::Word(ref w) = token.token {
                let checkpoint = *parser;
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
                    Keyword::OAUTH => {
                        let options = parser.parse_options()?;
                        (&mut builder.password, UserOption::OAuth(options))
                    }
                    _ => {
                        parser.expected_at(
                            checkpoint,
                            "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN \
                            | NOLOGIN | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL | OAUTH",
                        )?;
                        unreachable!()
                    }
                };
                add_option(item_mut_ref, user_option)?;
            } else {
                parser.expected(
                    "SUPERUSER | NOSUPERUSER | CREATEDB | NOCREATEDB | LOGIN | NOLOGIN \
                        | CREATEUSER | NOCREATEUSER | [ENCRYPTED] PASSWORD | NULL | OAUTH",
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
        impl_parse_to!(user_name: ObjectName, p);
        impl_parse_to!(mode: AlterUserMode, p);

        Ok(AlterUserStatement { user_name, mode })
    }
}

impl ParseTo for AlterUserMode {
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(p: &mut Parser<'_>) -> ModalResult<Self> {
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
    fn parse_to(parser: &mut Parser<'_>) -> ModalResult<Self> {
        let drop_mode = if parser.parse_keyword(Keyword::CASCADE) {
            DropMode::Cascade
        } else if parser.parse_keyword(Keyword::RESTRICT) {
            DropMode::Restrict
        } else {
            return parser.expected("CASCADE | RESTRICT");
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
