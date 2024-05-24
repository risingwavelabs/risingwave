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

//! Parsers for data types.
//!
//! This module contains parsers for data types. To handle the anbiguity of `>>` and `> >` in struct definition,
//! we need to use a stateful parser here. See [`with_state`] for more information.

use core::cell::RefCell;
use std::rc::Rc;

use winnow::combinator::{
    alt, cut_err, delimited, dispatch, empty, fail, opt, preceded, repeat, separated, seq,
    terminated, trace,
};
use winnow::error::{ContextError, ErrMode, ErrorKind, FromExternalError, StrContext};
use winnow::{PResult, Parser, Stateful};

use super::{
    identifier_non_reserved, keyword, literal_uint, object_name, precision_in_range, with_state,
    TokenStream,
};
use crate::ast::{DataType, StructField};
use crate::keywords::Keyword;
use crate::tokenizer::Token;

#[derive(Default, Debug)]
struct DataTypeParsingState {
    /// Since we can't distinguish between `>>` and `> >` in tokenizer, we need to handle this case in the parser.
    /// When we want a [`>`][Token::Gt] but actually consumed a [`>>`][Token::ShiftRight], we set this to true.
    /// When the value was true and we want a [`>`][Token::Gt], we just set this to false instead of really consume it.
    remaining_close: Rc<RefCell<bool>>,
}

type StatefulStream<S> = Stateful<S, DataTypeParsingState>;

/// Consume struct type definitions
fn struct_data_type<S>(input: &mut StatefulStream<S>) -> PResult<Vec<StructField>>
where
    S: TokenStream,
{
    let remaining_close1 = input.state.remaining_close.clone();
    let remaining_close2 = input.state.remaining_close.clone();

    // Consume an abstract `>`, it may be the `remaining_close1` flag set by previous `>>`.
    let consume_close = trace(
        "consume_struct_close",
        alt((
            trace(
                "consume_remaining_close",
                move |input: &mut StatefulStream<S>| -> PResult<()> {
                    if *remaining_close1.borrow() {
                        *remaining_close1.borrow_mut() = false;
                        Ok(())
                    } else {
                        fail(input)
                    }
                },
            )
            .void(),
            trace(
                "produce_remaining_close",
                (
                    Token::ShiftRight,
                    move |_input: &mut StatefulStream<S>| -> PResult<()> {
                        *remaining_close2.borrow_mut() = true;
                        Ok(())
                    },
                )
                    .void(),
            ),
            Token::Gt.void(),
        )),
    );

    // If there is an `over-consumed' `>`, we shouldn't handle `,`.
    let sep = |input: &mut StatefulStream<S>| -> PResult<()> {
        if *input.state.remaining_close.borrow() {
            fail(input)
        } else {
            Token::Comma.void().parse_next(input)
        }
    };

    delimited(
        Token::Lt,
        cut_err(separated(
            1..,
            trace(
                "struct_field",
                seq! {
                    StructField {
                        name: identifier_non_reserved,
                        data_type: data_type_stateful,
                    }
                },
            ),
            sep,
        )),
        cut_err(consume_close),
    )
    .context(StrContext::Label("struct_data_type"))
    .parse_next(input)
}

/// Consume a data type definition.
///
/// The parser is the main entry point for data type parsing.
pub fn data_type<S>(input: &mut S) -> PResult<DataType>
where
    S: TokenStream,
{
    #[derive(Debug, thiserror::Error)]
    #[error("Unconsumed `>>`")]
    struct UnconsumedShiftRight;

    with_state::<S, DataTypeParsingState, _, _>(terminated(
        data_type_stateful,
        trace("data_type_verify_state", |input: &mut StatefulStream<S>| {
            // If there is remaining `>`, we should fail.
            if *input.state.remaining_close.borrow() {
                Err(ErrMode::Cut(ContextError::from_external_error(
                    input,
                    ErrorKind::Fail,
                    UnconsumedShiftRight,
                )))
            } else {
                Ok(())
            }
        }),
    ))
    .context(StrContext::Label("data_type"))
    .parse_next(input)
}

/// Data type parsing with stateful stream.
fn data_type_stateful<S>(input: &mut StatefulStream<S>) -> PResult<DataType>
where
    S: TokenStream,
{
    repeat(0.., (Token::LBracket, cut_err(Token::RBracket)))
        .fold1(data_type_stateful_inner, |mut acc, _| {
            acc = DataType::Array(Box::new(acc));
            acc
        })
        .parse_next(input)
}

/// Consume a data type except [`DataType::Array`].
fn data_type_stateful_inner<S>(input: &mut StatefulStream<S>) -> PResult<DataType>
where
    S: TokenStream,
{
    let with_time_zone = || {
        opt(alt((
            (Keyword::WITH, Keyword::TIME, Keyword::ZONE).value(true),
            (Keyword::WITHOUT, Keyword::TIME, Keyword::ZONE).value(false),
        )))
        .map(|x| x.unwrap_or(false))
    };

    let precision_and_scale = || {
        opt(delimited(
            Token::LParen,
            (literal_uint, opt(preceded(Token::Comma, literal_uint))),
            Token::RParen,
        ))
        .map(|p| match p {
            Some((x, y)) => (Some(x), y),
            None => (None, None),
        })
    };

    let keywords = dispatch! {keyword;
        Keyword::BOOLEAN | Keyword::BOOL => empty.value(DataType::Boolean),
        Keyword::FLOAT => opt(precision_in_range(1..54)).map(DataType::Float),
        Keyword::REAL => empty.value(DataType::Real),
        Keyword::DOUBLE => opt(Keyword::PRECISION).value(DataType::Double),
        Keyword::SMALLINT => empty.value(DataType::SmallInt),
        Keyword::INT | Keyword::INTEGER => empty.value(DataType::Int),
        Keyword::BIGINT => empty.value(DataType::BigInt),
        Keyword::STRING | Keyword::VARCHAR => empty.value(DataType::Varchar),
        Keyword::CHAR | Keyword::CHARACTER => dispatch! {keyword;
            Keyword::VARYING => empty.value(DataType::Varchar),
            _ => opt(precision_in_range(..)).map(DataType::Char),
        },
        Keyword::UUID => empty.value(DataType::Uuid),
        Keyword::DATE => empty.value(DataType::Date),
        Keyword::TIMESTAMP => with_time_zone().map(DataType::Timestamp),
        Keyword::TIME => with_time_zone().map(DataType::Time),
        // TODO: Support complex interval type parsing.
        Keyword::INTERVAL => empty.value(DataType::Interval),
        Keyword::TEXT => empty.value(DataType::Text),
        Keyword::STRUCT => cut_err(struct_data_type).map(DataType::Struct),
        Keyword::BYTEA => empty.value(DataType::Bytea),
        Keyword::NUMERIC | Keyword::DECIMAL | Keyword::DEC => cut_err(precision_and_scale()).map(|(precision, scale)| {
            DataType::Decimal(precision, scale)
        }),
        _ =>  fail,
    };

    trace(
        "data_type_inner",
        alt((
            keywords,
            trace(
                "non_keyword_data_type",
                object_name.map(
                    |name| match name.to_string().to_ascii_lowercase().as_str() {
                        // PostgreSQL built-in data types that are not keywords.
                        "jsonb" => DataType::Jsonb,
                        "regclass" => DataType::Regclass,
                        "regproc" => DataType::Regproc,
                        _ => DataType::Custom(name),
                    },
                ),
            ),
        )),
    )
    .parse_next(input)
}
