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

use winnow::combinator::impls::Context;
use winnow::combinator::{separated, trace};
use winnow::error::{AddContext, ContextError, ErrMode, ParserError, StrContext};
use winnow::stream::{Stream, StreamIsPartial};
use winnow::token::any;
use winnow::{ModalResult, Parser, Stateful};

use crate::ast::{Ident, ObjectName};
use crate::keywords::{self, Keyword};
use crate::tokenizer::{Token, TokenWithLocation};

mod compact;
mod data_type;
mod expr;
mod impl_;
mod number;

pub(crate) use data_type::*;
pub(crate) use expr::*;
pub(crate) use number::*;

/// Bundle trait requirements from winnow, so that we don't need to write them everywhere.
///
/// All combinators should accept a generic `S` that implements `TokenStream`.
pub trait TokenStream:
    Stream<Token = TokenWithLocation> + StreamIsPartial + Default + compact::ParseV1
{
}

impl<S> TokenStream for S where
    S: Stream<Token = TokenWithLocation> + StreamIsPartial + Default + compact::ParseV1
{
}

/// Consume any token.
///
/// If you need to consume a specific token, use [`Token::?`][Token] directly, which already implements [`Parser`].
fn token<S>(input: &mut S) -> ModalResult<TokenWithLocation>
where
    S: TokenStream,
{
    any(input)
}

/// Consume a keyword.
///
/// If you need to consume a specific keyword, use [`Keyword::?`][Keyword] directly, which already implements [`Parser`].
pub fn keyword<S>(input: &mut S) -> ModalResult<Keyword>
where
    S: TokenStream,
{
    trace(
        "keyword",
        token.verify_map(|t| match &t.token {
            Token::Word(w) if w.keyword != Keyword::NoKeyword => Some(w.keyword),
            _ => None,
        }),
    )
    .context(StrContext::Label("keyword"))
    .parse_next(input)
}

impl<I> Parser<I, TokenWithLocation, ErrMode<ContextError>> for Token
where
    I: TokenStream,
{
    fn parse_next(&mut self, input: &mut I) -> ModalResult<TokenWithLocation, ContextError> {
        trace(
            format_args!("token {}", self.clone()),
            token.verify(move |t: &TokenWithLocation| t.token == *self),
        )
        .parse_next(input)
    }
}

impl<I> Parser<I, Keyword, ErrMode<ContextError>> for Keyword
where
    I: TokenStream,
{
    fn parse_next(&mut self, input: &mut I) -> ModalResult<Keyword, ContextError> {
        trace(
            format_args!("keyword {}", self.clone()),
            token.verify_map(move |t| match &t.token {
                Token::Word(w) if *self == w.keyword => Some(w.keyword),
                _ => None,
            }),
        )
        .parse_next(input)
    }
}

/// Consume an identifier that is not a reserved keyword.
fn identifier_non_reserved<S>(input: &mut S) -> ModalResult<Ident>
where
    S: TokenStream,
{
    // FIXME: Reporting error correctly.
    token
        .verify_map(|t| match &t.token {
            Token::Word(w) if !keywords::RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(&w.keyword) => {
                w.to_ident().ok()
            }
            _ => None,
        })
        .parse_next(input)
}

/// Consume an 'single-quoted string'.
pub fn single_quoted_string<S>(input: &mut S) -> ModalResult<String>
where
    S: TokenStream,
{
    token
        .verify_map(|t| match &t.token {
            Token::SingleQuotedString(s) => Some(s.clone()),
            _ => None,
        })
        .parse_next(input)
}

/// Consume an $$ dollar-quoted string $$.
pub fn dollar_quoted_string<S>(input: &mut S) -> ModalResult<String>
where
    S: TokenStream,
{
    token
        .verify_map(|t| match &t.token {
            Token::DollarQuotedString(s) => Some(s.value.clone()),
            _ => None,
        })
        .parse_next(input)
}

/// Consume an object name.
///
/// FIXME: Object name is extremely complex, we only handle a subset here.
fn object_name<S>(input: &mut S) -> ModalResult<ObjectName>
where
    S: TokenStream,
{
    separated(1.., identifier_non_reserved, Token::Period)
        .map(ObjectName)
        .parse_next(input)
}

/// Accept a subparser contains a given state.
///
/// The state will be constructed using [`Default::default()`].
fn with_state<S, State, O, ParseNext, E>(mut parse_next: ParseNext) -> impl Parser<S, O, E>
where
    S: TokenStream,
    State: Default,
    ParseNext: Parser<Stateful<S, State>, O, E>,
    E: ParserError<S>,
{
    move |input: &mut S| -> winnow::Result<O, E> {
        let state = State::default();
        let input2 = std::mem::take(input);
        let mut stateful = Stateful {
            input: input2,
            state,
        };
        let output = parse_next.parse_next(&mut stateful);
        *input = stateful.input;
        output
    }
}

pub trait ParserExt<I, O, E>: Parser<I, O, E> {
    /// Add a context to the error message.
    ///
    /// This is a shorthand for `context(StrContext::Expected(StrContextValue::Description(expected)))`.
    fn expect(self, expected: &'static str) -> Context<Self, I, O, E, StrContext>
    where
        Self: Sized,
        I: Stream,
        E: AddContext<I, StrContext>,
        E: ParserError<I>,
    {
        self.context(StrContext::Expected(
            winnow::error::StrContextValue::Description(expected),
        ))
    }
}

impl<I, O, E, T> ParserExt<I, O, E> for T where T: Parser<I, O, E> {}
