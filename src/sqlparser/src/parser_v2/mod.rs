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

use winnow::combinator::{preceded, separated, trace};
use winnow::error::{ContextError, StrContext};
use winnow::stream::{Location, Stream, StreamIsPartial};
use winnow::token::{any, take_while};
use winnow::{PResult, Parser, Stateful};

use crate::ast::{Ident, ObjectName};
use crate::keywords::{self, Keyword};
use crate::tokenizer::{Token, TokenWithLocation};

mod data_type;
mod impl_;
mod number;

pub(crate) use data_type::*;
pub(crate) use impl_::TokenStreamWrapper;
pub(crate) use number::*;

/// Bundle trait requirements from winnow, so that we don't need to write them everywhere.
///
/// All combinators should accept a generic `S` that implements `TokenStream`.
pub trait TokenStream:
    Stream<Token = TokenWithLocation> + StreamIsPartial + Location + Default
{
}

impl<S> TokenStream for S where
    S: Stream<Token = TokenWithLocation> + StreamIsPartial + Location + Default
{
}

/// Consume any token, including whitespaces. In almost all cases, you should use [`token`] instead.
fn any_token<S>(input: &mut S) -> PResult<TokenWithLocation>
where
    S: TokenStream,
{
    any(input)
}

/// Consume any non-whitespace token.
///
/// If you need to consume a specific token, use [`Token::?`][Token] directly, which already implements [`Parser`].
fn token<S>(input: &mut S) -> PResult<TokenWithLocation>
where
    S: TokenStream,
{
    preceded(
        take_while(0.., |token: TokenWithLocation| {
            matches!(token.token, Token::Whitespace(_))
        }),
        any_token,
    )
    .parse_next(input)
}

/// Consume a keyword.
///
/// If you need to consume a specific keyword, use [`Keyword::?`][Keyword] directly, which already implements [`Parser`].
fn keyword<S>(input: &mut S) -> PResult<Keyword>
where
    S: TokenStream,
{
    token
        .verify_map(|t| match &t.token {
            Token::Word(w) if w.keyword != Keyword::NoKeyword => Some(w.keyword),
            _ => None,
        })
        .context(StrContext::Label("keyword"))
        .parse_next(input)
}

impl<I> Parser<I, TokenWithLocation, ContextError> for Token
where
    I: TokenStream,
{
    fn parse_next(&mut self, input: &mut I) -> PResult<TokenWithLocation, ContextError> {
        trace(
            format!("token {}", self),
            token.verify(move |t: &TokenWithLocation| t.token == *self),
        )
        .parse_next(input)
    }
}

impl<I> Parser<I, Keyword, ContextError> for Keyword
where
    I: TokenStream,
{
    fn parse_next(&mut self, input: &mut I) -> PResult<Keyword, ContextError> {
        token
            .verify_map(move |t| match &t.token {
                Token::Word(w) if *self == w.keyword => Some(w.keyword),
                _ => None,
            })
            .parse_next(input)
    }
}

/// Consume an identifier that is not a reserved keyword.
fn identifier_non_reserved<S>(input: &mut S) -> PResult<Ident>
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

/// Consume an object name.
///
/// FIXME: Object name is extremely complex, we only handle a subset here.
fn object_name<S>(input: &mut S) -> PResult<ObjectName>
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
fn with_state<S, State, O, ParseNext>(mut parse_next: ParseNext) -> impl Parser<S, O, ContextError>
where
    S: TokenStream,
    State: Default,
    ParseNext: Parser<Stateful<S, State>, O, ContextError>,
{
    move |input: &mut S| -> PResult<O> {
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

#[cfg(test)]
mod tests {
    use winnow::Located;

    use super::*;
    use crate::tokenizer::Tokenizer;

    #[test]
    fn test_basic() {
        let input = "SELECT 1";
        let tokens = Tokenizer::new(input).tokenize_with_location().unwrap();
        let mut token_stream = Located::new(&*tokens);
        Token::make_keyword("SELECT")
            .parse_next(&mut token_stream)
            .unwrap();
    }

    #[test]
    fn test_stateful() {
        let input = "SELECT 1";
        let tokens = Tokenizer::new(input).tokenize_with_location().unwrap();
        let mut token_stream = Located::new(&*tokens);
        with_state(|input: &mut Stateful<_, usize>| -> PResult<()> {
            input.state += 1;
            Token::make_keyword("SELECT").void().parse_next(input)
        })
        .parse_next(&mut token_stream)
        .unwrap();
    }
}
