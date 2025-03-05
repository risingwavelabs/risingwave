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

use core::ops::RangeBounds;

use winnow::combinator::{cut_err, delimited};
use winnow::error::{ContextError, StrContext};
use winnow::{ModalParser, ModalResult, Parser};

use super::{TokenStream, token};
use crate::tokenizer::Token;

/// Consume a [number][Token::Number] from token.
pub fn token_number<S>(input: &mut S) -> ModalResult<String>
where
    S: TokenStream,
{
    token
        .verify_map(|t| {
            if let Token::Number(number) = t.token {
                Some(number)
            } else {
                None
            }
        })
        .context(StrContext::Label("number"))
        .parse_next(input)
}

/// Consume an unsigned literal integer/long
pub fn literal_uint<S>(input: &mut S) -> ModalResult<u64>
where
    S: TokenStream,
{
    token_number
        .try_map(|s| s.parse::<u64>())
        .context(StrContext::Label("u64"))
        .parse_next(input)
}

/// Consume an unsigned literal integer
pub fn literal_u32<S>(input: &mut S) -> ModalResult<u32>
where
    S: TokenStream,
{
    token_number
        .try_map(|s| s.parse::<u32>())
        .context(StrContext::Label("u32"))
        .parse_next(input)
}

/// Consume an literal integer
pub fn literal_i64<S>(input: &mut S) -> ModalResult<i64>
where
    S: TokenStream,
{
    token_number
        .try_map(|s| s.parse::<i64>())
        .context(StrContext::Label("i64"))
        .parse_next(input)
}

/// Consume a precision definition in some types, e.g. `FLOAT(32)`.
///
/// The precision must be in the given range.
pub fn precision_in_range<S>(
    range: impl RangeBounds<u64> + std::fmt::Debug,
) -> impl ModalParser<S, u64, ContextError>
where
    S: TokenStream,
{
    #[derive(Debug, thiserror::Error)]
    #[error("precision must be in range {0}")]
    struct OutOfRange(String);

    delimited(
        Token::LParen,
        cut_err(literal_uint.try_map(move |v| {
            if range.contains(&v) {
                Ok(v)
            } else {
                Err(OutOfRange(format!("{:?}", range)))
            }
        })),
        cut_err(Token::RParen),
    )
}
