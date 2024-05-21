use core::ops::{Range, RangeBounds};

use winnow::combinator::{delimited, opt};
use winnow::error::ContextError;
use winnow::{PResult, Parser};

use super::{token, TokenStream};
use crate::tokenizer::Token;

pub fn token_number<S>(input: &mut S) -> PResult<String>
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
        .parse_next(input)
}

/// Parse an unsigned literal integer/long
pub fn literal_uint<S>(input: &mut S) -> PResult<u64>
where
    S: TokenStream,
{
    token_number.try_map(|s| s.parse::<u64>()).parse_next(input)
}

pub fn precision_in_range<S>(range: impl RangeBounds<u64>) -> impl Parser<S, u64, ContextError>
where
    S: TokenStream,
{
    delimited(Token::LParen, literal_uint, Token::LParen).verify(move |v| range.contains(v))
}
