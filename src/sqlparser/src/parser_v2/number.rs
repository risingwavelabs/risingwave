use core::ops::RangeBounds;

use winnow::combinator::{cut_err, delimited};
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

pub fn precision_in_range<S>(
    range: impl RangeBounds<u64> + std::fmt::Debug,
) -> impl Parser<S, u64, ContextError>
where
    S: TokenStream,
{
    #[derive(Debug, thiserror::Error)]
    enum Error {
        #[error("Precision must be in range {0:?}")]
        OutOfRange(String),
    }

    cut_err(
        delimited(Token::LParen, literal_uint, Token::LParen).try_map(move |v| {
            if range.contains(&v) {
                Ok(v)
            } else {
                Err(Error::OutOfRange(format!("{:?}", range)))
            }
        }),
    )
}
