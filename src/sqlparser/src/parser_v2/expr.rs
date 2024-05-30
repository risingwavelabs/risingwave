use winnow::combinator::{
    cut_err, delimited, empty, fail, opt, preceded, repeat, seq, todo, trace,
};
use winnow::error::{ContextError, ErrMode};
use winnow::{PResult, Parser};

use super::{data_type, keyword, TokenStream};
use crate::ast::Expr;
use crate::keywords::Keyword;
use crate::parser::Precedence;
use crate::tokenizer::Token;

fn expr<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    subexpr(Precedence::Zero).parse_next(input)
}

fn subexpr<S>(min_precedence: Precedence) -> impl Parser<S, Expr, ContextError>
where
    S: TokenStream,
{
    trace("subexpr", move |input: &mut S| {
        let mut expr = expr_prefix(input)?;
        loop {
            let next_precedence = peek_precedence(input)?;
            match empty
                .verify(|_| next_precedence < min_precedence)
                .parse_next(input)
            {
                Ok(_) => expr = expr_infix(expr, next_precedence).parse_next(input)?,
                Err(ErrMode::Backtrack(_)) => return Ok(expr),
                Err(e) => return Err(e.into()),
            }
        }
    })
}

/// Consume an expression prefix
fn expr_prefix<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    todo(input)
}

fn peek_precedence<S>(input: &mut S) -> PResult<Precedence>
where
    S: TokenStream,
{
    fail(input)
}

fn expr_infix<S>(prefix: Expr, min_precedence: Precedence) -> impl Parser<S, Expr, ContextError>
where
    S: TokenStream,
{
    fail
}

fn expr_case<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    (
        opt(expr),
        repeat(
            1..,
            (
                Keyword::WHEN,
                cut_err(expr),
                cut_err(Keyword::THEN),
                cut_err(expr),
            ),
        ),
        opt(preceded(Keyword::ELSE, cut_err(expr))),
        cut_err(Keyword::END),
    )
        .map(|(operand, branches, else_result, _)| {
            let branches: Vec<_> = branches;
            let (conditions, results) = branches.into_iter().map(|(_, c, _, t)| (c, t)).unzip();
            Expr::Case {
                operand: operand.map(Box::new),
                conditions,
                results,
                else_result: else_result.map(Box::new),
            }
        })
        .parse_next(input)
}

fn expr_cast<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    delimited(
        Token::LParen,
        cut_err((expr.map(Box::new), cut_err(Keyword::AS), cut_err(data_type))),
        cut_err(Token::RParen),
    )
    .map(|(expr, _, data_type)| Expr::Cast { expr, data_type })
    .parse_next(input)
}

fn expr_try_cast<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    fail(input)
}

fn expr_exists<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    fail(input)
}

fn expr_extract<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    fail(input)
}

fn expr_substring<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    fail(input)
}
