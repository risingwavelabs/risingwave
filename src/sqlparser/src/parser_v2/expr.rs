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
use winnow::combinator::{alt, cut_err, opt, preceded, repeat, trace};
use winnow::{PResult, Parser};

use super::{data_type, token, ParserExt, TokenStream};
use crate::ast::Expr;
use crate::keywords::Keyword;
use crate::tokenizer::Token;

fn expr<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    // TODO: implement this function using combinator style.
    trace("expr", |input: &mut S| {
        input.parse_v1(|parser| parser.parse_expr())
    })
    .parse_next(input)
}

pub fn expr_case<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    let parse = (
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
        });

    trace("expr_case", parse).parse_next(input)
}

/// Consome a SQL CAST function e.g. `CAST(expr AS FLOAT)`
pub fn expr_cast<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    let parse = (
        cut_err(Token::LParen),
        cut_err(expr).map(Box::new),
        cut_err(Keyword::AS),
        cut_err(data_type),
        cut_err(Token::RParen),
    )
        .map(|(_, expr, _, data_type, _)| Expr::Cast { expr, data_type });

    trace("expr_cast", parse).parse_next(input)
}

/// Consume a SQL TRY_CAST function e.g. `TRY_CAST(expr AS FLOAT)`
pub fn expr_try_cast<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    let parse = (
        cut_err(Token::LParen),
        cut_err(expr).map(Box::new),
        cut_err(Keyword::AS),
        cut_err(data_type),
        cut_err(Token::RParen),
    )
        .map(|(_, expr, _, data_type, _)| Expr::TryCast { expr, data_type });

    trace("expr_try_cast", parse).parse_next(input)
}

/// Consume a SQL EXTRACT function e.g. `EXTRACT(YEAR FROM expr)`
pub fn expr_extract<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    let date_time_field = token
        .verify_map(|token| match token.token {
            Token::Word(w) => Some(w.value.to_uppercase()),
            Token::SingleQuotedString(s) => Some(s.to_uppercase()),
            _ => None,
        })
        .expect("date/time field");

    let parse = (
        cut_err(Token::LParen),
        cut_err(date_time_field),
        cut_err(Keyword::FROM),
        cut_err(expr).map(Box::new),
        cut_err(Token::RParen),
    )
        .map(|(_, field, _, expr, _)| Expr::Extract { field, expr });

    trace("expr_extract", parse).parse_next(input)
}

/// Consume `SUBSTRING (EXPR [FROM 1] [FOR 3])`
pub fn expr_substring<S>(input: &mut S) -> PResult<Expr>
where
    S: TokenStream,
{
    let parse = (
        cut_err(Token::LParen),
        cut_err(expr).map(Box::new),
        opt(preceded(
            alt((Token::Comma.void(), Keyword::FROM.void())),
            cut_err(expr).map(Box::new),
        )),
        opt(preceded(
            alt((Token::Comma.void(), Keyword::FOR.void())),
            cut_err(expr).map(Box::new),
        )),
        cut_err(Token::RParen),
    )
        .map(
            |(_, expr, substring_from, substring_for, _)| Expr::Substring {
                expr,
                substring_from,
                substring_for,
            },
        );

    trace("expr_substring", parse).parse_next(input)
}
