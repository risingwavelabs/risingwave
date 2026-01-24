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
use winnow::combinator::{alt, cut_err, opt, preceded, repeat, seq, trace};
use winnow::error::ContextError;
use winnow::{ModalParser, ModalResult, Parser};

use super::{ParserExt, TokenStream, data_type, token};
use crate::ast::Expr;
use crate::keywords::Keyword;
use crate::parser::Precedence;
use crate::tokenizer::Token;

fn expr_parse<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    // TODO: implement this function using combinator style.
    trace("expr", |input: &mut S| {
        input.parse_v1(|parser| parser.parse_expr())
    })
    .parse_next(input)
}

fn subexpr<S>(precedence: Precedence) -> impl ModalParser<S, Expr, ContextError>
where
    S: TokenStream,
{
    // TODO: implement this function using combinator style.
    trace("subexpr", move |input: &mut S| {
        input.parse_v1(|parser| parser.parse_subexpr(precedence))
    })
}

pub fn expr_case<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let parse = (
        opt(expr_parse),
        repeat(
            1..,
            (
                Keyword::WHEN,
                cut_err(expr_parse),
                cut_err(Keyword::THEN),
                cut_err(expr_parse),
            ),
        ),
        opt(preceded(Keyword::ELSE, cut_err(expr_parse))),
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

/// Consume a SQL CAST function e.g. `CAST(expr AS FLOAT)`
pub fn expr_cast<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let parse = cut_err(seq! {Expr::Cast {
        _: Token::LParen,
        expr: expr_parse.map(Box::new),
        _: Keyword::AS,
        data_type: data_type,
        _: Token::RParen,
    }});

    trace("expr_cast", parse).parse_next(input)
}

/// Consume a SQL TRY_CAST function e.g. `TRY_CAST(expr AS FLOAT)`
pub fn expr_try_cast<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let parse = cut_err(seq! {Expr::TryCast {
        _: Token::LParen,
        expr: expr_parse.map(Box::new),
        _: Keyword::AS,
        data_type: data_type,
        _: Token::RParen,
    }});

    trace("expr_try_cast", parse).parse_next(input)
}

/// Consume a SQL EXTRACT function e.g. `EXTRACT(YEAR FROM expr)`
pub fn expr_extract<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let mut date_time_field = token
        .verify_map(|token| match token.token {
            Token::Word(w) => Some(w.value.to_uppercase()),
            Token::SingleQuotedString(s) => Some(s.to_uppercase()),
            _ => None,
        })
        .expect("date/time field");

    let parse = cut_err(seq! {Expr::Extract {
        _: Token::LParen,
        field: date_time_field,
        _: Keyword::FROM,
        expr: expr_parse.map(Box::new),
        _: Token::RParen,
    }});

    trace("expr_extract", parse).parse_next(input)
}

/// Consume `SUBSTRING (EXPR [FROM 1] [FOR 3])`
pub fn expr_substring<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let mut substring_from = opt(preceded(
        alt((Token::Comma.void(), Keyword::FROM.void())),
        cut_err(expr_parse).map(Box::new),
    ));
    let mut substring_for = opt(preceded(
        alt((Token::Comma.void(), Keyword::FOR.void())),
        cut_err(expr_parse).map(Box::new),
    ));
    let parse = cut_err(seq! {Expr::Substring {
        _: Token::LParen,
        expr: expr_parse.map(Box::new),
        substring_from: substring_from,
        substring_for: substring_for,
        _: Token::RParen,
    }});

    trace("expr_substring", parse).parse_next(input)
}

/// `POSITION(<expr> IN <expr>)`
pub fn expr_position<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let parse = cut_err(seq! {Expr::Position {
        _: Token::LParen,
        // Logically `parse_expr`, but limited to those with precedence higher than `BETWEEN`/`IN`,
        // to avoid conflict with general IN operator, for example `position(a IN (b) IN (c))`.
        // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/parser/gram.y#L16012
        substring: subexpr(Precedence::Between).map(Box::new),
        _: Keyword::IN,
        string: subexpr(Precedence::Between).map(Box::new),
        _: Token::RParen,
    }});

    trace("expr_position", parse).parse_next(input)
}

/// `OVERLAY(<expr> PLACING <expr> FROM <expr> [ FOR <expr> ])`
pub fn expr_overlay<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    let mut count_parse = opt(preceded(
        Keyword::FOR.void(),
        cut_err(expr_parse).map(Box::new),
    ));

    let parse = cut_err(seq! {Expr::Overlay {
        _: Token::LParen,
        expr: expr_parse.map(Box::new),
        _: Keyword::PLACING,
        new_substring: expr_parse.map(Box::new),
        _: Keyword::FROM,
        start: expr_parse.map(Box::new),
        count: count_parse,
        _: Token::RParen,
    }});

    trace("expr_overlay", parse).parse_next(input)
}
