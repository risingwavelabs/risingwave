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
use winnow::combinator::{cut_err, opt, preceded, repeat, trace};
use winnow::{PResult, Parser};

use super::TokenStream;
use crate::ast::Expr;
use crate::keywords::Keyword;

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
