// Copyright 2023 RisingWave Labs
//
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

use std::iter::Peekable;

use itertools::Itertools;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::expr::expr_node::{PbType, RexNode};
use risingwave_pb::expr::{ExprNode, FunctionCall};

use super::expr_array_concat::ArrayConcatExpression;
use super::expr_case::CaseExpression;
use super::expr_coalesce::CoalesceExpression;
use super::expr_concat_ws::ConcatWsExpression;
use super::expr_field::FieldExpression;
use super::expr_in::InExpression;
use super::expr_nested_construct::NestedConstructExpression;
use super::expr_regexp::RegexpMatchExpression;
use super::expr_some_all::SomeAllExpression;
use super::expr_udf::UdfExpression;
use super::expr_vnode::VnodeExpression;
use crate::expr::expr_proctime::ProcTimeExpression;
use crate::expr::{
    BoxedExpression, Expression, InputRefExpression, LiteralExpression, TryFromExprNodeBoxed,
};
use crate::sig::func::FUNC_SIG_MAP;
use crate::sig::FuncSigDebug;
use crate::{bail, ExprError, Result};

/// Build an expression from protobuf.
pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    use PbType as E;

    let binding = FunctionCall { children: vec![] };
    let func_call = match prost.get_rex_node()? {
        RexNode::InputRef(_) => return InputRefExpression::try_from_boxed(prost),
        RexNode::Constant(_) => return LiteralExpression::try_from_boxed(prost),
        RexNode::Udf(_) => return UdfExpression::try_from_boxed(prost),
        RexNode::FuncCall(func_call) => func_call,
        RexNode::Now(_) => &binding,
    };

    let func_type = prost.function_type();

    match func_type {
        // Dedicated types
        E::All | E::Some => SomeAllExpression::try_from_boxed(prost),
        E::In => InExpression::try_from_boxed(prost),
        E::Case => CaseExpression::try_from_boxed(prost),
        E::Coalesce => CoalesceExpression::try_from_boxed(prost),
        E::ConcatWs => ConcatWsExpression::try_from_boxed(prost),
        E::Field => FieldExpression::try_from_boxed(prost),
        E::Array => NestedConstructExpression::try_from_boxed(prost),
        E::Row => NestedConstructExpression::try_from_boxed(prost),
        E::RegexpMatch => RegexpMatchExpression::try_from_boxed(prost),
        E::ArrayCat | E::ArrayAppend | E::ArrayPrepend => {
            // Now we implement these three functions as a single expression for the
            // sake of simplicity. If performance matters at some time, we can split
            // the implementation to improve performance.
            ArrayConcatExpression::try_from_boxed(prost)
        }
        E::Vnode => VnodeExpression::try_from_boxed(prost),
        E::Proctime => ProcTimeExpression::try_from_boxed(prost),

        _ => {
            let ret_type = DataType::from(prost.get_return_type().unwrap());
            let children = func_call
                .get_children()
                .iter()
                .map(build_from_prost)
                .try_collect()?;

            build_func(func_type, ret_type, children)
        }
    }
}

/// Build an expression in `FuncCall` variant.
pub fn build_func(
    func: PbType,
    ret_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let args = children
        .iter()
        .map(|c| c.return_type().into())
        .collect_vec();
    let desc = FUNC_SIG_MAP
        .get(func, &args, (&ret_type).into())
        .ok_or_else(|| {
            ExprError::UnsupportedFunction(format!(
                "{:?}",
                FuncSigDebug {
                    func: func.as_str_name(),
                    inputs_type: &args,
                    ret_type: (&ret_type).into(),
                    set_returning: false,
                }
            ))
        })?;
    (desc.build)(ret_type, children)
}

pub(super) fn get_children_and_return_type(prost: &ExprNode) -> Result<(&[ExprNode], DataType)> {
    let ret_type = DataType::from(prost.get_return_type().unwrap());
    if let RexNode::FuncCall(func_call) = prost.get_rex_node().unwrap() {
        Ok((func_call.get_children(), ret_type))
    } else {
        bail!("Expected RexNode::FuncCall");
    }
}

/// Build an expression from a string.
///
/// # Example
/// ```
/// # use risingwave_expr::expr::build_from_pretty;
/// build_from_pretty("42:int2"); // literal
/// build_from_pretty("$0:int8"); // inputref
/// build_from_pretty("(add:int8 42:int2 $1:int8)"); // function
/// build_from_pretty("(add:int8 42:int2 (add:int8 42:int2 $1:int8))");
/// ```
///
/// # Syntax
///
/// ```text
/// <expr>      ::= <literal> | <input_ref> | <function>
/// <literal>   ::= <value>:<type>
/// <input_ref> ::= <index>:<type>
/// <function>  ::= (<name>:<type> <expr>...)
/// <name>      ::= [a-zA-Z_][a-zA-Z0-9_]*
/// <index>     ::= $[0-9]+
/// ```
pub fn build_from_pretty(s: impl AsRef<str>) -> BoxedExpression {
    let tokens = lexer(s.as_ref());
    Parser::new(tokens.into_iter()).parse_expression()
}

struct Parser<Iter: Iterator> {
    tokens: Peekable<Iter>,
}

impl<Iter: Iterator<Item = Token>> Parser<Iter> {
    fn new(tokens: Iter) -> Self {
        Self {
            tokens: tokens.peekable(),
        }
    }

    fn parse_expression(&mut self) -> BoxedExpression {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Index(index) => {
                assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
                let ty = self.parse_type();
                InputRefExpression::new(ty, index).boxed()
            }
            Token::LParen => {
                let func = self.parse_function();
                assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
                let ty = self.parse_type();

                let mut children = Vec::new();
                while self.tokens.peek() != Some(&Token::RParen) {
                    children.push(self.parse_expression());
                }
                self.tokens.next(); // Consume the RParen

                build_func(func, ty, children).expect("Failed to build")
            }
            Token::Literal(value) => {
                assert_eq!(self.tokens.next(), Some(Token::Colon), "Expected a Colon");
                let ty = self.parse_type();
                let value = match value.as_str() {
                    "null" => None,
                    _ => Some(
                        ScalarImpl::from_text(value.as_bytes(), &ty).expect_str("value", &value),
                    ),
                };
                LiteralExpression::new(ty, value).boxed()
            }
            _ => panic!("Unexpected token"),
        }
    }

    fn parse_type(&mut self) -> DataType {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => name.parse::<DataType>().expect_str("type", &name),
            t => panic!("Expected a Literal, got {t:?}"),
        }
    }

    fn parse_function(&mut self) -> PbType {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => {
                PbType::from_str_name(&name.to_uppercase()).expect_str("function", &name)
            }
            t => panic!("Expected a Literal, got {t:?}"),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
enum Token {
    LParen,
    RParen,
    Colon,
    Index(usize),
    Literal(String),
}

fn lexer(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        let token = match c {
            '(' => Token::LParen,
            ')' => Token::RParen,
            ':' => Token::Colon,
            '$' => {
                let mut number = String::new();
                while let Some(c) = chars.peek() && c.is_ascii_digit() {
                    number.push(chars.next().unwrap());
                }
                let index = number.parse::<usize>().expect("Invalid number");
                Token::Index(index)
            }
            ' ' | '\t' | '\r' | '\n' => continue,
            _ => {
                let mut literal = String::new();
                literal.push(c);
                while let Some(&c) = chars.peek()
                    && !matches!(c, '(' | ')' | ':' | ' ' | '\t' | '\r' | '\n')
                {
                    literal.push(chars.next().unwrap());
                }
                Token::Literal(literal)
            }
        };
        tokens.push(token);
    }
    tokens
}

trait ExpectExt<T> {
    fn expect_str(self, what: &str, s: &str) -> T;
}

impl<T> ExpectExt<T> for Option<T> {
    #[track_caller]
    fn expect_str(self, what: &str, s: &str) -> T {
        match self {
            Some(x) => x,
            None => panic!("expect {what} in {s:?}"),
        }
    }
}

impl<T, E> ExpectExt<T> for std::result::Result<T, E> {
    #[track_caller]
    fn expect_str(self, what: &str, s: &str) -> T {
        match self {
            Ok(x) => x,
            Err(_) => panic!("expect {what} in {s:?}"),
        }
    }
}
