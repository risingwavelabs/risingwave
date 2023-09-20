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
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::expr::expr_node::{PbType, RexNode};
use risingwave_pb::expr::ExprNode;

use super::expr_array_transform::ArrayTransformExpression;
use super::expr_case::CaseExpression;
use super::expr_coalesce::CoalesceExpression;
use super::expr_field::FieldExpression;
use super::expr_in::InExpression;
use super::expr_some_all::SomeAllExpression;
use super::expr_udf::UdfExpression;
use super::expr_vnode::VnodeExpression;
use super::wrapper::{Checked, EvalErrorReport, NonStrict};
use crate::expr::{BoxedExpression, Expression, InputRefExpression, LiteralExpression};
use crate::sig::func::FUNC_SIG_MAP;
use crate::sig::FuncSigDebug;
use crate::{bail, ExprError, Result};

/// Build an expression from protobuf.
pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    ExprBuilder::new().build(prost)
}

/// Build an expression from protobuf in non-strict mode.
pub fn build_non_strict_from_prost(
    prost: &ExprNode,
    error_report: impl EvalErrorReport + 'static,
) -> Result<BoxedExpression> {
    ExprBuilder::new_non_strict(Arc::new(error_report)).build(prost)
}

pub(crate) trait Build: Expression + Sized + 'static {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self>;

    fn build_boxed(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        Self::build(prost, build_child).map(Expression::boxed)
    }

    fn build_for_test(prost: &ExprNode) -> Result<Self> {
        Self::build(prost, build_from_prost)
    }
}

struct ExprBuilder<R> {
    error_report: Option<R>,
}

impl ExprBuilder<!> {
    fn new() -> Self {
        Self { error_report: None }
    }
}

impl<R> ExprBuilder<R>
where
    R: EvalErrorReport + Clone + 'static,
{
    fn new_non_strict(error_report: R) -> Self {
        Self {
            error_report: Some(error_report),
        }
    }

    /// Build an expression with `build_inner` and attach some wrappers.
    fn build(&self, prost: &ExprNode) -> Result<BoxedExpression> {
        let expr = self.build_inner(prost)?;

        let checked = Checked(expr);

        let may_non_strict = if let Some(error_report) = &self.error_report {
            NonStrict::new(checked, error_report.clone()).boxed()
        } else {
            checked.boxed()
        };

        Ok(may_non_strict)
    }

    /// Do build an expression from protobuf.
    fn build_inner(&self, prost: &ExprNode) -> Result<BoxedExpression> {
        let build_child = |prost: &'_ ExprNode| self.build(prost);

        use PbType as E;

        let func_call = match prost.get_rex_node()? {
            RexNode::InputRef(_) => return InputRefExpression::build_boxed(prost, build_child),
            RexNode::Constant(_) => return LiteralExpression::build_boxed(prost, build_child),
            RexNode::Udf(_) => return UdfExpression::build_boxed(prost, build_child),
            RexNode::FuncCall(func_call) => func_call,
            RexNode::Now(_) => unreachable!("now should not be built at backend"),
        };

        let func_type = prost.function_type();

        match func_type {
            // Dedicated types
            E::All | E::Some => SomeAllExpression::build_boxed(prost, build_child),
            E::In => InExpression::build_boxed(prost, build_child),
            E::Case => CaseExpression::build_boxed(prost, build_child),
            E::Coalesce => CoalesceExpression::build_boxed(prost, build_child),
            E::Field => FieldExpression::build_boxed(prost, build_child),
            E::Vnode => VnodeExpression::build_boxed(prost, build_child),

            _ => {
                let ret_type = DataType::from(prost.get_return_type().unwrap());
                let children = func_call
                    .get_children()
                    .iter()
                    .map(build_child)
                    .try_collect()?;

                build_func(func_type, ret_type, children)
            }
        }
    }
}

/// Build an expression in `FuncCall` variant.
pub fn build_func(
    func: PbType,
    ret_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    if func == PbType::ArrayTransform {
        // TODO: The function framework can't handle the lambda arg now.
        let [array, lambda] = <[BoxedExpression; 2]>::try_from(children).unwrap();
        return Ok(ArrayTransformExpression { array, lambda }.boxed());
    }

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
                    deprecated: false,
                    append_only: false,
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
pub(crate) enum Token {
    LParen,
    RParen,
    Colon,
    Index(usize),
    Literal(String),
}

pub(crate) fn lexer(input: &str) -> Vec<Token> {
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

pub(crate) trait ExpectExt<T> {
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
