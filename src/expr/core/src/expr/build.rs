// Copyright 2025 RisingWave Labs
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
use risingwave_expr::expr::LogReport;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{PbType, RexNode};

use super::NonStrictExpression;
use super::expr_some_all::SomeAllExpression;
use super::expr_udf::UserDefinedFunction;
use super::strict::Strict;
use super::wrapper::EvalErrorReport;
use super::wrapper::checked::Checked;
use super::wrapper::non_strict::NonStrict;
use crate::expr::{
    BoxedExpression, Expression, ExpressionBoxExt, InputRefExpression, LiteralExpression,
};
use crate::expr_context::strict_mode;
use crate::sig::FUNCTION_REGISTRY;
use crate::{Result, bail};

/// Build an expression from protobuf.
pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    let expr = ExprBuilder::new_strict().build(prost)?;
    Ok(Strict::new(expr).boxed())
}

/// Build an expression from protobuf in non-strict mode.
pub fn build_non_strict_from_prost(
    prost: &ExprNode,
    error_report: impl EvalErrorReport + 'static,
) -> Result<NonStrictExpression> {
    ExprBuilder::new_non_strict(error_report)
        .build(prost)
        .map(NonStrictExpression)
}

/// Build a strict or non-strict expression according to expr context.
///
/// When strict mode is off, the expression will not fail but leave a null value as result.
///
/// Unlike [`build_non_strict_from_prost`], the returning value here can be either non-strict or
/// strict. Thus, the caller is supposed to handle potential errors under strict mode.
pub fn build_batch_expr_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    if strict_mode()? {
        build_from_prost(prost)
    } else {
        // TODO(eric): report errors to users via psql notice
        Ok(ExprBuilder::new_non_strict(LogReport).build(prost)?.boxed())
    }
}

/// Build an expression from protobuf with possibly some wrappers attached to each node.
struct ExprBuilder<R> {
    /// The error reporting for non-strict mode.
    ///
    /// If set, each expression node will be wrapped with a [`NonStrict`] node that reports
    /// errors to this error reporting.
    error_report: Option<R>,
}

impl ExprBuilder<!> {
    /// Create a new builder in strict mode.
    fn new_strict() -> Self {
        Self { error_report: None }
    }
}

impl<R> ExprBuilder<R>
where
    R: EvalErrorReport + 'static,
{
    /// Create a new builder in non-strict mode with the given error reporting.
    fn new_non_strict(error_report: R) -> Self {
        Self {
            error_report: Some(error_report),
        }
    }

    /// Attach wrappers to an expression.
    #[expect(clippy::let_and_return)]
    fn wrap(&self, expr: impl Expression + 'static) -> BoxedExpression {
        let checked = Checked(expr);

        let may_non_strict = if let Some(error_report) = &self.error_report {
            NonStrict::new(checked, error_report.clone()).boxed()
        } else {
            checked.boxed()
        };

        may_non_strict
    }

    /// Build an expression with `build_inner` and attach some wrappers.
    fn build(&self, prost: &ExprNode) -> Result<BoxedExpression> {
        let expr = self.build_inner(prost)?;
        Ok(self.wrap(expr))
    }

    /// Build an expression from protobuf.
    fn build_inner(&self, prost: &ExprNode) -> Result<BoxedExpression> {
        use PbType as E;

        let build_child = |prost: &'_ ExprNode| self.build(prost);

        match prost.get_rex_node()? {
            RexNode::InputRef(_) => InputRefExpression::build_boxed(prost, build_child),
            RexNode::Constant(_) => LiteralExpression::build_boxed(prost, build_child),
            RexNode::Udf(_) => UserDefinedFunction::build_boxed(prost, build_child),

            RexNode::FuncCall(_) => match prost.function_type() {
                // Dedicated types
                E::All | E::Some => SomeAllExpression::build_boxed(prost, build_child),

                // General types, lookup in the function signature map
                _ => FuncCallBuilder::build_boxed(prost, build_child),
            },

            RexNode::Now(_) => unreachable!("now should not be built at backend"),
        }
    }
}

/// Manually build the expression `Self` from protobuf.
pub(crate) trait Build: Expression + Sized {
    /// Build the expression `Self` from protobuf.
    ///
    /// To build children, call `build_child` on each child instead of [`build_from_prost`].
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self>;

    /// Build the expression `Self` from protobuf for test, where each child is built with
    /// [`build_from_prost`].
    #[cfg(test)]
    fn build_for_test(prost: &ExprNode) -> Result<Self> {
        Self::build(prost, build_from_prost)
    }
}

/// Manually build a boxed expression from protobuf.
pub(crate) trait BuildBoxed: 'static {
    /// Build a boxed expression from protobuf.
    fn build_boxed(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<BoxedExpression>;
}

/// Implement [`BuildBoxed`] for all expressions that implement [`Build`].
impl<E: Build + 'static> BuildBoxed for E {
    fn build_boxed(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        Self::build(prost, build_child).map(ExpressionBoxExt::boxed)
    }
}

/// Build a function call expression from protobuf with [`build_func`].
struct FuncCallBuilder;

impl BuildBoxed for FuncCallBuilder {
    fn build_boxed(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<BoxedExpression> {
        let func_type = prost.function_type();
        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let func_call = prost
            .get_rex_node()?
            .as_func_call()
            .expect("not a func call");

        let children = func_call
            .get_children()
            .iter()
            .map(build_child)
            .try_collect()?;

        build_func(func_type, ret_type, children)
    }
}

/// Build an expression in `FuncCall` variant.
pub fn build_func(
    func: PbType,
    ret_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let args = children.iter().map(|c| c.return_type()).collect_vec();
    let desc = FUNCTION_REGISTRY.get(func, &args, &ret_type)?;
    desc.build_scalar(ret_type, children)
}

/// Build an expression in `FuncCall` variant in non-strict mode.
///
/// Note: This is a workaround, and only the root node are wrappedin non-strict mode.
/// Prefer [`build_non_strict_from_prost`] if possible.
pub fn build_func_non_strict(
    func: PbType,
    ret_type: DataType,
    children: Vec<BoxedExpression>,
    error_report: impl EvalErrorReport + 'static,
) -> Result<NonStrictExpression> {
    let expr = build_func(func, ret_type, children)?;
    let wrapped = NonStrictExpression(ExprBuilder::new_non_strict(error_report).wrap(expr));

    Ok(wrapped)
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
///
/// ```ignore
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
                    "null" | "NULL" => None,
                    _ => Some(ScalarImpl::from_text(&value, &ty).expect_str("value", &value)),
                };
                LiteralExpression::new(ty, value).boxed()
            }
            _ => panic!("Unexpected token"),
        }
    }

    fn parse_type(&mut self) -> DataType {
        match self.tokens.next().expect("Unexpected end of input") {
            Token::Literal(name) => {
                let mut processed_name = name.replace('_', " ");

                if processed_name.starts_with("map") {
                    processed_name = processed_name.replace('<', "(").replace('>', ")");
                }

                processed_name
                    .parse::<DataType>()
                    .expect_str("type", &processed_name)
            }
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
                while let Some(c) = chars.peek()
                    && c.is_ascii_digit()
                {
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
