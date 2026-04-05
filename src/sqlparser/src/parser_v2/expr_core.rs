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

//! Core expression parser using winnow's Pratt parser combinator.
//!
//! This module provides a pure parser_v2 implementation that does NOT
//! fall back to v1 parser. It's designed as a foundation for gradually
//! migrating expression parsing to combinator style.

// TODO: Remove this once there are call sites outside of tests.
#![allow(dead_code)]

use winnow::combinator::{Infix, Postfix, Prefix, alt, cut_err, expression, fail, trace};
use winnow::error::{ContextError, ErrMode};
use winnow::{ModalResult, Parser};

use super::{TokenStream, keyword, token};
use crate::ast::{BinaryOperator, Expr, UnaryOperator, Value};
use crate::keywords::Keyword;
use crate::tokenizer::{Token, TokenWithLocation};

/// Binding power for prefix operators (unary +, -)
const PREFIX_POWER: i64 = 25;

/// Binding power for logical OR (lowest precedence)
const OR_POWER: i64 = 5;

/// Binding power for logical AND
const AND_POWER: i64 = 7;

/// Binding power for comparison operators (=, <>, <, >, <=, >=)
const CMP_POWER: i64 = 11;

/// Binding power for addition/subtraction
const ADD_POWER: i64 = 15;

/// Binding power for multiplication/division/modulo
const MUL_POWER: i64 = 17;

/// Binding power for exponentiation (^, right associative)
const EXP_POWER: i64 = 19;

/// Binding power for LIKE/ILIKE (between comparisons and IS)
const LIKE_POWER: i64 = 12;

/// Binding power for IS NULL / IS NOT NULL (between LIKE and AND)
const IS_NULL_POWER: i64 = 10;

/// Parse a core expression.
///
/// This is a pure parser_v2 implementation using winnow's `expression` combinator.
/// It does NOT fall back to v1 parser.
///
/// Supported operators (in precedence order, high to low):
/// - unary `+`, `-` (highest)
/// - `^` (right associative)
/// - `*`, `/`, `%`
/// - `+`, `-`
/// - `=`, `<>`, `<`, `>`, `<=`, `>=`
/// - `LIKE`, `ILIKE`
/// - `AND`
/// - `OR` (lowest)
pub fn expr_core<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace("expr_core", |input: &mut S| {
        expression(atom_core)
            .prefix(prefix_op_core)
            .postfix(postfix_op_core)
            .infix(infix_op_core)
            .parse_next(input)
    })
    .parse_next(input)
}

/// Parse a core atomic expression (operand).
///
/// This only handles "safe" atoms that don't require v1 fallback:
/// - Literals (numbers, strings, booleans, NULL)
/// - Column references (simple identifiers)
/// - Parenthesized expressions
fn atom_core<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace(
        "atom_core",
        alt((expr_parenthesized_core, expr_literal, expr_identifier)),
    )
    .parse_next(input)
}

/// Parse a parenthesized expression.
/// Uses `expr_core` recursively.
fn expr_parenthesized_core<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace(
        "expr_parenthesized_core",
        (Token::LParen, cut_err(expr_core), cut_err(Token::RParen)).map(|(_, e, _)| e),
    )
    .parse_next(input)
}

/// Parse prefix (unary) operators for core expressions.
/// Only handles `+` and `-`.
fn prefix_op_core<S>(input: &mut S) -> ModalResult<Prefix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    trace(
        "prefix_op_core",
        token.verify_map(|t: TokenWithLocation| match t.token {
            Token::Minus => Some(Prefix(PREFIX_POWER, |_, e: Expr| {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(e),
                })
            })),
            Token::Plus => Some(Prefix(PREFIX_POWER, |_, e: Expr| {
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(e),
                })
            })),
            _ => None,
        }),
    )
    .parse_next(input)
}

/// Parse postfix operators for core expressions.
/// Handles: IS \[NOT\] NULL, IS \[NOT\] TRUE, IS \[NOT\] FALSE, IS \[NOT\] UNKNOWN
fn postfix_op_core<S>(input: &mut S) -> ModalResult<Postfix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    trace(
        "postfix_op_core",
        token.verify_map(|t: TokenWithLocation| match &t.token {
            Token::Word(w) if w.keyword == Keyword::IS => {
                Some(Postfix(IS_NULL_POWER, |input: &mut S, expr: Expr| {
                    // Check for optional NOT
                    let is_not = (Keyword::NOT).parse_next(input).is_ok();

                    // Parse the target: NULL, TRUE, FALSE, or UNKNOWN
                    let target = keyword.parse_next(input)?;

                    match target {
                        Keyword::NULL => {
                            if is_not {
                                Ok(Expr::IsNotNull(Box::new(expr)))
                            } else {
                                Ok(Expr::IsNull(Box::new(expr)))
                            }
                        }
                        Keyword::TRUE => {
                            if is_not {
                                Ok(Expr::IsNotTrue(Box::new(expr)))
                            } else {
                                Ok(Expr::IsTrue(Box::new(expr)))
                            }
                        }
                        Keyword::FALSE => {
                            if is_not {
                                Ok(Expr::IsNotFalse(Box::new(expr)))
                            } else {
                                Ok(Expr::IsFalse(Box::new(expr)))
                            }
                        }
                        Keyword::UNKNOWN => {
                            if is_not {
                                Ok(Expr::IsNotUnknown(Box::new(expr)))
                            } else {
                                Ok(Expr::IsUnknown(Box::new(expr)))
                            }
                        }
                        _ => fail.parse_next(input),
                    }
                }))
            }
            _ => None,
        }),
    )
    .parse_next(input)
}

/// Parse infix (binary) operators for core expressions.
///
/// Handles: OR, AND, comparisons, +, -, *, /, %, ^
fn infix_op_core<S>(input: &mut S) -> ModalResult<Infix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    trace(
        "infix_op_core",
        token.verify_map(|t: TokenWithLocation| match &t.token {
            // Logical OR (lowest precedence, left associative)
            Token::Word(w) if w.keyword == Keyword::OR => {
                Some(Infix::Left(OR_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::BinaryOp {
                        left: Box::new(a),
                        op: BinaryOperator::Or,
                        right: Box::new(b),
                    })
                }))
            }
            // Logical AND (left associative)
            Token::Word(w) if w.keyword == Keyword::AND => {
                Some(Infix::Left(AND_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::BinaryOp {
                        left: Box::new(a),
                        op: BinaryOperator::And,
                        right: Box::new(b),
                    })
                }))
            }
            // Comparison operators (neither associative - chains like a = b = c are invalid)
            Token::Eq => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Eq,
                    right: Box::new(b),
                })
            })),
            Token::Neq => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::NotEq,
                    right: Box::new(b),
                })
            })),
            Token::Lt => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Lt,
                    right: Box::new(b),
                })
            })),
            Token::Gt => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Gt,
                    right: Box::new(b),
                })
            })),
            Token::LtEq => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::LtEq,
                    right: Box::new(b),
                })
            })),
            Token::GtEq => Some(Infix::Neither(CMP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::GtEq,
                    right: Box::new(b),
                })
            })),
            // LIKE / ILIKE (left associative, lower precedence than comparisons)
            // These construct Expr::Like / Expr::ILike, not BinaryOp
            Token::Word(w) if w.keyword == Keyword::LIKE => {
                Some(Infix::Left(LIKE_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::Like {
                        negated: false,
                        expr: Box::new(a),
                        pattern: Box::new(b),
                        escape_char: None,
                    })
                }))
            }
            Token::Word(w) if w.keyword == Keyword::ILIKE => {
                Some(Infix::Left(LIKE_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::ILike {
                        negated: false,
                        expr: Box::new(a),
                        pattern: Box::new(b),
                        escape_char: None,
                    })
                }))
            }
            // Addition/subtraction (left associative)
            Token::Plus => Some(Infix::Left(ADD_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Plus,
                    right: Box::new(b),
                })
            })),
            Token::Minus => Some(Infix::Left(ADD_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Minus,
                    right: Box::new(b),
                })
            })),
            // Multiplication/division/modulo (left associative)
            Token::Mul => Some(Infix::Left(MUL_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Multiply,
                    right: Box::new(b),
                })
            })),
            Token::Div => Some(Infix::Left(MUL_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Divide,
                    right: Box::new(b),
                })
            })),
            Token::Mod => Some(Infix::Left(MUL_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Modulo,
                    right: Box::new(b),
                })
            })),
            // Exponentiation (right associative)
            Token::Caret => Some(Infix::Right(EXP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Pow,
                    right: Box::new(b),
                })
            })),
            _ => None,
        }),
    )
    .parse_next(input)
}

/// Parse a literal value.
fn expr_literal<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace(
        "expr_literal",
        token.verify_map(|t: TokenWithLocation| match t.token {
            Token::Number(n) => Some(Expr::Value(Value::Number(n))),
            Token::SingleQuotedString(s) => Some(Expr::Value(Value::SingleQuotedString(s))),
            Token::NationalStringLiteral(s) => Some(Expr::Value(Value::NationalStringLiteral(s))),
            Token::HexStringLiteral(s) => Some(Expr::Value(Value::HexStringLiteral(s))),
            Token::DollarQuotedString(s) => Some(Expr::Value(Value::DollarQuotedString(s))),
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Some(Expr::Value(Value::Boolean(true))),
                Keyword::FALSE => Some(Expr::Value(Value::Boolean(false))),
                Keyword::NULL => Some(Expr::Value(Value::Null)),
                _ => None,
            },
            _ => None,
        }),
    )
    .parse_next(input)
}

/// Parse an identifier expression.
/// Handles:
/// - Simple identifiers: `foo`
/// - Quoted identifiers: `"CaseSensitive"`
/// - Compound identifiers: `foo.bar`, `schema.table.col`
fn expr_identifier<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace("expr_identifier", |input: &mut S| {
        // Parse the first identifier part
        let first = parse_single_ident(input)?;

        // Check for compound identifier (dot-separated parts)
        let mut parts = vec![first];

        // Use `repeat` combinator for zero or more `.ident` sequences
        use winnow::combinator::repeat;
        let additional: Vec<_> = repeat(
            0..,
            (Token::Period, parse_single_ident).map(|(_, ident)| ident),
        )
        .parse_next(input)?;

        parts.extend(additional);

        if parts.len() == 1 {
            Ok(Expr::Identifier(parts.into_iter().next().unwrap()))
        } else {
            Ok(Expr::CompoundIdentifier(parts))
        }
    })
    .parse_next(input)
}

/// Parse a single identifier (simple or quoted).
/// Accepts both keywords and non-keywords (like the v1 parser).
fn parse_single_ident<S>(input: &mut S) -> ModalResult<crate::ast::Ident>
where
    S: TokenStream,
{
    token
        .verify_map(|t: TokenWithLocation| match t.token {
            // Any Word token can be an identifier (keyword or not)
            Token::Word(w) => {
                match w.quote_style {
                    // Quoted identifier (e.g., "CaseSensitive")
                    Some(quote) => Some(crate::ast::Ident::with_quote_unchecked(quote, w.value)),
                    // Simple unquoted identifier (may be a keyword like TABLE)
                    None => Some(crate::ast::Ident::new_unchecked(w.value)),
                }
            }
            _ => None,
        })
        .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokenizer::Tokenizer;

    fn parse_expr_core(sql: &str) -> Result<Expr, String> {
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer
            .tokenize_with_location()
            .map_err(|e| e.to_string())?;
        let mut parser = crate::parser::Parser(&tokens);
        let result = expr_core
            .parse_next(&mut parser)
            .map_err(|e| e.to_string())?;

        // Ensure all tokens were consumed (except EOF)
        // The parser should have consumed the entire expression
        // We check by verifying the remaining tokens are just EOF
        match parser.0.first() {
            None
            | Some(crate::tokenizer::TokenWithLocation {
                token: crate::tokenizer::Token::EOF,
                ..
            }) => (),
            Some(t) => return Err(format!("Unexpected trailing token: {:?}", t)),
        }

        Ok(result)
    }

    #[test]
    fn test_literal_number() {
        let result = parse_expr_core("42").unwrap();
        assert!(matches!(result, Expr::Value(Value::Number(n)) if n == "42"));
    }

    #[test]
    fn test_literal_string() {
        let result = parse_expr_core("'hello'").unwrap();
        assert!(matches!(result, Expr::Value(Value::SingleQuotedString(s)) if s == "hello"));
    }

    #[test]
    fn test_literal_boolean() {
        let result = parse_expr_core("true").unwrap();
        assert!(matches!(result, Expr::Value(Value::Boolean(true))));
    }

    #[test]
    fn test_identifier() {
        let result = parse_expr_core("foo").unwrap();
        assert!(matches!(result, Expr::Identifier(_)));
    }

    #[test]
    fn test_unary_minus() {
        let result = parse_expr_core("-42").unwrap();
        assert!(matches!(
            result,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                ..
            }
        ));
    }

    #[test]
    fn test_binary_add() {
        let result = parse_expr_core("1 + 2").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::Plus,
                ..
            }
        ));
    }

    #[test]
    fn test_precedence_mul_before_add() {
        // Should parse as 1 + (2 * 3), not (1 + 2) * 3
        let result = parse_expr_core("1 + 2 * 3").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Plus,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Value(Value::Number(n)) if n == "1"));
            assert!(matches!(
                right.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Multiply,
                    ..
                }
            ));
        } else {
            panic!("Expected Plus at top level");
        }
    }

    #[test]
    fn test_comparison() {
        let result = parse_expr_core("a = 1").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::Eq,
                ..
            }
        ));
    }

    #[test]
    fn test_logical_and() {
        let result = parse_expr_core("a AND b").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::And,
                ..
            }
        ));
    }

    #[test]
    fn test_logical_or() {
        let result = parse_expr_core("a OR b").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::Or,
                ..
            }
        ));
    }

    #[test]
    fn test_parenthesized() {
        let result = parse_expr_core("(1 + 2) * 3").unwrap();
        // Should parse as (1 + 2) * 3, not 1 + (2 * 3)
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Multiply,
            ..
        } = result
        {
            assert!(matches!(
                left.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Plus,
                    ..
                }
            ));
        } else {
            panic!("Expected Multiply at top level");
        }
    }

    #[test]
    fn test_complex_expr() {
        // Test a complex expression with multiple operators
        let result = parse_expr_core("a + b * c = d AND e > f").unwrap();
        // Should be: ((a + (b * c)) = d) AND (e > f)
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::And,
                ..
            }
        ));
    }

    #[test]
    fn test_like() {
        let result = parse_expr_core("a LIKE 'x'").unwrap();
        assert!(matches!(result, Expr::Like { negated: false, .. }));
    }

    #[test]
    fn test_ilike() {
        let result = parse_expr_core("a ILIKE 'x'").unwrap();
        assert!(matches!(result, Expr::ILike { negated: false, .. }));
    }

    #[test]
    fn test_like_precedence_with_and() {
        // Should parse as: (a LIKE 'x') AND b
        // LIKE has lower precedence than comparison, higher than AND
        let result = parse_expr_core("a LIKE 'x' AND b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Like { .. }));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected And at top level");
        }
    }

    #[test]
    fn test_like_with_arithmetic() {
        // Should parse as: (a + 1) LIKE 'x'
        // Arithmetic has higher precedence than LIKE
        let result = parse_expr_core("a + 1 LIKE 'x'").unwrap();
        if let Expr::Like { expr, .. } = result {
            assert!(matches!(
                expr.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Plus,
                    ..
                }
            ));
        } else {
            panic!("Expected Like at top level");
        }
    }

    #[test]
    fn test_like_parenthesized_with_or() {
        // Should parse as: (a LIKE 'x') OR b
        let result = parse_expr_core("(a LIKE 'x') OR b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Like { .. }));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected Or at top level");
        }
    }

    #[test]
    fn test_compound_identifier() {
        let result = parse_expr_core("foo.bar").unwrap();
        if let Expr::CompoundIdentifier(parts) = result {
            assert_eq!(parts.len(), 2);
        } else {
            panic!("Expected CompoundIdentifier");
        }
    }

    #[test]
    fn test_compound_identifier_three_parts() {
        let result = parse_expr_core("schema.table.col").unwrap();
        if let Expr::CompoundIdentifier(parts) = result {
            assert_eq!(parts.len(), 3);
        } else {
            panic!("Expected CompoundIdentifier with 3 parts");
        }
    }

    #[test]
    fn test_quoted_identifier() {
        let result = parse_expr_core("\"CaseSensitive\"").unwrap();
        if let Expr::Identifier(ident) = result {
            // The identifier should have quote_style set
            assert_eq!(ident.quote_style(), Some('"'));
            assert_eq!(ident.real_value(), "CaseSensitive");
        } else {
            panic!("Expected Identifier");
        }
    }

    #[test]
    fn test_compound_identifier_with_expression() {
        // foo.bar + 1 should parse as (foo.bar) + 1
        let result = parse_expr_core("foo.bar + 1").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Plus,
            ..
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::CompoundIdentifier(_)));
        } else {
            panic!("Expected Plus at top level");
        }
    }

    #[test]
    fn test_compound_identifier_with_like() {
        // schema.tbl.col ilike 'x'
        let result = parse_expr_core("schema.tbl.col ilike 'x'").unwrap();
        if let Expr::ILike { expr, .. } = result {
            assert!(matches!(expr.as_ref(), Expr::CompoundIdentifier(_)));
        } else {
            panic!("Expected ILike at top level");
        }
    }

    #[test]
    fn test_is_null() {
        let result = parse_expr_core("a IS NULL").unwrap();
        assert!(matches!(result, Expr::IsNull(_)));
    }

    #[test]
    fn test_is_not_null() {
        let result = parse_expr_core("a IS NOT NULL").unwrap();
        assert!(matches!(result, Expr::IsNotNull(_)));
    }

    #[test]
    fn test_is_null_precedence_with_and() {
        // a IS NULL AND b should parse as (a IS NULL) AND b
        let result = parse_expr_core("a IS NULL AND b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::IsNull(_)));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected And at top level");
        }
    }

    #[test]
    fn test_is_null_with_arithmetic() {
        // a + 1 IS NULL should parse as (a + 1) IS NULL
        let result = parse_expr_core("a + 1 IS NULL").unwrap();
        if let Expr::IsNull(expr) = result {
            assert!(matches!(
                expr.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Plus,
                    ..
                }
            ));
        } else {
            panic!("Expected IsNull at top level");
        }
    }

    #[test]
    fn test_is_null_parenthesized_with_or() {
        // (a IS NULL) OR b should parse correctly
        let result = parse_expr_core("(a IS NULL) OR b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::IsNull(_)));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected Or at top level");
        }
    }

    #[test]
    fn test_is_true() {
        let result = parse_expr_core("a IS TRUE").unwrap();
        assert!(matches!(result, Expr::IsTrue(_)));
    }

    #[test]
    fn test_is_not_true() {
        let result = parse_expr_core("a IS NOT TRUE").unwrap();
        assert!(matches!(result, Expr::IsNotTrue(_)));
    }

    #[test]
    fn test_is_false() {
        let result = parse_expr_core("a IS FALSE").unwrap();
        assert!(matches!(result, Expr::IsFalse(_)));
    }

    #[test]
    fn test_is_not_false() {
        let result = parse_expr_core("a IS NOT FALSE").unwrap();
        assert!(matches!(result, Expr::IsNotFalse(_)));
    }

    #[test]
    fn test_is_unknown() {
        let result = parse_expr_core("a IS UNKNOWN").unwrap();
        assert!(matches!(result, Expr::IsUnknown(_)));
    }

    #[test]
    fn test_is_not_unknown() {
        let result = parse_expr_core("a IS NOT UNKNOWN").unwrap();
        assert!(matches!(result, Expr::IsNotUnknown(_)));
    }

    #[test]
    fn test_is_true_precedence_with_and() {
        // a IS TRUE AND b should parse as (a IS TRUE) AND b
        let result = parse_expr_core("a IS TRUE AND b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::IsTrue(_)));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected And at top level");
        }
    }

    #[test]
    fn test_is_false_precedence_with_or() {
        // a IS FALSE OR b should parse as (a IS FALSE) OR b
        let result = parse_expr_core("a IS FALSE OR b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::IsFalse(_)));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected Or at top level");
        }
    }
}
