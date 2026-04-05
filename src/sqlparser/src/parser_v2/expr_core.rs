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
//! This module provides a v2 expression parser that uses winnow's `expression`
//! combinator for operator precedence parsing. Atoms (primary expressions
//! including prefix operators like NOT and unary +/-) are parsed via the v1
//! bridge, while all infix and postfix operators are handled natively by the
//! Pratt parser.

use winnow::combinator::{Infix, Postfix, alt, cut_err, expression, fail, trace};
use winnow::error::{ContextError, ErrMode};
use winnow::{ModalResult, Parser};

use super::TokenStream;
#[allow(unused_imports)] // Used implicitly via method dispatch on TokenStream
use super::compact::ParseV1;
use super::token;
use crate::ast::{BinaryOperator, Expr};
use crate::keywords::Keyword;
use crate::parser::Precedence;
use crate::tokenizer::{Token, TokenWithLocation};

// ============================================================
// Binding power constants
// ============================================================
// These maintain the same relative ordering as v1's Precedence enum.
// Higher value = tighter binding.

/// Logical OR
const OR_POWER: i64 = 5;

/// Logical XOR
const XOR_POWER: i64 = 6;

/// Logical AND
const AND_POWER: i64 = 7;

/// IS NULL/TRUE/FALSE/UNKNOWN, IS DISTINCT FROM, IS JSON, ISNULL, NOTNULL
const IS_POWER: i64 = 9;

/// Comparison operators (=, <>, <, >, <=, >=)
const CMP_POWER: i64 = 10;

/// LIKE, ILIKE, SIMILAR TO (and their NOT variants)
const LIKE_POWER: i64 = 11;

/// BETWEEN, IN (and their NOT variants)
const BETWEEN_POWER: i64 = 12;

/// Custom operators (Op, Pipe, OPERATOR, ALL/ANY/SOME)
const OTHER_POWER: i64 = 13;

/// Addition, subtraction
const ADD_POWER: i64 = 15;

/// Multiplication, division, modulo
const MUL_POWER: i64 = 17;

/// Exponentiation (^)
const EXP_POWER: i64 = 19;

/// AT TIME ZONE
const AT_POWER: i64 = 20;

/// Array subscript (\[\])
const ARRAY_POWER: i64 = 27;

/// PostgreSQL cast (::)
const DOUBLE_COLON_POWER: i64 = 30;

// ============================================================
// Main expression parser
// ============================================================

/// Parse an expression using the Pratt parser combinator.
///
/// Atoms (including prefix operators like NOT, unary +/-) are parsed via the
/// v1 bridge. All infix and postfix operators are handled natively by the
/// Pratt parser.
pub fn expr_core<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace("expr_core", |input: &mut S| {
        expression(atom_core)
            .prefix(fail)
            .postfix(postfix_op_core)
            .infix(infix_op_core)
            .parse_next(input)
    })
    .parse_next(input)
}

// ============================================================
// Atom parser (via v1 bridge)
// ============================================================

/// Parse an atomic expression by delegating to v1's `parse_prefix`.
///
/// This handles all primary expressions including:
/// - Literals (numbers, strings, booleans, NULL)
/// - Identifiers (simple, quoted, compound) with reserved keyword rejection
/// - Parenthesized expressions, subqueries, and row constructors
/// - Prefix operators (NOT, unary +/- with negative literal normalization, custom Op)
/// - Function calls
/// - Keyword expressions (CASE, CAST, EXISTS, ARRAY, etc.)
/// - Typed strings (DATE '...', TIMESTAMP '...', etc.)
/// - Parameters ($1), Lambda expressions (|args| body)
/// - COLLATE suffix on atoms
fn atom_core<S>(input: &mut S) -> ModalResult<Expr>
where
    S: TokenStream,
{
    trace("atom_core", |input: &mut S| {
        input.parse_v1(|parser| parser.parse_prefix())
    })
    .parse_next(input)
}

// ============================================================
// Postfix operators
// ============================================================

/// Parse postfix operators.
///
/// Handles: IS variants, NOT BETWEEN/IN/LIKE/ILIKE/SIMILAR TO, BETWEEN, IN,
/// AT TIME ZONE, :: cast, \[\] subscript, ISNULL, NOTNULL,
/// OPERATOR(...), custom Op, ALL/ANY/SOME (all via reset pattern where needed).
fn postfix_op_core<S>(input: &mut S) -> ModalResult<Postfix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    trace("postfix_op_core", |input: &mut S| {
        let checkpoint = input.checkpoint();
        let t = token.parse_next(input)?;

        match &t.token {
            Token::Word(w) => match w.keyword {
                // ---- IS [NOT] { NULL | TRUE | FALSE | UNKNOWN | DISTINCT FROM | JSON } ----
                Keyword::IS => Ok(Postfix(IS_POWER, parse_is_suffix)),

                // ---- ISNULL ----
                Keyword::ISNULL => Ok(Postfix(IS_POWER, |_: &mut S, expr: Expr| {
                    Ok(Expr::IsNull(Box::new(expr)))
                })),

                // ---- NOTNULL ----
                Keyword::NOTNULL => Ok(Postfix(IS_POWER, |_: &mut S, expr: Expr| {
                    Ok(Expr::IsNotNull(Box::new(expr)))
                })),

                // ---- NOT { BETWEEN | IN | LIKE | ILIKE | SIMILAR TO } ----
                Keyword::NOT => {
                    // Peek at the token after NOT to decide which compound operator.
                    match token.parse_next(input) {
                        Ok(t2) => match &t2.token {
                            Token::Word(w2) => match w2.keyword {
                                Keyword::BETWEEN => Ok(Postfix(
                                    BETWEEN_POWER,
                                    |input: &mut S, expr: Expr| {
                                        input.parse_v1(|p| p.parse_between(expr, true))
                                    },
                                )),
                                Keyword::IN => Ok(Postfix(
                                    BETWEEN_POWER,
                                    |input: &mut S, expr: Expr| {
                                        input.parse_v1(|p| p.parse_in(expr, true))
                                    },
                                )),
                                Keyword::LIKE => Ok(Postfix(
                                    LIKE_POWER,
                                    |input: &mut S, expr: Expr| {
                                        let pattern = input
                                            .parse_v1(|p| p.parse_subexpr(Precedence::Like))?;
                                        let escape = input.parse_v1(|p| p.parse_escape())?;
                                        Ok(Expr::Like {
                                            negated: true,
                                            expr: Box::new(expr),
                                            pattern: Box::new(pattern),
                                            escape_char: escape,
                                        })
                                    },
                                )),
                                Keyword::ILIKE => Ok(Postfix(
                                    LIKE_POWER,
                                    |input: &mut S, expr: Expr| {
                                        let pattern = input
                                            .parse_v1(|p| p.parse_subexpr(Precedence::Like))?;
                                        let escape = input.parse_v1(|p| p.parse_escape())?;
                                        Ok(Expr::ILike {
                                            negated: true,
                                            expr: Box::new(expr),
                                            pattern: Box::new(pattern),
                                            escape_char: escape,
                                        })
                                    },
                                )),
                                Keyword::SIMILAR => {
                                    // Consume TO after SIMILAR
                                    cut_err(Keyword::TO).parse_next(input)?;
                                    Ok(Postfix(
                                        LIKE_POWER,
                                        |input: &mut S, expr: Expr| {
                                            let pattern = input.parse_v1(|p| {
                                                p.parse_subexpr(Precedence::Like)
                                            })?;
                                            let escape =
                                                input.parse_v1(|p| p.parse_escape())?;
                                            Ok(Expr::SimilarTo {
                                                negated: true,
                                                expr: Box::new(expr),
                                                pattern: Box::new(pattern),
                                                escape_char: escape,
                                            })
                                        },
                                    ))
                                }
                                _ => {
                                    input.reset(&checkpoint);
                                    Err(ErrMode::Backtrack(ContextError::new()))
                                }
                            },
                            _ => {
                                input.reset(&checkpoint);
                                Err(ErrMode::Backtrack(ContextError::new()))
                            }
                        },
                        Err(_) => {
                            input.reset(&checkpoint);
                            Err(ErrMode::Backtrack(ContextError::new()))
                        }
                    }
                }

                // ---- BETWEEN ... AND ... ----
                Keyword::BETWEEN => Ok(Postfix(
                    BETWEEN_POWER,
                    |input: &mut S, expr: Expr| {
                        input.parse_v1(|p| p.parse_between(expr, false))
                    },
                )),

                // ---- IN (...) ----
                Keyword::IN => Ok(Postfix(BETWEEN_POWER, |input: &mut S, expr: Expr| {
                    input.parse_v1(|p| p.parse_in(expr, false))
                })),

                // ---- AT TIME ZONE ----
                Keyword::AT => {
                    // Must match AT TIME ZONE; reset if not.
                    if Keyword::TIME.parse_next(input).is_ok()
                        && Keyword::ZONE.parse_next(input).is_ok()
                    {
                        Ok(Postfix(AT_POWER, |input: &mut S, expr: Expr| {
                            let tz =
                                input.parse_v1(|p| p.parse_subexpr(Precedence::At))?;
                            Ok(Expr::AtTimeZone {
                                timestamp: Box::new(expr),
                                time_zone: Box::new(tz),
                            })
                        }))
                    } else {
                        input.reset(&checkpoint);
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }

                // ---- OPERATOR(...) binary (reset pattern) ----
                Keyword::OPERATOR => {
                    let has_lparen = Token::LParen.parse_next(input).is_ok();
                    // Reset to before OPERATOR so the fold function can re-consume.
                    input.reset(&checkpoint);
                    if has_lparen {
                        Ok(Postfix(OTHER_POWER, |input: &mut S, expr: Expr| {
                            input.parse_v1(|p| {
                                p.next_token(); // consume OPERATOR
                                let op = p.parse_qualified_operator()?;
                                let rhs = p.parse_subexpr(Precedence::Other)?;
                                Ok(Expr::BinaryOp {
                                    left: Box::new(expr),
                                    op: BinaryOperator::PGQualified(Box::new(op)),
                                    right: Box::new(rhs),
                                })
                            })
                        }))
                    } else {
                        Err(ErrMode::Backtrack(ContextError::new()))
                    }
                }

                // ---- ALL / ANY / SOME as postfix (reset pattern) ----
                Keyword::ALL | Keyword::ANY | Keyword::SOME => {
                    input.reset(&checkpoint);
                    Ok(Postfix(OTHER_POWER, |input: &mut S, _expr: Expr| {
                        input.parse_v1(|p| {
                            let tok = p.next_token();
                            let kw = match &tok.token {
                                Token::Word(w) => w.keyword,
                                _ => return fail.parse_next(p),
                            };
                            p.expect_token(&Token::LParen)?;
                            let sub = p.parse_expr()?;
                            p.expect_token(&Token::RParen)?;
                            Ok(match kw {
                                Keyword::ALL => Expr::AllOp(Box::new(sub)),
                                _ => Expr::SomeOp(Box::new(sub)),
                            })
                        })
                    }))
                }

                _ => {
                    input.reset(&checkpoint);
                    Err(ErrMode::Backtrack(ContextError::new()))
                }
            },

            // ---- :: (PostgreSQL cast) ----
            Token::DoubleColon => {
                Ok(Postfix(DOUBLE_COLON_POWER, |input: &mut S, expr: Expr| {
                    input.parse_v1(|p| p.parse_pg_cast(expr))
                }))
            }

            // ---- [] (array subscript / slice) ----
            Token::LBracket => {
                Ok(Postfix(ARRAY_POWER, |input: &mut S, expr: Expr| {
                    input.parse_v1(|p| p.parse_array_index(expr))
                }))
            }

            // ---- Custom operator (e.g., ||, @@) - reset pattern ----
            Token::Op(_) => {
                input.reset(&checkpoint);
                Ok(Postfix(OTHER_POWER, |input: &mut S, expr: Expr| {
                    input.parse_v1(|p| {
                        let tok = p.next_token();
                        let name = match tok.token {
                            Token::Op(name) => name,
                            _ => return fail.parse_next(p),
                        };
                        let rhs = p.parse_subexpr(Precedence::Other)?;
                        Ok(Expr::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::Custom(name),
                            right: Box::new(rhs),
                        })
                    })
                }))
            }

            _ => {
                input.reset(&checkpoint);
                Err(ErrMode::Backtrack(ContextError::new()))
            }
        }
    })
    .parse_next(input)
}

/// Parse the suffix after IS keyword.
///
/// Handles: IS \[NOT\] { NULL | TRUE | FALSE | UNKNOWN | DISTINCT FROM | JSON }
fn parse_is_suffix<S>(input: &mut S, expr: Expr) -> ModalResult<Expr>
where
    S: TokenStream,
{
    // Try non-negated forms first
    if Keyword::TRUE.parse_next(input).is_ok() {
        return Ok(Expr::IsTrue(Box::new(expr)));
    }
    if Keyword::FALSE.parse_next(input).is_ok() {
        return Ok(Expr::IsFalse(Box::new(expr)));
    }
    if Keyword::UNKNOWN.parse_next(input).is_ok() {
        return Ok(Expr::IsUnknown(Box::new(expr)));
    }
    if Keyword::NULL.parse_next(input).is_ok() {
        return Ok(Expr::IsNull(Box::new(expr)));
    }
    if Keyword::DISTINCT.parse_next(input).is_ok() {
        cut_err(Keyword::FROM).parse_next(input)?;
        let expr2 = input.parse_v1(|p| p.parse_expr())?;
        return Ok(Expr::IsDistinctFrom(Box::new(expr), Box::new(expr2)));
    }
    if Keyword::JSON.parse_next(input).is_ok() {
        return input.parse_v1(|p| p.parse_is_json(expr, false));
    }

    // Try negated forms (IS NOT ...)
    if Keyword::NOT.parse_next(input).is_ok() {
        if Keyword::TRUE.parse_next(input).is_ok() {
            return Ok(Expr::IsNotTrue(Box::new(expr)));
        }
        if Keyword::FALSE.parse_next(input).is_ok() {
            return Ok(Expr::IsNotFalse(Box::new(expr)));
        }
        if Keyword::UNKNOWN.parse_next(input).is_ok() {
            return Ok(Expr::IsNotUnknown(Box::new(expr)));
        }
        if Keyword::NULL.parse_next(input).is_ok() {
            return Ok(Expr::IsNotNull(Box::new(expr)));
        }
        if Keyword::DISTINCT.parse_next(input).is_ok() {
            cut_err(Keyword::FROM).parse_next(input)?;
            let expr2 = input.parse_v1(|p| p.parse_expr())?;
            return Ok(Expr::IsNotDistinctFrom(Box::new(expr), Box::new(expr2)));
        }
        if Keyword::JSON.parse_next(input).is_ok() {
            return input.parse_v1(|p| p.parse_is_json(expr, true));
        }
    }

    // Nothing matched after IS [NOT]
    fail.parse_next(input)
}

// ============================================================
// Infix operators
// ============================================================

/// Parse infix (binary) operators.
///
/// Handles: OR, XOR, AND, comparisons, arithmetic, LIKE, ILIKE, SIMILAR TO, Pipe.
fn infix_op_core<S>(input: &mut S) -> ModalResult<Infix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    trace(
        "infix_op_core",
        alt((
            // Multi-token infix: SIMILAR TO (must come before single-token)
            infix_similar_to,
            // Single-token infix operators
            infix_single_token,
        )),
    )
    .parse_next(input)
}

/// Parse SIMILAR TO as a multi-token infix operator.
fn infix_similar_to<S>(input: &mut S) -> ModalResult<Infix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    (Keyword::SIMILAR, Keyword::TO)
        .void()
        .parse_next(input)?;
    Ok(Infix::Left(
        LIKE_POWER,
        |input: &mut S, a: Expr, b: Expr| {
            let escape = input.parse_v1(|p| p.parse_escape())?;
            Ok(Expr::SimilarTo {
                negated: false,
                expr: Box::new(a),
                pattern: Box::new(b),
                escape_char: escape,
            })
        },
    ))
}

/// Parse single-token infix operators.
fn infix_single_token<S>(input: &mut S) -> ModalResult<Infix<S, Expr, ErrMode<ContextError>>>
where
    S: TokenStream,
{
    token
        .verify_map(|t: TokenWithLocation| match &t.token {
            // ---- Logical operators ----
            Token::Word(w) if w.keyword == Keyword::OR => {
                Some(Infix::Left(OR_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::BinaryOp {
                        left: Box::new(a),
                        op: BinaryOperator::Or,
                        right: Box::new(b),
                    })
                }))
            }
            Token::Word(w) if w.keyword == Keyword::XOR => {
                Some(Infix::Left(XOR_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::BinaryOp {
                        left: Box::new(a),
                        op: BinaryOperator::Xor,
                        right: Box::new(b),
                    })
                }))
            }
            Token::Word(w) if w.keyword == Keyword::AND => {
                Some(Infix::Left(AND_POWER, |_, a: Expr, b: Expr| {
                    Ok(Expr::BinaryOp {
                        left: Box::new(a),
                        op: BinaryOperator::And,
                        right: Box::new(b),
                    })
                }))
            }

            // ---- Comparison operators ----
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

            // ---- LIKE / ILIKE (with ESCAPE in fold fn) ----
            Token::Word(w) if w.keyword == Keyword::LIKE => {
                Some(Infix::Left(
                    LIKE_POWER,
                    |input: &mut S, a: Expr, b: Expr| {
                        let escape = input.parse_v1(|p| p.parse_escape())?;
                        Ok(Expr::Like {
                            negated: false,
                            expr: Box::new(a),
                            pattern: Box::new(b),
                            escape_char: escape,
                        })
                    },
                ))
            }
            Token::Word(w) if w.keyword == Keyword::ILIKE => {
                Some(Infix::Left(
                    LIKE_POWER,
                    |input: &mut S, a: Expr, b: Expr| {
                        let escape = input.parse_v1(|p| p.parse_escape())?;
                        Ok(Expr::ILike {
                            negated: false,
                            expr: Box::new(a),
                            pattern: Box::new(b),
                            escape_char: escape,
                        })
                    },
                ))
            }

            // ---- Arithmetic operators ----
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
            Token::Caret => Some(Infix::Right(EXP_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Pow,
                    right: Box::new(b),
                })
            })),

            // ---- Pipe (|) as custom binary operator ----
            Token::Pipe => Some(Infix::Left(OTHER_POWER, |_, a: Expr, b: Expr| {
                Ok(Expr::BinaryOp {
                    left: Box::new(a),
                    op: BinaryOperator::Custom("|".to_owned()),
                    right: Box::new(b),
                })
            })),

            _ => None,
        })
        .parse_next(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{UnaryOperator, Value};
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

    // ---- Literal tests ----

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

    // ---- Identifier tests ----

    #[test]
    fn test_identifier() {
        let result = parse_expr_core("foo").unwrap();
        assert!(matches!(result, Expr::Identifier(_)));
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
            assert_eq!(ident.quote_style(), Some('"'));
            assert_eq!(ident.real_value(), "CaseSensitive");
        } else {
            panic!("Expected Identifier");
        }
    }

    // ---- Unary operator tests ----

    #[test]
    fn test_unary_minus_literal() {
        // v1 normalizes -42 to Value(Number("-42"))
        let result = parse_expr_core("-42").unwrap();
        assert!(matches!(result, Expr::Value(Value::Number(n)) if n == "-42"));
    }

    #[test]
    fn test_unary_minus_identifier() {
        let result = parse_expr_core("-a").unwrap();
        assert!(matches!(
            result,
            Expr::UnaryOp {
                op: UnaryOperator::Minus,
                ..
            }
        ));
    }

    #[test]
    fn test_not_prefix() {
        let result = parse_expr_core("NOT a").unwrap();
        assert!(matches!(
            result,
            Expr::UnaryOp {
                op: UnaryOperator::Not,
                ..
            }
        ));
    }

    // ---- Binary operator tests ----

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
    fn test_logical_xor() {
        let result = parse_expr_core("a XOR b").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::Xor,
                ..
            }
        ));
    }

    #[test]
    fn test_parenthesized() {
        let result = parse_expr_core("(1 + 2) * 3").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Multiply,
            ..
        } = result
        {
            // v1 wraps parenthesized expressions in Expr::Nested
            assert!(matches!(left.as_ref(), Expr::Nested(_)));
        } else {
            panic!("Expected Multiply at top level");
        }
    }

    #[test]
    fn test_complex_expr() {
        let result = parse_expr_core("a + b * c = d AND e > f").unwrap();
        assert!(matches!(
            result,
            Expr::BinaryOp {
                op: BinaryOperator::And,
                ..
            }
        ));
    }

    // ---- LIKE / ILIKE tests ----

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
    fn test_not_like() {
        let result = parse_expr_core("a NOT LIKE 'x'").unwrap();
        assert!(matches!(result, Expr::Like { negated: true, .. }));
    }

    #[test]
    fn test_not_ilike() {
        let result = parse_expr_core("a NOT ILIKE 'x'").unwrap();
        assert!(matches!(result, Expr::ILike { negated: true, .. }));
    }

    #[test]
    fn test_like_escape() {
        let result = parse_expr_core("a LIKE 'x%' ESCAPE '\\'").unwrap();
        if let Expr::Like { escape_char, .. } = result {
            assert!(escape_char.is_some());
        } else {
            panic!("Expected Like");
        }
    }

    #[test]
    fn test_like_precedence_with_and() {
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
        let result = parse_expr_core("(a LIKE 'x') OR b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Nested(_)));
            assert!(matches!(right.as_ref(), Expr::Identifier(_)));
        } else {
            panic!("Expected Or at top level");
        }
    }

    // ---- SIMILAR TO tests ----

    #[test]
    fn test_similar_to() {
        let result = parse_expr_core("a SIMILAR TO 'x'").unwrap();
        assert!(matches!(
            result,
            Expr::SimilarTo {
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn test_not_similar_to() {
        let result = parse_expr_core("a NOT SIMILAR TO 'x'").unwrap();
        assert!(matches!(
            result,
            Expr::SimilarTo {
                negated: true,
                ..
            }
        ));
    }

    // ---- BETWEEN tests ----

    #[test]
    fn test_between() {
        let result = parse_expr_core("a BETWEEN 1 AND 10").unwrap();
        assert!(matches!(
            result,
            Expr::Between {
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn test_not_between() {
        let result = parse_expr_core("a NOT BETWEEN 1 AND 10").unwrap();
        assert!(matches!(
            result,
            Expr::Between {
                negated: true,
                ..
            }
        ));
    }

    // ---- IN tests ----

    #[test]
    fn test_in_list() {
        let result = parse_expr_core("a IN (1, 2, 3)").unwrap();
        assert!(matches!(
            result,
            Expr::InList {
                negated: false,
                ..
            }
        ));
    }

    #[test]
    fn test_not_in_list() {
        let result = parse_expr_core("a NOT IN (1, 2, 3)").unwrap();
        assert!(matches!(
            result,
            Expr::InList {
                negated: true,
                ..
            }
        ));
    }

    // ---- IS tests ----

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
        let result = parse_expr_core("(a IS NULL) OR b").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Nested(_)));
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

    // ---- IS DISTINCT FROM tests ----

    #[test]
    fn test_is_distinct_from() {
        let result = parse_expr_core("a IS DISTINCT FROM b").unwrap();
        assert!(matches!(result, Expr::IsDistinctFrom(_, _)));
    }

    #[test]
    fn test_is_not_distinct_from() {
        let result = parse_expr_core("a IS NOT DISTINCT FROM b").unwrap();
        assert!(matches!(result, Expr::IsNotDistinctFrom(_, _)));
    }

    // ---- AT TIME ZONE test ----

    #[test]
    fn test_at_time_zone() {
        let result = parse_expr_core("a AT TIME ZONE 'UTC'").unwrap();
        assert!(matches!(result, Expr::AtTimeZone { .. }));
    }

    // ---- :: cast test ----

    #[test]
    fn test_double_colon_cast() {
        let result = parse_expr_core("a::int").unwrap();
        assert!(matches!(result, Expr::Cast { .. }));
    }

    // ---- [] subscript test ----

    #[test]
    fn test_array_subscript() {
        let result = parse_expr_core("a[1]").unwrap();
        assert!(matches!(result, Expr::Index { .. }));
    }

    // ---- Compound expression tests ----

    #[test]
    fn test_compound_identifier_with_expression() {
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
        let result = parse_expr_core("schema.tbl.col ilike 'x'").unwrap();
        if let Expr::ILike { expr, .. } = result {
            assert!(matches!(expr.as_ref(), Expr::CompoundIdentifier(_)));
        } else {
            panic!("Expected ILike at top level");
        }
    }

    #[test]
    fn test_not_binds_correctly() {
        // NOT a = b should parse as NOT (a = b)
        let result = parse_expr_core("NOT a = b").unwrap();
        if let Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } = result
        {
            assert!(matches!(
                expr.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Eq,
                    ..
                }
            ));
        } else {
            panic!("Expected UnaryOp(Not) at top level");
        }
    }

    #[test]
    fn test_between_with_arithmetic() {
        // a + 1 BETWEEN 2 AND 3 should parse as (a + 1) BETWEEN 2 AND 3
        let result = parse_expr_core("a + 1 BETWEEN 2 AND 3").unwrap();
        if let Expr::Between { expr, .. } = result {
            assert!(matches!(
                expr.as_ref(),
                Expr::BinaryOp {
                    op: BinaryOperator::Plus,
                    ..
                }
            ));
        } else {
            panic!("Expected Between at top level");
        }
    }

    #[test]
    fn test_complex_with_between_and_is() {
        // a BETWEEN 1 AND 10 AND b IS NULL
        let result = parse_expr_core("a BETWEEN 1 AND 10 AND b IS NULL").unwrap();
        if let Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = result
        {
            assert!(matches!(left.as_ref(), Expr::Between { .. }));
            assert!(matches!(right.as_ref(), Expr::IsNull(_)));
        } else {
            panic!("Expected And at top level");
        }
    }
}
