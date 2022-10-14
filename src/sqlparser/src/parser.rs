// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SQL Parser

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
    vec,
    vec::Vec,
};
use core::fmt;

use tracing::debug;

use crate::ast::{ParseTo, *};
use crate::keywords::{self, Keyword};
use crate::tokenizer::*;

#[derive(Debug, Clone, PartialEq)]
pub enum ParserError {
    TokenizerError(String),
    ParserError(String),
}

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

// Returns a successful result if the optional expression is some
macro_rules! return_ok_if_some {
    ($e:expr) => {{
        if let Some(v) = $e {
            return Ok(v);
        }
    }};
}

#[derive(PartialEq)]
pub enum IsOptional {
    Optional,
    Mandatory,
}

use IsOptional::*;

pub enum IsLateral {
    Lateral,
    NotLateral,
}

use IsLateral::*;

pub enum WildcardExpr {
    Expr(Expr),
    /// Expr and Qualified wildcard, expr is a table or a column struct, object_name is field.
    /// e.g. `(table.v1).*` or `(table).v1.*`
    ExprQualifiedWildcard(Expr, ObjectName),
    QualifiedWildcard(ObjectName),
    Wildcard,
}

impl From<WildcardExpr> for FunctionArgExpr {
    fn from(wildcard_expr: WildcardExpr) -> Self {
        match wildcard_expr {
            WildcardExpr::Expr(expr) => Self::Expr(expr),
            WildcardExpr::ExprQualifiedWildcard(expr, prefix) => {
                Self::ExprQualifiedWildcard(expr, prefix)
            }
            WildcardExpr::QualifiedWildcard(prefix) => Self::QualifiedWildcard(prefix),
            WildcardExpr::Wildcard => Self::Wildcard,
        }
    }
}

impl From<TokenizerError> for ParserError {
    fn from(e: TokenizerError) -> Self {
        ParserError::TokenizerError(e.to_string())
    }
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sql parser error: {}",
            match self {
                ParserError::TokenizerError(s) => s,
                ParserError::ParserError(s) => s,
            }
        )
    }
}

#[cfg(feature = "std")]
impl std::error::Error for ParserError {}

pub struct Parser {
    tokens: Vec<Token>,
    /// The index of the first unprocessed token in `self.tokens`
    index: usize,
    /// Since we cannot distinguish `>>` and double `>`, so use `angle_brackets_num` to store the
    /// number of `<` to match `>` in sql like `struct<v1 struct<v2 int>>`.
    angle_brackets_num: i32,
}

impl Parser {
    const BETWEEN_PREC: u8 = 20;
    const PLUS_MINUS_PREC: u8 = 30;
    const UNARY_NOT_PREC: u8 = 15;

    /// Parse the specified tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser {
            tokens,
            index: 0,
            angle_brackets_num: 0,
        }
    }

    /// Parse a SQL statement and produce an Abstract Syntax Tree (AST)
    pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let mut tokenizer = Tokenizer::new(sql);
        let tokens = tokenizer.tokenize()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = Vec::new();
        let mut expecting_statement_delimiter = false;
        debug!("Parsing sql '{}'...", sql);
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Parse a single top-level statement (such as SELECT, INSERT, CREATE, etc.),
    /// stopping before the statement separator, if any.
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::EXPLAIN => Ok(self.parse_explain()?),
                Keyword::ANALYZE => Ok(self.parse_analyze()?),
                Keyword::SELECT | Keyword::WITH | Keyword::VALUES => {
                    self.prev_token();
                    Ok(Statement::Query(Box::new(self.parse_query()?)))
                }
                Keyword::TRUNCATE => Ok(self.parse_truncate()?),
                Keyword::CREATE => Ok(self.parse_create()?),
                Keyword::DROP => Ok(self.parse_drop()?),
                Keyword::DELETE => Ok(self.parse_delete()?),
                Keyword::INSERT => Ok(self.parse_insert()?),
                Keyword::UPDATE => Ok(self.parse_update()?),
                Keyword::ALTER => Ok(self.parse_alter()?),
                Keyword::COPY => Ok(self.parse_copy()?),
                Keyword::SET => Ok(self.parse_set()?),
                Keyword::SHOW => Ok(self.parse_show()?),
                Keyword::DESCRIBE => Ok(Statement::Describe {
                    name: self.parse_object_name()?,
                }),
                Keyword::GRANT => Ok(self.parse_grant()?),
                Keyword::REVOKE => Ok(self.parse_revoke()?),
                Keyword::START => Ok(self.parse_start_transaction()?),
                Keyword::ABORT => Ok(Statement::Abort),
                // `BEGIN` is a nonstandard but common alias for the
                // standard `START TRANSACTION` statement. It is supported
                // by at least PostgreSQL and MySQL.
                Keyword::BEGIN => Ok(self.parse_begin()?),
                Keyword::COMMIT => Ok(self.parse_commit()?),
                Keyword::ROLLBACK => Ok(self.parse_rollback()?),
                // `PREPARE`, `EXECUTE` and `DEALLOCATE` are Postgres-specific
                // syntaxes. They are used for Postgres prepared statement.
                Keyword::DEALLOCATE => Ok(self.parse_deallocate()?),
                Keyword::EXECUTE => Ok(self.parse_execute()?),
                Keyword::PREPARE => Ok(self.parse_prepare()?),
                Keyword::COMMENT => Ok(self.parse_comment()?),
                Keyword::FLUSH => Ok(Statement::Flush),
                _ => self.expected("an SQL statement", Token::Word(w)),
            },
            Token::LParen => {
                self.prev_token();
                Ok(Statement::Query(Box::new(self.parse_query()?)))
            }
            unexpected => self.expected("an SQL statement", unexpected),
        }
    }

    pub fn parse_truncate(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_keyword(Keyword::TABLE);
        let table_name = self.parse_object_name()?;
        Ok(Statement::Truncate { table_name })
    }

    pub fn parse_analyze(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_object_name()?;

        Ok(Statement::Analyze { table_name })
    }

    /// Parse a new expression including wildcard & qualified wildcard
    pub fn parse_wildcard_expr(&mut self) -> Result<WildcardExpr, ParserError> {
        let index = self.index;

        match self.next_token() {
            Token::Word(w) if self.peek_token() == Token::Period => {
                // Since there's no parenthesis, `w` must be a column or a table
                // So what follows must be dot-delimited identifiers, e.g. `a.b.c.*`
                let wildcard_expr = self.parse_simple_wildcard_expr(index)?;
                return self.word_concat_wildcard_expr(w.to_ident(), wildcard_expr);
            }
            Token::Mul => {
                return Ok(WildcardExpr::Wildcard);
            }
            // TODO: support (((table.v1).v2).*)
            // parser wildcard field selection expression
            Token::LParen => {
                let mut expr = self.parse_expr()?;
                if self.consume_token(&Token::RParen) {
                    // Cast off nested expression to avoid interface by parenthesis.
                    while let Expr::Nested(expr1) = expr {
                        expr = *expr1;
                    }
                    // Now that we have an expr, what follows must be
                    // dot-delimited identifiers, e.g. `(a).b.c.*`
                    let wildcard_expr = self.parse_simple_wildcard_expr(index)?;
                    return self.expr_concat_wildcard_expr(expr, wildcard_expr);
                }
            }
            _ => (),
        };

        self.index = index;
        self.parse_expr().map(WildcardExpr::Expr)
    }

    /// Will return a `WildcardExpr::QualifiedWildcard(ObjectName)` with word concat or
    /// `WildcardExpr::Expr`
    pub fn word_concat_wildcard_expr(
        &mut self,
        ident: Ident,
        expr: WildcardExpr,
    ) -> Result<WildcardExpr, ParserError> {
        if let WildcardExpr::QualifiedWildcard(mut idents) = expr {
            let mut id_parts = vec![ident];
            id_parts.append(&mut idents.0);
            Ok(WildcardExpr::QualifiedWildcard(ObjectName(id_parts)))
        } else {
            Ok(expr)
        }
    }

    /// Will return a `WildcardExpr::ExprQualifiedWildcard(Expr,ObjectName)` with expr concat or
    /// `WildcardExpr::Expr`
    pub fn expr_concat_wildcard_expr(
        &mut self,
        expr: Expr,
        wildcard_expr: WildcardExpr,
    ) -> Result<WildcardExpr, ParserError> {
        if let WildcardExpr::QualifiedWildcard(idents) = wildcard_expr {
            let mut id_parts = idents.0;
            if let Expr::FieldIdentifier(expr, mut idents) = expr {
                idents.append(&mut id_parts);
                Ok(WildcardExpr::ExprQualifiedWildcard(
                    *expr,
                    ObjectName(idents),
                ))
            } else {
                Ok(WildcardExpr::ExprQualifiedWildcard(
                    expr,
                    ObjectName(id_parts),
                ))
            }
        } else {
            Ok(wildcard_expr)
        }
    }

    /// Will return a `WildcardExpr::QualifiedWildcard(ObjectName)` or `WildcardExpr::Expr`
    pub fn parse_simple_wildcard_expr(
        &mut self,
        index: usize,
    ) -> Result<WildcardExpr, ParserError> {
        let mut id_parts = vec![];
        while self.consume_token(&Token::Period) {
            match self.next_token() {
                Token::Word(w) => id_parts.push(w.to_ident()),
                Token::Mul => {
                    return Ok(WildcardExpr::QualifiedWildcard(ObjectName(id_parts)));
                }
                unexpected => {
                    return self.expected("an identifier or a '*' after '.'", unexpected);
                }
            }
        }
        self.index = index;
        self.parse_expr().map(WildcardExpr::Expr)
    }

    /// Parse a new expression
    pub fn parse_expr(&mut self) -> Result<Expr, ParserError> {
        self.parse_subexpr(0)
    }

    /// Parse tokens until the precedence changes
    pub fn parse_subexpr(&mut self, precedence: u8) -> Result<Expr, ParserError> {
        debug!("parsing expr");
        let mut expr = self.parse_prefix()?;
        debug!("prefix: {:?}", expr);
        loop {
            let next_precedence = self.get_next_precedence()?;
            debug!("next precedence: {:?}", next_precedence);

            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix(expr, next_precedence)?;
        }
        Ok(expr)
    }

    /// Parse an expression prefix
    pub fn parse_prefix(&mut self) -> Result<Expr, ParserError> {
        // PostgreSQL allows any string literal to be preceded by a type name, indicating that the
        // string literal represents a literal of that type. Some examples:
        //
        //      DATE '2020-05-20'
        //      TIMESTAMP WITH TIME ZONE '2020-05-20 7:43:54'
        //      BOOL 'true'
        //
        // The first two are standard SQL, while the latter is a PostgreSQL extension. Complicating
        // matters is the fact that INTERVAL string literals may optionally be followed by special
        // keywords, e.g.:
        //
        //      INTERVAL '7' DAY
        //
        // Note also that naively `SELECT date` looks like a syntax error because the `date` type
        // name is not followed by a string literal, but in fact in PostgreSQL it is a valid
        // expression that should parse as the column name "date".
        return_ok_if_some!(self.maybe_parse(|parser| {
            match parser.parse_data_type()? {
                DataType::Interval => parser.parse_literal_interval(),
                // PostgreSQL allows almost any identifier to be used as custom data type name,
                // and we support that in `parse_data_type()`. But unlike Postgres we don't
                // have a list of globally reserved keywords (since they vary across dialects),
                // so given `NOT 'a' LIKE 'b'`, we'd accept `NOT` as a possible custom data type
                // name, resulting in `NOT 'a'` being recognized as a `TypedString` instead of
                // an unary negation `NOT ('a' LIKE 'b')`. To solve this, we don't accept the
                // `type 'string'` syntax for the custom data types at all.
                DataType::Custom(..) => parser_err!("dummy"),
                data_type => Ok(Expr::TypedString {
                    data_type,
                    value: parser.parse_literal_string()?,
                }),
            }
        }));

        let expr = match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE | Keyword::FALSE | Keyword::NULL => {
                    self.prev_token();
                    Ok(Expr::Value(self.parse_value()?))
                }
                Keyword::CASE => self.parse_case_expr(),
                Keyword::CAST => self.parse_cast_expr(),
                Keyword::TRY_CAST => self.parse_try_cast_expr(),
                Keyword::EXISTS => self.parse_exists_expr(),
                Keyword::EXTRACT => self.parse_extract_expr(),
                Keyword::SUBSTRING => self.parse_substring_expr(),
                Keyword::OVERLAY => self.parse_overlay_expr(),
                Keyword::TRIM => self.parse_trim_expr(),
                Keyword::INTERVAL => self.parse_literal_interval(),
                Keyword::NOT => Ok(Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.parse_subexpr(Self::UNARY_NOT_PREC)?),
                }),
                Keyword::ROW => self.parse_row_expr(),
                Keyword::ARRAY => Ok(Expr::Array(
                    self.parse_token_wrapped_exprs(&Token::LBracket, &Token::RBracket)?,
                )),
                k if keywords::RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(&k) => {
                    parser_err!(format!("syntax error at or near \"{w}\""))
                }
                // Here `w` is a word, check if it's a part of a multi-part
                // identifier, a function call, or a simple identifier:
                _ => match self.peek_token() {
                    Token::LParen | Token::Period => {
                        let mut id_parts: Vec<Ident> = vec![w.to_ident()];
                        while self.consume_token(&Token::Period) {
                            match self.next_token() {
                                Token::Word(w) => id_parts.push(w.to_ident()),
                                unexpected => {
                                    return self
                                        .expected("an identifier or a '*' after '.'", unexpected);
                                }
                            }
                        }

                        if self.consume_token(&Token::LParen) {
                            self.prev_token();
                            self.parse_function(ObjectName(id_parts))
                        } else {
                            Ok(Expr::CompoundIdentifier(id_parts))
                        }
                    }
                    _ => Ok(Expr::Identifier(w.to_ident())),
                },
            }, // End of Token::Word
            tok @ Token::Minus | tok @ Token::Plus => {
                let op = if tok == Token::Plus {
                    UnaryOperator::Plus
                } else {
                    UnaryOperator::Minus
                };
                let mut sub_expr = self.parse_subexpr(Self::PLUS_MINUS_PREC)?;
                if let Expr::Value(Value::Number(ref mut s)) = sub_expr {
                    if tok == Token::Minus {
                        *s = format!("-{}", s);
                    }
                    return Ok(sub_expr);
                }
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(sub_expr),
                })
            }
            tok @ Token::DoubleExclamationMark
            | tok @ Token::PGSquareRoot
            | tok @ Token::PGCubeRoot
            | tok @ Token::AtSign
            | tok @ Token::Tilde => {
                let op = match tok {
                    Token::DoubleExclamationMark => UnaryOperator::PGPrefixFactorial,
                    Token::PGSquareRoot => UnaryOperator::PGSquareRoot,
                    Token::PGCubeRoot => UnaryOperator::PGCubeRoot,
                    Token::AtSign => UnaryOperator::PGAbs,
                    Token::Tilde => UnaryOperator::PGBitwiseNot,
                    _ => unreachable!(),
                };
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(self.parse_subexpr(Self::PLUS_MINUS_PREC)?),
                })
            }
            Token::Number(_)
            | Token::SingleQuotedString(_)
            | Token::NationalStringLiteral(_)
            | Token::HexStringLiteral(_) => {
                self.prev_token();
                Ok(Expr::Value(self.parse_value()?))
            }

            Token::LParen => {
                let expr =
                    if self.parse_keyword(Keyword::SELECT) || self.parse_keyword(Keyword::WITH) {
                        self.prev_token();
                        Expr::Subquery(Box::new(self.parse_query()?))
                    } else {
                        let mut exprs = self.parse_comma_separated(Parser::parse_expr)?;
                        if exprs.len() == 1 {
                            Expr::Nested(Box::new(exprs.pop().unwrap()))
                        } else {
                            Expr::Row(exprs)
                        }
                    };
                self.expect_token(&Token::RParen)?;
                if self.peek_token() == Token::Period {
                    self.parse_struct_selection(expr)
                } else {
                    Ok(expr)
                }
            }
            unexpected => self.expected("an expression:", unexpected),
        }?;

        if self.parse_keyword(Keyword::COLLATE) {
            Ok(Expr::Collate {
                expr: Box::new(expr),
                collation: self.parse_object_name()?,
            })
        } else {
            Ok(expr)
        }
    }

    // Parser field selection expression
    pub fn parse_struct_selection(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        if let Expr::Nested(compound_expr) = expr.clone() {
            let mut nested_expr = *compound_expr;
            // Cast off nested expression to avoid interface by parenthesis.
            while let Expr::Nested(expr1) = nested_expr {
                nested_expr = *expr1;
            }
            match nested_expr {
                // Parser expr like `SELECT (foo).v1 from foo`
                Expr::Identifier(ident) => Ok(Expr::FieldIdentifier(
                    Box::new(Expr::Identifier(ident)),
                    self.parse_field()?,
                )),
                // Parser expr like `SELECT (foo.v1).v2 from foo`
                Expr::CompoundIdentifier(idents) => Ok(Expr::FieldIdentifier(
                    Box::new(Expr::CompoundIdentifier(idents)),
                    self.parse_field()?,
                )),
                // Parser expr like `SELECT ((1,2,3)::foo).v1`
                Expr::Cast { expr, data_type } => Ok(Expr::FieldIdentifier(
                    Box::new(Expr::Cast { expr, data_type }),
                    self.parse_field()?,
                )),
                // Parser expr like `SELECT ((foo.v1).v2).v3 from foo`
                Expr::FieldIdentifier(expr, mut idents) => {
                    idents.extend(self.parse_field()?);
                    Ok(Expr::FieldIdentifier(expr, idents))
                }
                _ => Ok(expr),
            }
        } else {
            Ok(expr)
        }
    }

    /// Parser all words after period until not period
    pub fn parse_field(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        while self.consume_token(&Token::Period) {
            match self.next_token() {
                Token::Word(w) => {
                    idents.push(w.to_ident());
                }
                unexpected => {
                    return self.expected("an identifier after '.'", unexpected);
                }
            }
        }
        Ok(idents)
    }

    pub fn parse_function(&mut self, name: ObjectName) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let distinct = self.parse_all_or_distinct()?;
        let (args, order_by) = self.parse_optional_args()?;
        let over = if self.parse_keyword(Keyword::OVER) {
            // TBD: support window names (`OVER mywin`) in place of inline specification
            self.expect_token(&Token::LParen)?;
            let partition_by = if self.parse_keywords(&[Keyword::PARTITION, Keyword::BY]) {
                // a list of possibly-qualified column names
                self.parse_comma_separated(Parser::parse_expr)?
            } else {
                vec![]
            };
            let order_by_window = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            let window_frame = if !self.consume_token(&Token::RParen) {
                let window_frame = self.parse_window_frame()?;
                self.expect_token(&Token::RParen)?;
                Some(window_frame)
            } else {
                None
            };

            Some(WindowSpec {
                partition_by,
                order_by: order_by_window,
                window_frame,
            })
        } else {
            None
        };

        let filter = if self.parse_keyword(Keyword::FILTER) {
            self.expect_token(&Token::LParen)?;
            self.expect_keyword(Keyword::WHERE)?;
            let filter_expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Some(Box::new(filter_expr))
        } else {
            None
        };

        Ok(Expr::Function(Function {
            name,
            args,
            over,
            distinct,
            order_by,
            filter,
        }))
    }

    pub fn parse_window_frame_units(&mut self) -> Result<WindowFrameUnits, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::ROWS => Ok(WindowFrameUnits::Rows),
                Keyword::RANGE => Ok(WindowFrameUnits::Range),
                Keyword::GROUPS => Ok(WindowFrameUnits::Groups),
                _ => self.expected("ROWS, RANGE, GROUPS", Token::Word(w))?,
            },
            unexpected => self.expected("ROWS, RANGE, GROUPS", unexpected),
        }
    }

    pub fn parse_window_frame(&mut self) -> Result<WindowFrame, ParserError> {
        let units = self.parse_window_frame_units()?;
        let (start_bound, end_bound) = if self.parse_keyword(Keyword::BETWEEN) {
            let start_bound = self.parse_window_frame_bound()?;
            self.expect_keyword(Keyword::AND)?;
            let end_bound = Some(self.parse_window_frame_bound()?);
            (start_bound, end_bound)
        } else {
            (self.parse_window_frame_bound()?, None)
        };
        Ok(WindowFrame {
            units,
            start_bound,
            end_bound,
        })
    }

    /// Parse `CURRENT ROW` or `{ <positive number> | UNBOUNDED } { PRECEDING | FOLLOWING }`
    pub fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParserError> {
        if self.parse_keywords(&[Keyword::CURRENT, Keyword::ROW]) {
            Ok(WindowFrameBound::CurrentRow)
        } else {
            let rows = if self.parse_keyword(Keyword::UNBOUNDED) {
                None
            } else {
                Some(self.parse_literal_uint()?)
            };
            if self.parse_keyword(Keyword::PRECEDING) {
                Ok(WindowFrameBound::Preceding(rows))
            } else if self.parse_keyword(Keyword::FOLLOWING) {
                Ok(WindowFrameBound::Following(rows))
            } else {
                self.expected("PRECEDING or FOLLOWING", self.peek_token())
            }
        }
    }

    /// parse a group by expr. a group by expr can be one of group sets, roll up, cube, or simple
    /// expr.
    fn parse_group_by_expr(&mut self) -> Result<Expr, ParserError> {
        if self.parse_keywords(&[Keyword::GROUPING, Keyword::SETS]) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(false, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::GroupingSets(result))
        } else if self.parse_keyword(Keyword::CUBE) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Cube(result))
        } else if self.parse_keyword(Keyword::ROLLUP) {
            self.expect_token(&Token::LParen)?;
            let result = self.parse_comma_separated(|p| p.parse_tuple(true, true))?;
            self.expect_token(&Token::RParen)?;
            Ok(Expr::Rollup(result))
        } else {
            self.parse_expr()
        }
    }

    /// parse a tuple with `(` and `)`.
    /// If `lift_singleton` is true, then a singleton tuple is lifted to a tuple of length 1,
    /// otherwise it will fail. If `allow_empty` is true, then an empty tuple is allowed.
    fn parse_tuple(
        &mut self,
        lift_singleton: bool,
        allow_empty: bool,
    ) -> Result<Vec<Expr>, ParserError> {
        if lift_singleton {
            if self.consume_token(&Token::LParen) {
                let result = if allow_empty && self.consume_token(&Token::RParen) {
                    vec![]
                } else {
                    let result = self.parse_comma_separated(Parser::parse_expr)?;
                    self.expect_token(&Token::RParen)?;
                    result
                };
                Ok(result)
            } else {
                Ok(vec![self.parse_expr()?])
            }
        } else {
            self.expect_token(&Token::LParen)?;
            let result = if allow_empty && self.consume_token(&Token::RParen) {
                vec![]
            } else {
                let result = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(&Token::RParen)?;
                result
            };
            Ok(result)
        }
    }

    pub fn parse_case_expr(&mut self) -> Result<Expr, ParserError> {
        let mut operand = None;
        if !self.parse_keyword(Keyword::WHEN) {
            operand = Some(Box::new(self.parse_expr()?));
            self.expect_keyword(Keyword::WHEN)?;
        }
        let mut conditions = vec![];
        let mut results = vec![];
        loop {
            conditions.push(self.parse_expr()?);
            self.expect_keyword(Keyword::THEN)?;
            results.push(self.parse_expr()?);
            if !self.parse_keyword(Keyword::WHEN) {
                break;
            }
        }
        let else_result = if self.parse_keyword(Keyword::ELSE) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::END)?;
        Ok(Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse a SQL CAST function e.g. `CAST(expr AS FLOAT)`
    pub fn parse_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL TRY_CAST function e.g. `TRY_CAST(expr AS FLOAT)`
    pub fn parse_try_cast_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.expect_keyword(Keyword::AS)?;
        let data_type = self.parse_data_type()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::TryCast {
            expr: Box::new(expr),
            data_type,
        })
    }

    /// Parse a SQL EXISTS expression e.g. `WHERE EXISTS(SELECT ...)`.
    pub fn parse_exists_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let exists_node = Expr::Exists(Box::new(self.parse_query()?));
        self.expect_token(&Token::RParen)?;
        Ok(exists_node)
    }

    pub fn parse_extract_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let field = self.parse_date_time_field()?;
        self.expect_keyword(Keyword::FROM)?;
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;
        Ok(Expr::Extract {
            field,
            expr: Box::new(expr),
        })
    }

    pub fn parse_substring_expr(&mut self) -> Result<Expr, ParserError> {
        // PARSE SUBSTRING (EXPR [FROM 1] [FOR 3])
        self.expect_token(&Token::LParen)?;
        let expr = self.parse_expr()?;
        let mut from_expr = None;
        if self.parse_keyword(Keyword::FROM) || self.consume_token(&Token::Comma) {
            from_expr = Some(self.parse_expr()?);
        }

        let mut to_expr = None;
        if self.parse_keyword(Keyword::FOR) || self.consume_token(&Token::Comma) {
            to_expr = Some(self.parse_expr()?);
        }
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Substring {
            expr: Box::new(expr),
            substring_from: from_expr.map(Box::new),
            substring_for: to_expr.map(Box::new),
        })
    }

    /// OVERLAY(<expr> PLACING <expr> FROM <expr> [ FOR <expr> ])
    pub fn parse_overlay_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;

        let expr = self.parse_expr()?;

        self.expect_keyword(Keyword::PLACING)?;
        let new_substring = self.parse_expr()?;

        self.expect_keyword(Keyword::FROM)?;
        let start = self.parse_expr()?;

        let mut count = None;
        if self.parse_keyword(Keyword::FOR) {
            count = Some(self.parse_expr()?);
        }

        self.expect_token(&Token::RParen)?;

        Ok(Expr::Overlay {
            expr: Box::new(expr),
            new_substring: Box::new(new_substring),
            start: Box::new(start),
            count: count.map(Box::new),
        })
    }

    /// TRIM (WHERE 'text' FROM 'text')\
    /// TRIM ('text')
    pub fn parse_trim_expr(&mut self) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let mut where_expr = None;
        if let Token::Word(word) = self.peek_token() {
            if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING]
                .iter()
                .any(|d| word.keyword == *d)
            {
                let trim_where = self.parse_trim_where()?;
                let sub_expr = self.parse_expr()?;
                self.expect_keyword(Keyword::FROM)?;
                where_expr = Some((trim_where, Box::new(sub_expr)));
            }
        }
        let expr = self.parse_expr()?;
        self.expect_token(&Token::RParen)?;

        Ok(Expr::Trim {
            expr: Box::new(expr),
            trim_where: where_expr,
        })
    }

    pub fn parse_trim_where(&mut self) -> Result<TrimWhereField, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::BOTH => Ok(TrimWhereField::Both),
                Keyword::LEADING => Ok(TrimWhereField::Leading),
                Keyword::TRAILING => Ok(TrimWhereField::Trailing),
                _ => self.expected("trim_where field", Token::Word(w))?,
            },
            unexpected => self.expected("trim_where field", unexpected),
        }
    }

    // This function parses date/time fields for both the EXTRACT function-like
    // operator and interval qualifiers. EXTRACT supports a wider set of
    // date/time fields than interval qualifiers, so this function may need to
    // be split in two.
    pub fn parse_date_time_field(&mut self) -> Result<DateTimeField, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::YEAR => Ok(DateTimeField::Year),
                Keyword::MONTH => Ok(DateTimeField::Month),
                Keyword::DAY => Ok(DateTimeField::Day),
                Keyword::HOUR => Ok(DateTimeField::Hour),
                Keyword::MINUTE => Ok(DateTimeField::Minute),
                Keyword::SECOND => Ok(DateTimeField::Second),
                _ => self.expected("date/time field", Token::Word(w))?,
            },
            unexpected => self.expected("date/time field", unexpected),
        }
    }

    /// Parse an INTERVAL literal.
    ///
    /// Some syntactically valid intervals:
    ///
    ///   1. `INTERVAL '1' DAY`
    ///   2. `INTERVAL '1-1' YEAR TO MONTH`
    ///   3. `INTERVAL '1' SECOND`
    ///   4. `INTERVAL '1:1:1.1' HOUR (5) TO SECOND (5)`
    ///   5. `INTERVAL '1.1' SECOND (2, 2)`
    ///   6. `INTERVAL '1:1' HOUR (5) TO MINUTE (5)`
    ///
    /// Note that we do not currently attempt to parse the quoted value.
    pub fn parse_literal_interval(&mut self) -> Result<Expr, ParserError> {
        // The SQL standard allows an optional sign before the value string, but
        // it is not clear if any implementations support that syntax, so we
        // don't currently try to parse it. (The sign can instead be included
        // inside the value string.)

        // The first token in an interval is a string literal which specifies
        // the duration of the interval.
        let value = self.parse_literal_string()?;

        // Following the string literal is a qualifier which indicates the units
        // of the duration specified in the string literal.
        //
        // Note that PostgreSQL allows omitting the qualifier, so we provide
        // this more general implementation.
        let leading_field = match self.peek_token() {
            Token::Word(kw)
                if [
                    Keyword::YEAR,
                    Keyword::MONTH,
                    Keyword::DAY,
                    Keyword::HOUR,
                    Keyword::MINUTE,
                    Keyword::SECOND,
                ]
                .iter()
                .any(|d| kw.keyword == *d) =>
            {
                Some(self.parse_date_time_field()?)
            }
            _ => None,
        };

        let (leading_precision, last_field, fsec_precision) =
            if leading_field == Some(DateTimeField::Second) {
                // SQL mandates special syntax for `SECOND TO SECOND` literals.
                // Instead of
                //     `SECOND [(<leading precision>)] TO SECOND[(<fractional seconds precision>)]`
                // one must use the special format:
                //     `SECOND [( <leading precision> [ , <fractional seconds precision>] )]`
                let last_field = None;
                let (leading_precision, fsec_precision) = self.parse_optional_precision_scale()?;
                (leading_precision, last_field, fsec_precision)
            } else {
                let leading_precision = self.parse_optional_precision()?;
                if self.parse_keyword(Keyword::TO) {
                    let last_field = Some(self.parse_date_time_field()?);
                    let fsec_precision = if last_field == Some(DateTimeField::Second) {
                        self.parse_optional_precision()?
                    } else {
                        None
                    };
                    (leading_precision, last_field, fsec_precision)
                } else {
                    (leading_precision, None, None)
                }
            };

        Ok(Expr::Value(Value::Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision: fsec_precision,
        }))
    }

    /// Parse an operator following an expression
    pub fn parse_infix(&mut self, expr: Expr, precedence: u8) -> Result<Expr, ParserError> {
        let tok = self.next_token();
        let regular_binary_operator = match &tok {
            Token::Spaceship => Some(BinaryOperator::Spaceship),
            Token::DoubleEq => Some(BinaryOperator::Eq),
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::NotEq),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::GtEq => Some(BinaryOperator::GtEq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::LtEq => Some(BinaryOperator::LtEq),
            Token::Plus => Some(BinaryOperator::Plus),
            Token::Minus => Some(BinaryOperator::Minus),
            Token::Mul => Some(BinaryOperator::Multiply),
            Token::Mod => Some(BinaryOperator::Modulo),
            Token::Concat => Some(BinaryOperator::Concat),
            Token::Pipe => Some(BinaryOperator::BitwiseOr),
            Token::Caret => Some(BinaryOperator::BitwiseXor),
            Token::Ampersand => Some(BinaryOperator::BitwiseAnd),
            Token::Div => Some(BinaryOperator::Divide),
            Token::ShiftLeft => Some(BinaryOperator::PGBitwiseShiftLeft),
            Token::ShiftRight => Some(BinaryOperator::PGBitwiseShiftRight),
            Token::Sharp => Some(BinaryOperator::PGBitwiseXor),
            Token::Tilde => Some(BinaryOperator::PGRegexMatch),
            Token::TildeAsterisk => Some(BinaryOperator::PGRegexIMatch),
            Token::ExclamationMarkTilde => Some(BinaryOperator::PGRegexNotMatch),
            Token::ExclamationMarkTildeAsterisk => Some(BinaryOperator::PGRegexNotIMatch),
            Token::Word(w) => match w.keyword {
                Keyword::AND => Some(BinaryOperator::And),
                Keyword::OR => Some(BinaryOperator::Or),
                Keyword::LIKE => Some(BinaryOperator::Like),
                Keyword::ILIKE => Some(BinaryOperator::ILike),
                Keyword::NOT => {
                    if self.parse_keyword(Keyword::LIKE) {
                        Some(BinaryOperator::NotLike)
                    } else if self.parse_keyword(Keyword::ILIKE) {
                        Some(BinaryOperator::NotILike)
                    } else {
                        None
                    }
                }
                Keyword::XOR => Some(BinaryOperator::Xor),
                _ => None,
            },
            _ => None,
        };

        if let Some(op) = regular_binary_operator {
            Ok(Expr::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(self.parse_subexpr(precedence)?),
            })
        } else if let Token::Word(w) = &tok {
            match w.keyword {
                Keyword::IS => {
                    if self.parse_keyword(Keyword::TRUE) {
                        Ok(Expr::IsTrue(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::TRUE]) {
                        Ok(Expr::IsNotTrue(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::FALSE) {
                        Ok(Expr::IsFalse(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::FALSE]) {
                        Ok(Expr::IsNotFalse(Box::new(expr)))
                    } else if self.parse_keyword(Keyword::NULL) {
                        Ok(Expr::IsNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
                        Ok(Expr::IsNotNull(Box::new(expr)))
                    } else if self.parse_keywords(&[Keyword::DISTINCT, Keyword::FROM]) {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else if self.parse_keywords(&[Keyword::NOT, Keyword::DISTINCT, Keyword::FROM])
                    {
                        let expr2 = self.parse_expr()?;
                        Ok(Expr::IsNotDistinctFrom(Box::new(expr), Box::new(expr2)))
                    } else {
                        self.expected(
                            "[NOT] TRUE or [NOT] FALSE or [NOT] NULL or [NOT] DISTINCT FROM after IS",
                            self.peek_token(),
                        )
                    }
                }
                Keyword::NOT | Keyword::IN | Keyword::BETWEEN => {
                    self.prev_token();
                    let negated = self.parse_keyword(Keyword::NOT);
                    if self.parse_keyword(Keyword::IN) {
                        self.parse_in(expr, negated)
                    } else if self.parse_keyword(Keyword::BETWEEN) {
                        self.parse_between(expr, negated)
                    } else {
                        self.expected("IN or BETWEEN after NOT", self.peek_token())
                    }
                }
                // Can only happen if `get_next_precedence` got out of sync with this function
                _ => parser_err!(format!("No infix parser for token {:?}", tok)),
            }
        } else if Token::DoubleColon == tok {
            self.parse_pg_cast(expr)
        } else if Token::ExclamationMark == tok {
            // PostgreSQL factorial operation
            Ok(Expr::UnaryOp {
                op: UnaryOperator::PGPostfixFactorial,
                expr: Box::new(expr),
            })
        } else if Token::LBracket == tok {
            self.parse_array_index(expr)
        } else {
            // Can only happen if `get_next_precedence` got out of sync with this function
            parser_err!(format!("No infix parser for token {:?}", tok))
        }
    }

    pub fn parse_array_index(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        let index = Box::new(self.parse_expr()?);
        self.expect_token(&Token::RBracket)?;

        // Create ArrayIndex
        let array_index = Expr::ArrayIndex {
            obj: Box::new(expr),
            index,
        };

        // Return ArrayIndex Expr after after recursively checking for more indices
        if self.consume_token(&Token::LBracket) {
            self.parse_array_index(array_index)
        } else {
            Ok(array_index)
        }
    }

    /// Parses the parens following the `[ NOT ] IN` operator
    pub fn parse_in(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        self.expect_token(&Token::LParen)?;
        let in_op = if self.parse_keyword(Keyword::SELECT) || self.parse_keyword(Keyword::WITH) {
            self.prev_token();
            Expr::InSubquery {
                expr: Box::new(expr),
                subquery: Box::new(self.parse_query()?),
                negated,
            }
        } else {
            Expr::InList {
                expr: Box::new(expr),
                list: self.parse_comma_separated(Parser::parse_expr)?,
                negated,
            }
        };
        self.expect_token(&Token::RParen)?;
        Ok(in_op)
    }

    /// Parses `BETWEEN <low> AND <high>`, assuming the `BETWEEN` keyword was already consumed
    pub fn parse_between(&mut self, expr: Expr, negated: bool) -> Result<Expr, ParserError> {
        // Stop parsing subexpressions for <low> and <high> on tokens with
        // precedence lower than that of `BETWEEN`, such as `AND`, `IS`, etc.
        let low = self.parse_subexpr(Self::BETWEEN_PREC)?;
        self.expect_keyword(Keyword::AND)?;
        let high = self.parse_subexpr(Self::BETWEEN_PREC)?;
        Ok(Expr::Between {
            expr: Box::new(expr),
            negated,
            low: Box::new(low),
            high: Box::new(high),
        })
    }

    /// Parse a postgresql casting style which is in the form of `expr::datatype`
    pub fn parse_pg_cast(&mut self, expr: Expr) -> Result<Expr, ParserError> {
        Ok(Expr::Cast {
            expr: Box::new(expr),
            data_type: self.parse_data_type()?,
        })
    }

    /// Get the precedence of the next token
    pub fn get_next_precedence(&self) -> Result<u8, ParserError> {
        let token = self.peek_token();
        debug!("get_next_precedence() {:?}", token);
        match token {
            Token::Word(w) if w.keyword == Keyword::OR => Ok(5),
            Token::Word(w) if w.keyword == Keyword::AND => Ok(10),
            Token::Word(w) if w.keyword == Keyword::XOR => Ok(24),
            Token::Word(w) if w.keyword == Keyword::NOT => match self.peek_nth_token(1) {
                // The precedence of NOT varies depending on keyword that
                // follows it. If it is followed by IN, BETWEEN, or LIKE,
                // it takes on the precedence of those tokens. Otherwise it
                // is not an infix operator, and therefore has zero
                // precedence.
                Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::BETWEEN_PREC),
                Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::BETWEEN_PREC),
                _ => Ok(0),
            },
            Token::Word(w) if w.keyword == Keyword::IS => Ok(17),
            Token::Word(w) if w.keyword == Keyword::IN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::BETWEEN => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::LIKE => Ok(Self::BETWEEN_PREC),
            Token::Word(w) if w.keyword == Keyword::ILIKE => Ok(Self::BETWEEN_PREC),
            Token::Eq
            | Token::Lt
            | Token::LtEq
            | Token::Neq
            | Token::Gt
            | Token::GtEq
            | Token::DoubleEq
            | Token::Tilde
            | Token::TildeAsterisk
            | Token::ExclamationMarkTilde
            | Token::ExclamationMarkTildeAsterisk
            | Token::Spaceship => Ok(20),
            Token::Pipe => Ok(21),
            Token::Caret | Token::Sharp | Token::ShiftRight | Token::ShiftLeft => Ok(22),
            Token::Ampersand => Ok(23),
            Token::Plus | Token::Minus => Ok(Self::PLUS_MINUS_PREC),
            Token::Mul | Token::Div | Token::Mod | Token::Concat => Ok(40),
            Token::DoubleColon => Ok(50),
            Token::ExclamationMark => Ok(50),
            Token::LBracket => Ok(50),
            _ => Ok(0),
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file)
    pub fn peek_token(&self) -> Token {
        self.peek_nth_token(0)
    }

    /// Return nth non-whitespace token that has not yet been processed
    pub fn peek_nth_token(&self, mut n: usize) -> Token {
        let mut index = self.index;
        loop {
            index += 1;
            match self.tokens.get(index - 1) {
                Some(Token::Whitespace(_)) => continue,
                non_whitespace => {
                    if n == 0 {
                        return non_whitespace.cloned().unwrap_or(Token::EOF);
                    }
                    n -= 1;
                }
            }
        }
    }

    /// Return the first non-whitespace token that has not yet been processed
    /// (or None if reached end-of-file) and mark it as processed. OK to call
    /// repeatedly after reaching EOF.
    pub fn next_token(&mut self) -> Token {
        loop {
            self.index += 1;
            match self.tokens.get(self.index - 1) {
                Some(Token::Whitespace(_)) => continue,
                token => return token.cloned().unwrap_or(Token::EOF),
            }
        }
    }

    /// Return the first unprocessed token, possibly whitespace.
    pub fn next_token_no_skip(&mut self) -> Option<&Token> {
        self.index += 1;
        self.tokens.get(self.index - 1)
    }

    /// Push back the last one non-whitespace token. Must be called after
    /// `next_token()`, otherwise might panic. OK to call after
    /// `next_token()` indicates an EOF.
    pub fn prev_token(&mut self) {
        loop {
            assert!(self.index > 0);
            self.index -= 1;
            if let Some(Token::Whitespace(_)) = self.tokens.get(self.index) {
                continue;
            }
            return;
        }
    }

    /// Report unexpected token
    pub fn expected<T>(&self, expected: &str, found: Token) -> Result<T, ParserError> {
        parser_err!(format!("Expected {}, found: {}", expected, found))
    }

    /// Look for an expected keyword and consume it if it exists
    #[must_use]
    pub fn parse_keyword(&mut self, expected: Keyword) -> bool {
        match self.peek_token() {
            Token::Word(w) if expected == w.keyword => {
                self.next_token();
                true
            }
            _ => false,
        }
    }

    /// Look for an expected sequence of keywords and consume them if they exist
    #[must_use]
    pub fn parse_keywords(&mut self, keywords: &[Keyword]) -> bool {
        let index = self.index;
        for &keyword in keywords {
            if !self.parse_keyword(keyword) {
                // println!("parse_keywords aborting .. did not find {:?}", keyword);
                // reset index and return immediately
                self.index = index;
                return false;
            }
        }
        true
    }

    /// Look for one of the given keywords and return the one that matches.
    #[must_use]
    pub fn parse_one_of_keywords(&mut self, keywords: &[Keyword]) -> Option<Keyword> {
        match self.peek_token() {
            Token::Word(w) => {
                keywords
                    .iter()
                    .find(|keyword| **keyword == w.keyword)
                    .map(|keyword| {
                        self.next_token();
                        *keyword
                    })
            }
            _ => None,
        }
    }

    /// Bail out if the current token is not one of the expected keywords, or consume it if it is
    pub fn expect_one_of_keywords(&mut self, keywords: &[Keyword]) -> Result<Keyword, ParserError> {
        if let Some(keyword) = self.parse_one_of_keywords(keywords) {
            Ok(keyword)
        } else {
            let keywords: Vec<String> = keywords.iter().map(|x| format!("{:?}", x)).collect();
            self.expected(
                &format!("one of {}", keywords.join(" or ")),
                self.peek_token(),
            )
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_keyword(&mut self, expected: Keyword) -> Result<(), ParserError> {
        if self.parse_keyword(expected) {
            Ok(())
        } else {
            self.expected(format!("{:?}", &expected).as_str(), self.peek_token())
        }
    }

    /// Bail out if the following tokens are not the expected sequence of
    /// keywords, or consume them if they are.
    pub fn expect_keywords(&mut self, expected: &[Keyword]) -> Result<(), ParserError> {
        for &kw in expected {
            self.expect_keyword(kw)?;
        }
        Ok(())
    }

    /// Consume the next token if it matches the expected token, otherwise return false
    #[must_use]
    pub fn consume_token(&mut self, expected: &Token) -> bool {
        if self.peek_token() == *expected {
            self.next_token();
            true
        } else {
            false
        }
    }

    /// Bail out if the current token is not an expected keyword, or consume it if it is
    pub fn expect_token(&mut self, expected: &Token) -> Result<(), ParserError> {
        if self.consume_token(expected) {
            Ok(())
        } else {
            self.expected(&expected.to_string(), self.peek_token())
        }
    }

    /// Parse a comma-separated list of 1+ items accepted by `F`
    pub fn parse_comma_separated<T, F>(&mut self, mut f: F) -> Result<Vec<T>, ParserError>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let mut values = vec![];
        loop {
            values.push(f(self)?);
            if !self.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(values)
    }

    /// Run a parser method `f`, reverting back to the current position
    /// if unsuccessful.
    #[must_use]
    fn maybe_parse<T, F>(&mut self, mut f: F) -> Option<T>
    where
        F: FnMut(&mut Parser) -> Result<T, ParserError>,
    {
        let index = self.index;
        if let Ok(t) = f(self) {
            Some(t)
        } else {
            self.index = index;
            None
        }
    }

    /// Parse either `ALL` or `DISTINCT`. Returns `true` if `DISTINCT` is parsed and results in a
    /// `ParserError` if both `ALL` and `DISTINCT` are fround.
    pub fn parse_all_or_distinct(&mut self) -> Result<bool, ParserError> {
        let all = self.parse_keyword(Keyword::ALL);
        let distinct = self.parse_keyword(Keyword::DISTINCT);
        if all && distinct {
            parser_err!("Cannot specify both ALL and DISTINCT".to_string())
        } else {
            Ok(distinct)
        }
    }

    /// Parse either `ALL` or `DISTINCT` or `DISTINCT ON (<expr>)`.
    pub fn parse_all_or_distinct_on(&mut self) -> Result<Distinct, ParserError> {
        if self.parse_keywords(&[Keyword::DISTINCT, Keyword::ON]) {
            self.expect_token(&Token::LParen)?;
            let exprs = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
            return Ok(Distinct::DistinctOn(exprs));
        } else if self.parse_keyword(Keyword::DISTINCT) {
            return Ok(Distinct::Distinct);
        };
        _ = self.parse_keyword(Keyword::ALL);
        Ok(Distinct::All)
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let or_replace = self.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
        let temporary = self
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_create_table(or_replace, temporary)
        } else if self.parse_keyword(Keyword::VIEW) {
            self.parse_create_view(false, or_replace)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            self.parse_create_view(true, or_replace)
        } else if self.parse_keyword(Keyword::SOURCE) {
            self.parse_create_source(false, or_replace)
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::SOURCE]) {
            self.parse_create_source(true, or_replace)
        } else if self.parse_keyword(Keyword::SINK) {
            self.parse_create_sink(or_replace)
        } else if or_replace {
            self.expected(
                "[EXTERNAL] TABLE or [MATERIALIZED] VIEW after CREATE OR REPLACE",
                self.peek_token(),
            )
        } else if self.parse_keyword(Keyword::INDEX) {
            self.parse_create_index(false)
        } else if self.parse_keywords(&[Keyword::UNIQUE, Keyword::INDEX]) {
            self.parse_create_index(true)
        } else if self.parse_keyword(Keyword::SCHEMA) {
            self.parse_create_schema()
        } else if self.parse_keyword(Keyword::DATABASE) {
            self.parse_create_database()
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_create_user()
        } else {
            self.expected("an object type after CREATE", self.peek_token())
        }
    }

    pub fn parse_create_schema(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let schema_name = self.parse_object_name()?;
        Ok(Statement::CreateSchema {
            schema_name,
            if_not_exists,
        })
    }

    pub fn parse_create_database(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let db_name = self.parse_object_name()?;
        Ok(Statement::CreateDatabase {
            db_name,
            if_not_exists,
        })
    }

    pub fn parse_create_view(
        &mut self,
        materialized: bool,
        or_replace: bool,
    ) -> Result<Statement, ParserError> {
        // Many dialects support `OR ALTER` right after `CREATE`, but we don't (yet).
        // ANSI SQL and Postgres support RECURSIVE here, but we don't support it either.
        let name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let with_options = self.parse_options(Keyword::WITH)?;
        self.expect_keyword(Keyword::AS)?;
        let query = Box::new(self.parse_query()?);
        // Optional `WITH [ CASCADED | LOCAL ] CHECK OPTION` is widely supported here.
        Ok(Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            or_replace,
            with_options,
        })
    }

    // CREATE [OR REPLACE]?
    // [MATERIALIZED] SOURCE
    // [IF NOT EXISTS]?
    // <source_name: Ident>
    // [COLUMNS]?
    // [WITH (properties)]?
    // ROW FORMAT <row_format: Ident>
    // [ROW SCHEMA LOCATION <row_schema_location: String>]?
    pub fn parse_create_source(
        &mut self,
        is_materialized: bool,
        _or_replace: bool,
    ) -> Result<Statement, ParserError> {
        Ok(Statement::CreateSource {
            is_materialized,
            stmt: CreateSourceStatement::parse_to(self)?,
        })
    }

    // CREATE [OR REPLACE]?
    // SINK
    // [IF NOT EXISTS]?
    // <sink_name: Ident>
    // FROM
    // <materialized_view: Ident>
    // [WITH (properties)]?
    pub fn parse_create_sink(&mut self, _or_replace: bool) -> Result<Statement, ParserError> {
        Ok(Statement::CreateSink {
            stmt: CreateSinkStatement::parse_to(self)?,
        })
    }

    // CREATE USER name [ [ WITH ] option [ ... ] ]
    // where option can be:
    //       SUPERUSER | NOSUPERUSER
    //     | CREATEDB | NOCREATEDB
    //     | CREATEUSER | NOCREATEUSER
    //     | LOGIN | NOLOGIN
    //     | [ ENCRYPTED ] PASSWORD 'password' | PASSWORD NULL
    fn parse_create_user(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::CreateUser(CreateUserStatement::parse_to(self)?))
    }

    fn parse_with_properties(&mut self) -> Result<Vec<SqlOption>, ParserError> {
        Ok(self.parse_options(Keyword::WITH)?.to_vec())
    }

    pub fn parse_drop(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Drop(DropStatement::parse_to(self)?))
    }

    pub fn parse_create_index(&mut self, unique: bool) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let index_name = self.parse_object_name()?;
        self.expect_keyword(Keyword::ON)?;
        let table_name = self.parse_object_name()?;
        self.expect_token(&Token::LParen)?;
        let columns = self.parse_comma_separated(Parser::parse_order_by_expr)?;
        self.expect_token(&Token::RParen)?;
        let mut include = vec![];
        if self.parse_keyword(Keyword::INCLUDE) {
            self.expect_token(&Token::LParen)?;
            include = self.parse_comma_separated(Parser::parse_identifier_non_reserved)?;
            self.expect_token(&Token::RParen)?;
        }
        let mut distributed_by = vec![];
        if self.parse_keywords(&[Keyword::DISTRIBUTED, Keyword::BY]) {
            self.expect_token(&Token::LParen)?;
            distributed_by = self.parse_comma_separated(Parser::parse_identifier_non_reserved)?;
            self.expect_token(&Token::RParen)?;
        }
        Ok(Statement::CreateIndex {
            name: index_name,
            table_name,
            columns,
            include,
            distributed_by,
            unique,
            if_not_exists,
        })
    }

    pub fn parse_create_table(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parse_object_name()?;
        // parse optional column list (schema)
        let (columns, constraints) = self.parse_columns()?;

        // PostgreSQL supports `WITH ( options )`, before `AS`
        let with_options = self.parse_with_properties()?;
        // Parse optional `AS ( query )`
        let query = if self.parse_keyword(Keyword::AS) {
            Some(Box::new(self.parse_query()?))
        } else {
            None
        };

        Ok(Statement::CreateTable {
            name: table_name,
            temporary,
            columns,
            constraints,
            with_options,
            or_replace,
            if_not_exists,
            query,
        })
    }

    pub fn parse_columns(&mut self) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.consume_token(&Token::LParen) || self.consume_token(&Token::RParen) {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.peek_token() {
                columns.push(self.parse_column_def()?);
            } else {
                return self.expected("column name or constraint definition", self.peek_token());
            }
            let comma = self.consume_token(&Token::Comma);
            if self.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected("',' or ')' after column definition", self.peek_token());
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parse_identifier_non_reserved()?;
        let data_type = self.parse_data_type()?;

        let collation = if self.parse_keyword(Keyword::COLLATE) {
            Some(self.parse_object_name()?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parse_identifier_non_reserved()?);
                if let Some(option) = self.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.peek_token(),
                    );
                }
            } else if let Some(option) = self.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    pub fn parse_optional_column_option(&mut self) -> Result<Option<ColumnOption>, ParserError> {
        if self.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
            Ok(Some(ColumnOption::NotNull))
        } else if self.parse_keyword(Keyword::NULL) {
            Ok(Some(ColumnOption::Null))
        } else if self.parse_keyword(Keyword::DEFAULT) {
            Ok(Some(ColumnOption::Default(self.parse_expr()?)))
        } else if self.parse_keywords(&[Keyword::PRIMARY, Keyword::KEY]) {
            Ok(Some(ColumnOption::Unique { is_primary: true }))
        } else if self.parse_keyword(Keyword::UNIQUE) {
            Ok(Some(ColumnOption::Unique { is_primary: false }))
        } else if self.parse_keyword(Keyword::REFERENCES) {
            let foreign_table = self.parse_object_name()?;
            // PostgreSQL allows omitting the column list and
            // uses the primary key column of the foreign table by default
            let referred_columns = self.parse_parenthesized_column_list(Optional)?;
            let mut on_delete = None;
            let mut on_update = None;
            loop {
                if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                    on_delete = Some(self.parse_referential_action()?);
                } else if on_update.is_none()
                    && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                {
                    on_update = Some(self.parse_referential_action()?);
                } else {
                    break;
                }
            }
            Ok(Some(ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
            }))
        } else if self.parse_keyword(Keyword::CHECK) {
            self.expect_token(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(ColumnOption::Check(expr)))
        } else {
            Ok(None)
        }
    }

    pub fn parse_referential_action(&mut self) -> Result<ReferentialAction, ParserError> {
        if self.parse_keyword(Keyword::RESTRICT) {
            Ok(ReferentialAction::Restrict)
        } else if self.parse_keyword(Keyword::CASCADE) {
            Ok(ReferentialAction::Cascade)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::NULL]) {
            Ok(ReferentialAction::SetNull)
        } else if self.parse_keywords(&[Keyword::NO, Keyword::ACTION]) {
            Ok(ReferentialAction::NoAction)
        } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
            Ok(ReferentialAction::SetDefault)
        } else {
            self.expected(
                "one of RESTRICT, CASCADE, SET NULL, NO ACTION or SET DEFAULT",
                self.peek_token(),
            )
        }
    }

    pub fn parse_optional_table_constraint(
        &mut self,
    ) -> Result<Option<TableConstraint>, ParserError> {
        let name = if self.parse_keyword(Keyword::CONSTRAINT) {
            Some(self.parse_identifier_non_reserved()?)
        } else {
            None
        };
        match self.next_token() {
            Token::Word(w) if w.keyword == Keyword::PRIMARY || w.keyword == Keyword::UNIQUE => {
                let is_primary = w.keyword == Keyword::PRIMARY;
                if is_primary {
                    self.expect_keyword(Keyword::KEY)?;
                }
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                Ok(Some(TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::FOREIGN => {
                self.expect_keyword(Keyword::KEY)?;
                let columns = self.parse_parenthesized_column_list(Mandatory)?;
                self.expect_keyword(Keyword::REFERENCES)?;
                let foreign_table = self.parse_object_name()?;
                let referred_columns = self.parse_parenthesized_column_list(Mandatory)?;
                let mut on_delete = None;
                let mut on_update = None;
                loop {
                    if on_delete.is_none() && self.parse_keywords(&[Keyword::ON, Keyword::DELETE]) {
                        on_delete = Some(self.parse_referential_action()?);
                    } else if on_update.is_none()
                        && self.parse_keywords(&[Keyword::ON, Keyword::UPDATE])
                    {
                        on_update = Some(self.parse_referential_action()?);
                    } else {
                        break;
                    }
                }
                Ok(Some(TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                }))
            }
            Token::Word(w) if w.keyword == Keyword::CHECK => {
                self.expect_token(&Token::LParen)?;
                let expr = Box::new(self.parse_expr()?);
                self.expect_token(&Token::RParen)?;
                Ok(Some(TableConstraint::Check { name, expr }))
            }
            unexpected => {
                if name.is_some() {
                    self.expected("PRIMARY, UNIQUE, FOREIGN, or CHECK", unexpected)
                } else {
                    self.prev_token();
                    Ok(None)
                }
            }
        }
    }

    pub fn parse_options(&mut self, keyword: Keyword) -> Result<Vec<SqlOption>, ParserError> {
        if self.parse_keyword(keyword) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_sql_option)?;
            self.expect_token(&Token::RParen)?;
            Ok(options)
        } else {
            Ok(vec![])
        }
    }

    pub fn parse_sql_option(&mut self) -> Result<SqlOption, ParserError> {
        let name = self.parse_object_name()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_value()?;
        Ok(SqlOption { name, value })
    }

    pub fn parse_alter(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword(Keyword::TABLE) {
            self.parse_alter_table()
        } else if self.parse_keyword(Keyword::USER) {
            self.parse_alter_user()
        } else {
            self.expected("TABLE or USER after ALTER", self.peek_token())
        }
    }

    pub fn parse_alter_user(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::AlterUser(AlterUserStatement::parse_to(self)?))
    }

    pub fn parse_alter_table(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_keyword(Keyword::ONLY);
        let table_name = self.parse_object_name()?;
        let operation = if self.parse_keyword(Keyword::ADD) {
            if let Some(constraint) = self.parse_optional_table_constraint()? {
                AlterTableOperation::AddConstraint(constraint)
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN);
                let _if_not_exists =
                    self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
                let column_def = self.parse_column_def()?;
                AlterTableOperation::AddColumn { column_def }
            }
        } else if self.parse_keyword(Keyword::RENAME) {
            if self.parse_keyword(Keyword::CONSTRAINT) {
                let old_name = self.parse_identifier_non_reserved()?;
                self.expect_keyword(Keyword::TO)?;
                let new_name = self.parse_identifier_non_reserved()?;
                AlterTableOperation::RenameConstraint { old_name, new_name }
            } else if self.parse_keyword(Keyword::TO) {
                let table_name = self.parse_object_name()?;
                AlterTableOperation::RenameTable { table_name }
            } else {
                let _ = self.parse_keyword(Keyword::COLUMN);
                let old_column_name = self.parse_identifier_non_reserved()?;
                self.expect_keyword(Keyword::TO)?;
                let new_column_name = self.parse_identifier_non_reserved()?;
                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                }
            }
        } else if self.parse_keywords(&[Keyword::OWNER, Keyword::TO]) {
            let owner_name: Ident = self.parse_identifier()?;
            AlterTableOperation::ChangeOwner {
                new_owner_name: owner_name,
            }
        } else if self.parse_keyword(Keyword::DROP) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let if_exists = self.parse_keywords(&[Keyword::IF, Keyword::EXISTS]);
            let column_name = self.parse_identifier_non_reserved()?;
            let cascade = self.parse_keyword(Keyword::CASCADE);
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                cascade,
            }
        } else if self.parse_keyword(Keyword::ALTER) {
            let _ = self.parse_keyword(Keyword::COLUMN);
            let column_name = self.parse_identifier_non_reserved()?;

            let op = if self.parse_keywords(&[Keyword::SET, Keyword::NOT, Keyword::NULL]) {
                AlterColumnOperation::SetNotNull {}
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::NOT, Keyword::NULL]) {
                AlterColumnOperation::DropNotNull {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DEFAULT]) {
                AlterColumnOperation::SetDefault {
                    value: self.parse_expr()?,
                }
            } else if self.parse_keywords(&[Keyword::DROP, Keyword::DEFAULT]) {
                AlterColumnOperation::DropDefault {}
            } else if self.parse_keywords(&[Keyword::SET, Keyword::DATA, Keyword::TYPE])
                || (self.parse_keyword(Keyword::TYPE))
            {
                let data_type = self.parse_data_type()?;
                let using = if self.parse_keyword(Keyword::USING) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                AlterColumnOperation::SetDataType { data_type, using }
            } else {
                return self.expected(
                    "SET/DROP NOT NULL, SET DEFAULT, SET DATA TYPE after ALTER COLUMN",
                    self.peek_token(),
                );
            };
            AlterTableOperation::AlterColumn { column_name, op }
        } else {
            return self.expected("ADD, RENAME or DROP after ALTER TABLE", self.peek_token());
        };
        Ok(Statement::AlterTable {
            name: table_name,
            operation,
        })
    }

    /// Parse a copy statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        self.expect_keywords(&[Keyword::FROM, Keyword::STDIN])?;
        self.expect_token(&Token::SemiColon)?;
        let values = self.parse_tsv();
        Ok(Statement::Copy {
            table_name,
            columns,
            values,
        })
    }

    /// Parse a tab separated values in
    /// COPY payload
    fn parse_tsv(&mut self) -> Vec<Option<String>> {
        self.parse_tab_value()
    }

    fn parse_tab_value(&mut self) -> Vec<Option<String>> {
        let mut values = vec![];
        let mut content = String::from("");
        while let Some(t) = self.next_token_no_skip() {
            match t {
                Token::Whitespace(Whitespace::Tab) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Whitespace(Whitespace::Newline) => {
                    values.push(Some(content.to_string()));
                    content.clear();
                }
                Token::Backslash => {
                    if self.consume_token(&Token::Period) {
                        return values;
                    }
                    if let Token::Word(w) = self.next_token() {
                        if w.value == "N" {
                            values.push(None);
                        }
                    }
                }
                _ => {
                    content.push_str(&t.to_string());
                }
            }
        }
        values
    }

    /// Parse a literal value (numbers, strings, date/time, booleans)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::TRUE => Ok(Value::Boolean(true)),
                Keyword::FALSE => Ok(Value::Boolean(false)),
                Keyword::NULL => Ok(Value::Null),
                Keyword::NoKeyword if w.quote_style.is_some() => match w.quote_style {
                    Some('"') => Ok(Value::DoubleQuotedString(w.value)),
                    Some('\'') => Ok(Value::SingleQuotedString(w.value)),
                    _ => self.expected("A value?", Token::Word(w))?,
                },
                _ => self.expected("a concrete value", Token::Word(w)),
            },
            Token::Number(ref n) => Ok(Value::Number(n.clone())),
            Token::SingleQuotedString(ref s) => Ok(Value::SingleQuotedString(s.to_string())),
            Token::NationalStringLiteral(ref s) => Ok(Value::NationalStringLiteral(s.to_string())),
            Token::HexStringLiteral(ref s) => Ok(Value::HexStringLiteral(s.to_string())),
            unexpected => self.expected("a value", unexpected),
        }
    }

    pub fn parse_number_value(&mut self) -> Result<String, ParserError> {
        match self.parse_value()? {
            Value::Number(v) => Ok(v),
            _ => {
                self.prev_token();
                self.expected("literal number", self.peek_token())
            }
        }
    }

    /// Parse an unsigned literal integer/long
    pub fn parse_literal_uint(&mut self) -> Result<u64, ParserError> {
        match self.next_token() {
            Token::Number(s) => s.parse::<u64>().map_err(|e| {
                ParserError::ParserError(format!("Could not parse '{}' as u64: {}", s, e))
            }),
            unexpected => self.expected("literal int", unexpected),
        }
    }

    /// Parse a literal string
    pub fn parse_literal_string(&mut self) -> Result<String, ParserError> {
        match self.next_token() {
            Token::Word(Word { value, keyword, .. }) if keyword == Keyword::NoKeyword => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            unexpected => self.expected("literal string", unexpected),
        }
    }

    /// Parse a map key string
    pub fn parse_map_key(&mut self) -> Result<Expr, ParserError> {
        match self.next_token() {
            Token::Word(Word { value, keyword, .. }) if keyword == Keyword::NoKeyword => {
                if self.peek_token() == Token::LParen {
                    return self.parse_function(ObjectName(vec![Ident::new(value)]));
                }
                Ok(Expr::Value(Value::SingleQuotedString(value)))
            }
            Token::SingleQuotedString(s) => Ok(Expr::Value(Value::SingleQuotedString(s))),
            Token::Number(s) => Ok(Expr::Value(Value::Number(s))),
            unexpected => self.expected("literal string, number or function", unexpected),
        }
    }

    /// Parse a SQL datatype (in the context of a CREATE TABLE statement for example) and convert
    /// into an array of that datatype if needed
    pub fn parse_data_type(&mut self) -> Result<DataType, ParserError> {
        let mut data_type = self.parse_data_type_inner()?;
        while self.consume_token(&Token::LBracket) {
            self.expect_token(&Token::RBracket)?;
            data_type = DataType::Array(Box::new(data_type));
        }
        Ok(data_type)
    }

    /// Parse struct `data_type` e.g.`<v1 int, v2 int, v3 struct<...>>`.
    pub fn parse_struct_data_type(&mut self) -> Result<Vec<StructField>, ParserError> {
        let mut columns = vec![];
        if !self.consume_token(&Token::Lt) {
            return self.expected("'<' after struct", self.peek_token());
        }
        self.angle_brackets_num += 1;

        loop {
            if let Token::Word(_) = self.peek_token() {
                let name = self.parse_identifier_non_reserved()?;
                let data_type = self.parse_data_type()?;
                columns.push(StructField { name, data_type })
            } else {
                return self.expected("struct field name", self.peek_token());
            }
            if self.angle_brackets_num == 0 {
                break;
            } else if self.consume_token(&Token::Gt) {
                self.angle_brackets_num -= 1;
                break;
            } else if self.consume_token(&Token::ShiftRight) {
                if self.angle_brackets_num >= 1 {
                    self.angle_brackets_num -= 2;
                    break;
                } else {
                    return parser_err!("too much '>'");
                }
            } else if !self.consume_token(&Token::Comma) {
                return self.expected("',' or '>' after column definition", self.peek_token());
            }
        }

        Ok(columns)
    }

    /// Parse a SQL datatype
    pub fn parse_data_type_inner(&mut self) -> Result<DataType, ParserError> {
        match self.next_token() {
            Token::Word(w) => match w.keyword {
                Keyword::BOOLEAN | Keyword::BOOL => Ok(DataType::Boolean),
                Keyword::FLOAT => Ok(DataType::Float(self.parse_optional_precision()?)),
                Keyword::REAL => Ok(DataType::Real),
                Keyword::DOUBLE => {
                    let _ = self.parse_keyword(Keyword::PRECISION);
                    Ok(DataType::Double)
                }
                Keyword::TINYINT => Ok(DataType::TinyInt(self.parse_optional_precision()?)),
                Keyword::SMALLINT => Ok(DataType::SmallInt(self.parse_optional_precision()?)),
                Keyword::INT | Keyword::INTEGER => {
                    Ok(DataType::Int(self.parse_optional_precision()?))
                }
                Keyword::BIGINT => Ok(DataType::BigInt(self.parse_optional_precision()?)),
                Keyword::VARCHAR => Ok(DataType::Varchar),
                Keyword::CHAR | Keyword::CHARACTER => {
                    if self.parse_keyword(Keyword::VARYING) {
                        Ok(DataType::Varchar)
                    } else {
                        Ok(DataType::Char(self.parse_optional_precision()?))
                    }
                }
                Keyword::UUID => Ok(DataType::Uuid),
                Keyword::DATE => Ok(DataType::Date),
                Keyword::TIMESTAMP => {
                    let with_time_zone = self.parse_keyword(Keyword::WITH);
                    if with_time_zone || self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    }
                    Ok(DataType::Timestamp(with_time_zone))
                }
                Keyword::TIME => {
                    let with_time_zone = self.parse_keyword(Keyword::WITH);
                    if with_time_zone || self.parse_keyword(Keyword::WITHOUT) {
                        self.expect_keywords(&[Keyword::TIME, Keyword::ZONE])?;
                    }
                    Ok(DataType::Time(with_time_zone))
                }
                // Interval types can be followed by a complicated interval
                // qualifier that we don't currently support. See
                // parse_interval_literal for a taste.
                Keyword::INTERVAL => Ok(DataType::Interval),
                Keyword::REGCLASS => Ok(DataType::Regclass),
                Keyword::STRING => Ok(DataType::String),
                Keyword::TEXT => {
                    if self.consume_token(&Token::LBracket) {
                        // Note: this is postgresql-specific
                        self.expect_token(&Token::RBracket)?;
                        Ok(DataType::Array(Box::new(DataType::Text)))
                    } else {
                        Ok(DataType::Text)
                    }
                }
                Keyword::STRUCT => Ok(DataType::Struct(self.parse_struct_data_type()?)),
                Keyword::BYTEA => Ok(DataType::Bytea),
                Keyword::NUMERIC | Keyword::DECIMAL | Keyword::DEC => {
                    let (precision, scale) = self.parse_optional_precision_scale()?;
                    Ok(DataType::Decimal(precision, scale))
                }
                _ => {
                    self.prev_token();
                    let type_name = self.parse_object_name()?;
                    Ok(DataType::Custom(type_name))
                }
            },
            unexpected => self.expected("a data type name", unexpected),
        }
    }

    /// Parse `AS identifier` (or simply `identifier` if it's not a reserved keyword)
    /// Some examples with aliases: `SELECT 1 foo`, `SELECT COUNT(*) AS cnt`,
    /// `SELECT ... FROM t1 foo, t2 bar`, `SELECT ... FROM (...) AS bar`
    pub fn parse_optional_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<Ident>, ParserError> {
        let after_as = self.parse_keyword(Keyword::AS);
        match self.next_token() {
            // Accept any identifier after `AS` (though many dialects have restrictions on
            // keywords that may appear here). If there's no `AS`: don't parse keywords,
            // which may start a construct allowed in this position, to be parsed as aliases.
            // (For example, in `FROM t1 JOIN` the `JOIN` will always be parsed as a keyword,
            // not an alias.)
            Token::Word(w) if after_as || !reserved_kwds.contains(&w.keyword) => {
                Ok(Some(w.to_ident()))
            }
            // MSSQL supports single-quoted strings as aliases for columns
            // We accept them as table aliases too, although MSSQL does not.
            //
            // Note, that this conflicts with an obscure rule from the SQL
            // standard, which we don't implement:
            // https://crate.io/docs/sql-99/en/latest/chapters/07.html#character-string-literal-s
            //    "[Obscure Rule] SQL allows you to break a long <character
            //    string literal> up into two or more smaller <character string
            //    literal>s, split by a <separator> that includes a newline
            //    character. When it sees such a <literal>, your DBMS will
            //    ignore the <separator> and treat the multiple strings as
            //    a single <literal>."
            Token::SingleQuotedString(s) => Ok(Some(Ident::with_quote('\'', s))),
            not_an_ident => {
                if after_as {
                    return self.expected("an identifier after AS", not_an_ident);
                }
                self.prev_token();
                Ok(None) // no alias found
            }
        }
    }

    /// Parse `AS identifier` when the AS is describing a table-valued object,
    /// like in `... FROM generate_series(1, 10) AS t (col)`. In this case
    /// the alias is allowed to optionally name the columns in the table, in
    /// addition to the table itself.
    pub fn parse_optional_table_alias(
        &mut self,
        reserved_kwds: &[Keyword],
    ) -> Result<Option<TableAlias>, ParserError> {
        match self.parse_optional_alias(reserved_kwds)? {
            Some(name) => {
                let columns = self.parse_parenthesized_column_list(Optional)?;
                Ok(Some(TableAlias { name, columns }))
            }
            None => Ok(None),
        }
    }

    /// Parse a possibly qualified, possibly quoted identifier, e.g.
    /// `foo` or `myschema."table"
    pub fn parse_object_name(&mut self) -> Result<ObjectName, ParserError> {
        let mut idents = vec![];
        loop {
            idents.push(self.parse_identifier()?);
            if !self.consume_token(&Token::Period) {
                break;
            }
        }
        Ok(ObjectName(idents))
    }

    /// Parse identifiers strictly i.e. don't parse keywords
    pub fn parse_identifiers_non_keywords(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.peek_token() {
                Token::Word(w) => {
                    if w.keyword != Keyword::NoKeyword {
                        break;
                    }

                    idents.push(w.to_ident());
                }
                Token::EOF | Token::Eq => break,
                _ => {}
            }

            self.next_token();
        }

        Ok(idents)
    }

    /// Parse identifiers
    pub fn parse_identifiers(&mut self) -> Result<Vec<Ident>, ParserError> {
        let mut idents = vec![];
        loop {
            match self.next_token() {
                Token::Word(w) => {
                    idents.push(w.to_ident());
                }
                Token::EOF => break,
                _ => {}
            }
        }

        Ok(idents)
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a keyword)
    pub fn parse_identifier(&mut self) -> Result<Ident, ParserError> {
        match self.next_token() {
            Token::Word(w) => Ok(w.to_ident()),
            unexpected => self.expected("identifier", unexpected),
        }
    }

    /// Parse a simple one-word identifier (possibly quoted, possibly a non-reserved keyword)
    pub fn parse_identifier_non_reserved(&mut self) -> Result<Ident, ParserError> {
        match self.next_token() {
            Token::Word(w) => {
                match keywords::RESERVED_FOR_COLUMN_OR_TABLE_NAME.contains(&w.keyword) {
                    true => parser_err!(format!("syntax error at or near \"{w}\"")),
                    false => Ok(w.to_ident()),
                }
            }
            unexpected => self.expected("identifier", unexpected),
        }
    }

    /// Parse a parenthesized comma-separated list of unqualified, possibly quoted identifiers
    pub fn parse_parenthesized_column_list(
        &mut self,
        optional: IsOptional,
    ) -> Result<Vec<Ident>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let cols = self.parse_comma_separated(Parser::parse_identifier_non_reserved)?;
            self.expect_token(&Token::RParen)?;
            Ok(cols)
        } else if optional == Optional {
            Ok(vec![])
        } else {
            self.expected("a list of columns in parentheses", self.peek_token())
        }
    }

    pub fn parse_row_expr(&mut self) -> Result<Expr, ParserError> {
        Ok(Expr::Row(self.parse_token_wrapped_exprs(
            &Token::LParen,
            &Token::RParen,
        )?))
    }

    /// Parse a comma-separated list (maybe empty) from a wrapped expression
    pub fn parse_token_wrapped_exprs(
        &mut self,
        left: &Token,
        right: &Token,
    ) -> Result<Vec<Expr>, ParserError> {
        if self.consume_token(left) {
            let exprs = if self.consume_token(right) {
                vec![]
            } else {
                let exprs = self.parse_comma_separated(Parser::parse_expr)?;
                self.expect_token(right)?;
                exprs
            };
            Ok(exprs)
        } else {
            self.expected(left.to_string().as_str(), self.peek_token())
        }
    }

    pub fn parse_optional_precision(&mut self) -> Result<Option<u64>, ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            self.expect_token(&Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    pub fn parse_optional_precision_scale(
        &mut self,
    ) -> Result<(Option<u64>, Option<u64>), ParserError> {
        if self.consume_token(&Token::LParen) {
            let n = self.parse_literal_uint()?;
            let scale = if self.consume_token(&Token::Comma) {
                Some(self.parse_literal_uint()?)
            } else {
                None
            };
            self.expect_token(&Token::RParen)?;
            Ok((Some(n), scale))
        } else {
            Ok((None, None))
        }
    }

    pub fn parse_delete(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::FROM)?;
        let table_name = self.parse_object_name()?;
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Statement::Delete {
            table_name,
            selection,
        })
    }

    pub fn parse_optional_boolean(&mut self, default: bool) -> bool {
        if let Some(keyword) = self.parse_one_of_keywords(&[Keyword::TRUE, Keyword::FALSE]) {
            match keyword {
                Keyword::TRUE => true,
                Keyword::FALSE => false,
                _ => unreachable!(),
            }
        } else {
            default
        }
    }

    pub fn parse_explain(&mut self) -> Result<Statement, ParserError> {
        let mut options = ExplainOptions::default();
        let parse_explain_option = |parser: &mut Parser| -> Result<(), ParserError> {
            while let Some(keyword) = parser.parse_one_of_keywords(&[
                Keyword::VERBOSE,
                Keyword::TRACE,
                Keyword::TYPE,
                Keyword::LOGICAL,
                Keyword::PHYSICAL,
                Keyword::DISTSQL,
            ]) {
                match keyword {
                    Keyword::VERBOSE => options.verbose = parser.parse_optional_boolean(true),
                    Keyword::TRACE => options.trace = parser.parse_optional_boolean(true),
                    Keyword::TYPE => {
                        let explain_type = parser.expect_one_of_keywords(&[
                            Keyword::LOGICAL,
                            Keyword::PHYSICAL,
                            Keyword::DISTSQL,
                        ])?;
                        match explain_type {
                            Keyword::LOGICAL => options.explain_type = ExplainType::Logical,
                            Keyword::PHYSICAL => options.explain_type = ExplainType::Physical,
                            Keyword::DISTSQL => options.explain_type = ExplainType::DistSql,
                            _ => unreachable!("{}", keyword),
                        }
                    }
                    Keyword::LOGICAL => options.explain_type = ExplainType::Logical,
                    Keyword::PHYSICAL => options.explain_type = ExplainType::Physical,
                    Keyword::DISTSQL => options.explain_type = ExplainType::DistSql,
                    _ => unreachable!("{}", keyword),
                }
            }
            Ok(())
        };

        let analyze = self.parse_keyword(Keyword::ANALYZE);
        if self.consume_token(&Token::LParen) {
            self.parse_comma_separated(parse_explain_option)?;
            self.expect_token(&Token::RParen)?;
        }

        let statement = self.parse_statement()?;
        Ok(Statement::Explain {
            analyze,
            statement: Box::new(statement),
            options,
        })
    }

    /// Parse a query expression, i.e. a `SELECT` statement optionally
    /// preceded with some `WITH` CTE declarations and optionally followed
    /// by `ORDER BY`. Unlike some other parse_... methods, this one doesn't
    /// expect the initial keyword to be already consumed
    pub fn parse_query(&mut self) -> Result<Query, ParserError> {
        let with = if self.parse_keyword(Keyword::WITH) {
            Some(With {
                recursive: self.parse_keyword(Keyword::RECURSIVE),
                cte_tables: self.parse_comma_separated(Parser::parse_cte)?,
            })
        } else {
            None
        };

        let body = self.parse_query_body(0)?;

        let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_order_by_expr)?
        } else {
            vec![]
        };

        let limit = if self.parse_keyword(Keyword::LIMIT) {
            self.parse_limit()?
        } else {
            None
        };

        let offset = if self.parse_keyword(Keyword::OFFSET) {
            Some(self.parse_offset()?)
        } else {
            None
        };

        let fetch = if self.parse_keyword(Keyword::FETCH) {
            if limit.is_some() {
                return parser_err!("Cannot specify both LIMIT and FETCH".to_string());
            }
            let fetch = self.parse_fetch()?;
            if fetch.with_ties && order_by.is_empty() {
                return parser_err!(
                    "WITH TIES cannot be specified without ORDER BY clause".to_string()
                );
            }
            Some(fetch)
        } else {
            None
        };

        Ok(Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
        })
    }

    /// Parse a CTE (`alias [( col1, col2, ... )] AS (subquery)`)
    fn parse_cte(&mut self) -> Result<Cte, ParserError> {
        let name = self.parse_identifier_non_reserved()?;

        let mut cte = if self.parse_keyword(Keyword::AS) {
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias {
                name,
                columns: vec![],
            };
            Cte {
                alias,
                query,
                from: None,
            }
        } else {
            let columns = self.parse_parenthesized_column_list(Optional)?;
            self.expect_keyword(Keyword::AS)?;
            self.expect_token(&Token::LParen)?;
            let query = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            let alias = TableAlias { name, columns };
            Cte {
                alias,
                query,
                from: None,
            }
        };
        if self.parse_keyword(Keyword::FROM) {
            cte.from = Some(self.parse_identifier()?);
        }
        Ok(cte)
    }

    /// Parse a "query body", which is an expression with roughly the
    /// following grammar:
    /// ```text
    ///   query_body ::= restricted_select | '(' subquery ')' | set_operation
    ///   restricted_select ::= 'SELECT' [expr_list] [ from ] [ where ] [ groupby_having ]
    ///   subquery ::= query_body [ order_by_limit ]
    ///   set_operation ::= query_body { 'UNION' | 'EXCEPT' | 'INTERSECT' } [ 'ALL' ] query_body
    /// ```
    fn parse_query_body(&mut self, precedence: u8) -> Result<SetExpr, ParserError> {
        // We parse the expression using a Pratt parser, as in `parse_expr()`.
        // Start by parsing a restricted SELECT or a `(subquery)`:
        let mut expr = if self.parse_keyword(Keyword::SELECT) {
            SetExpr::Select(Box::new(self.parse_select()?))
        } else if self.consume_token(&Token::LParen) {
            // CTEs are not allowed here, but the parser currently accepts them
            let subquery = self.parse_query()?;
            self.expect_token(&Token::RParen)?;
            SetExpr::Query(Box::new(subquery))
        } else if self.parse_keyword(Keyword::VALUES) {
            SetExpr::Values(self.parse_values()?)
        } else {
            return self.expected(
                "SELECT, VALUES, or a subquery in the query body",
                self.peek_token(),
            );
        };

        loop {
            // The query can be optionally followed by a set operator:
            let op = self.parse_set_operator(&self.peek_token());
            let next_precedence = match op {
                // UNION and EXCEPT have the same binding power and evaluate left-to-right
                Some(SetOperator::Union) | Some(SetOperator::Except) => 10,
                // INTERSECT has higher precedence than UNION/EXCEPT
                Some(SetOperator::Intersect) => 20,
                // Unexpected token or EOF => stop parsing the query body
                None => break,
            };
            if precedence >= next_precedence {
                break;
            }
            self.next_token(); // skip past the set operator
            expr = SetExpr::SetOperation {
                left: Box::new(expr),
                op: op.unwrap(),
                all: self.parse_keyword(Keyword::ALL),
                right: Box::new(self.parse_query_body(next_precedence)?),
            };
        }

        Ok(expr)
    }

    fn parse_set_operator(&mut self, token: &Token) -> Option<SetOperator> {
        match token {
            Token::Word(w) if w.keyword == Keyword::UNION => Some(SetOperator::Union),
            Token::Word(w) if w.keyword == Keyword::EXCEPT => Some(SetOperator::Except),
            Token::Word(w) if w.keyword == Keyword::INTERSECT => Some(SetOperator::Intersect),
            _ => None,
        }
    }

    /// Parse a restricted `SELECT` statement (no CTEs / `UNION` / `ORDER BY`),
    /// assuming the initial `SELECT` was already consumed
    pub fn parse_select(&mut self) -> Result<Select, ParserError> {
        let distinct = self.parse_all_or_distinct_on()?;

        let projection = self.parse_comma_separated(Parser::parse_select_item)?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_COLUMN_ALIAS` / `RESERVED_FOR_TABLE_ALIAS`,
        // otherwise they may be parsed as an alias as part of the `projection`
        // or `from`.

        let from = if self.parse_keyword(Keyword::FROM) {
            self.parse_comma_separated(Parser::parse_table_and_joins)?
        } else {
            vec![]
        };
        let mut lateral_views = vec![];
        loop {
            if self.parse_keywords(&[Keyword::LATERAL, Keyword::VIEW]) {
                let outer = self.parse_keyword(Keyword::OUTER);
                let lateral_view = self.parse_expr()?;
                let lateral_view_name = self.parse_object_name()?;
                let lateral_col_alias = self
                    .parse_comma_separated(|parser| {
                        parser.parse_optional_alias(&[
                            Keyword::WHERE,
                            Keyword::GROUP,
                            Keyword::CLUSTER,
                            Keyword::HAVING,
                            Keyword::LATERAL,
                        ]) // This couldn't possibly be a bad idea
                    })?
                    .into_iter()
                    .flatten()
                    .collect();

                lateral_views.push(LateralView {
                    lateral_view,
                    lateral_view_name,
                    lateral_col_alias,
                    outer,
                });
            } else {
                break;
            }
        }

        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.parse_keywords(&[Keyword::GROUP, Keyword::BY]) {
            self.parse_comma_separated(Parser::parse_group_by_expr)?
        } else {
            vec![]
        };

        let having = if self.parse_keyword(Keyword::HAVING) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Select {
            distinct,
            projection,
            from,
            lateral_views,
            selection,
            group_by,
            having,
        })
    }

    pub fn parse_set(&mut self) -> Result<Statement, ParserError> {
        let modifier = self.parse_one_of_keywords(&[Keyword::SESSION, Keyword::LOCAL]);
        let variable = self.parse_identifier()?;
        if self.consume_token(&Token::Eq) || self.parse_keyword(Keyword::TO) {
            let mut values = vec![];
            loop {
                let token = self.peek_token();
                let value = match (self.parse_value(), token) {
                    (Ok(value), _) => SetVariableValue::Literal(value),
                    (Err(_), Token::Word(ident)) => SetVariableValue::Ident(ident.to_ident()),
                    (Err(_), unexpected) => self.expected("variable value", unexpected)?,
                };
                values.push(value);
                if self.consume_token(&Token::Comma) {
                    continue;
                }
                return Ok(Statement::SetVariable {
                    local: modifier == Some(Keyword::LOCAL),
                    variable,
                    value: values,
                });
            }
        } else if variable.value == "CHARACTERISTICS" {
            self.expect_keywords(&[Keyword::AS, Keyword::TRANSACTION])?;
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: true,
            })
        } else if variable.value == "TRANSACTION" && modifier.is_none() {
            if self.parse_keyword(Keyword::SNAPSHOT) {
                let snapshot_id = self.parse_value()?;
                return Ok(Statement::SetTransaction {
                    modes: vec![],
                    snapshot: Some(snapshot_id),
                    session: false,
                });
            }
            Ok(Statement::SetTransaction {
                modes: self.parse_transaction_modes()?,
                snapshot: None,
                session: false,
            })
        } else {
            self.expected("equals sign or TO", self.peek_token())
        }
    }

    /// If have `databases`,`tables`,`columns`,`schemas` and `materialized views` after show,
    /// return `Statement::ShowCommand` or `Statement::ShowColumn`,
    /// otherwise, return `Statement::ShowVariable`.
    pub fn parse_show(&mut self) -> Result<Statement, ParserError> {
        let index = self.index;
        if let Token::Word(w) = self.next_token() {
            match w.keyword {
                Keyword::TABLES => {
                    return Ok(Statement::ShowObjects(ShowObject::Table {
                        schema: self.parse_from_and_identifier()?,
                    }));
                }
                Keyword::SOURCES => {
                    return Ok(Statement::ShowObjects(ShowObject::Source {
                        schema: self.parse_from_and_identifier()?,
                    }));
                }
                Keyword::SINKS => {
                    return Ok(Statement::ShowObjects(ShowObject::Sink {
                        schema: self.parse_from_and_identifier()?,
                    }))
                }
                Keyword::DATABASES => {
                    return Ok(Statement::ShowObjects(ShowObject::Database));
                }
                Keyword::SCHEMAS => {
                    return Ok(Statement::ShowObjects(ShowObject::Schema));
                }
                Keyword::MATERIALIZED => {
                    if self.parse_keyword(Keyword::VIEWS) {
                        return Ok(Statement::ShowObjects(ShowObject::MaterializedView {
                            schema: self.parse_from_and_identifier()?,
                        }));
                    } else if self.parse_keyword(Keyword::SOURCES) {
                        return Ok(Statement::ShowObjects(ShowObject::MaterializedSource {
                            schema: self.parse_from_and_identifier()?,
                        }));
                    } else {
                        return self
                            .expected("VIEWS or SOURCES after MATERIALIZED", self.peek_token());
                    }
                }
                Keyword::COLUMNS => {
                    if self.parse_keyword(Keyword::FROM) {
                        return Ok(Statement::ShowObjects(ShowObject::Columns {
                            table: self.parse_object_name()?,
                        }));
                    } else {
                        return self.expected("from after columns", self.peek_token());
                    }
                }
                _ => {}
            }
        }
        self.index = index;
        Ok(Statement::ShowVariable {
            variable: self.parse_identifiers()?,
        })
    }

    /// Parser `from schema` after `show tables` and `show materialized views`, if not conclude
    /// `from` then use default schema name.
    pub fn parse_from_and_identifier(&mut self) -> Result<Option<Ident>, ParserError> {
        if self.parse_keyword(Keyword::FROM) {
            Ok(Some(self.parse_identifier_non_reserved()?))
        } else {
            Ok(None)
        }
    }

    pub fn parse_table_and_joins(&mut self) -> Result<TableWithJoins, ParserError> {
        let relation = self.parse_table_factor()?;

        // Note that for keywords to be properly handled here, they need to be
        // added to `RESERVED_FOR_TABLE_ALIAS`, otherwise they may be parsed as
        // a table alias.
        let mut joins = vec![];
        loop {
            let join = if self.parse_keyword(Keyword::CROSS) {
                let join_operator = if self.parse_keyword(Keyword::JOIN) {
                    JoinOperator::CrossJoin
                } else {
                    return self.expected("JOIN after CROSS", self.peek_token());
                };
                Join {
                    relation: self.parse_table_factor()?,
                    join_operator,
                }
            } else {
                let natural = self.parse_keyword(Keyword::NATURAL);
                let peek_keyword = if let Token::Word(w) = self.peek_token() {
                    w.keyword
                } else {
                    Keyword::NoKeyword
                };

                let join_operator_type = match peek_keyword {
                    Keyword::INNER | Keyword::JOIN => {
                        let _ = self.parse_keyword(Keyword::INNER);
                        self.expect_keyword(Keyword::JOIN)?;
                        JoinOperator::Inner
                    }
                    kw @ Keyword::LEFT | kw @ Keyword::RIGHT | kw @ Keyword::FULL => {
                        let _ = self.next_token();
                        let _ = self.parse_keyword(Keyword::OUTER);
                        self.expect_keyword(Keyword::JOIN)?;
                        match kw {
                            Keyword::LEFT => JoinOperator::LeftOuter,
                            Keyword::RIGHT => JoinOperator::RightOuter,
                            Keyword::FULL => JoinOperator::FullOuter,
                            _ => unreachable!(),
                        }
                    }
                    Keyword::OUTER => {
                        return self.expected("LEFT, RIGHT, or FULL", self.peek_token());
                    }
                    _ if natural => {
                        return self.expected("a join type after NATURAL", self.peek_token());
                    }
                    _ => break,
                };
                let relation = self.parse_table_factor()?;
                let join_constraint = self.parse_join_constraint(natural)?;
                Join {
                    relation,
                    join_operator: join_operator_type(join_constraint),
                }
            };
            joins.push(join);
        }
        Ok(TableWithJoins { relation, joins })
    }

    /// A table name or a parenthesized subquery, followed by optional `[AS] alias`
    pub fn parse_table_factor(&mut self) -> Result<TableFactor, ParserError> {
        if self.parse_keyword(Keyword::LATERAL) {
            // LATERAL must always be followed by a subquery.
            if !self.consume_token(&Token::LParen) {
                self.expected("subquery after LATERAL", self.peek_token())?;
            }
            self.parse_derived_table_factor(Lateral)
        } else if self.consume_token(&Token::LParen) {
            // A left paren introduces either a derived table (i.e., a subquery)
            // or a nested join. It's nearly impossible to determine ahead of
            // time which it is... so we just try to parse both.
            //
            // Here's an example that demonstrates the complexity:
            //                     /-------------------------------------------------------\
            //                     | /-----------------------------------\                 |
            //     SELECT * FROM ( ( ( (SELECT 1) UNION (SELECT 2) ) AS t1 NATURAL JOIN t2 ) )
            //                   ^ ^ ^ ^
            //                   | | | |
            //                   | | | |
            //                   | | | (4) belongs to a SetExpr::Query inside the subquery
            //                   | | (3) starts a derived table (subquery)
            //                   | (2) starts a nested join
            //                   (1) an additional set of parens around a nested join
            //

            // It can only be a subquery. We don't use `maybe_parse` so that a meaningful error can
            // be returned.
            match self.peek_token() {
                Token::Word(w)
                    if [Keyword::SELECT, Keyword::WITH, Keyword::VALUES].contains(&w.keyword) =>
                {
                    return self.parse_derived_table_factor(NotLateral);
                }
                _ => {}
            };
            // It can still be a subquery, e.g., the case (3) in the example above:
            // (SELECT 1) UNION (SELECT 2)
            // TODO: how to produce a good error message here?
            if self.peek_token() == Token::LParen {
                return_ok_if_some!(
                    self.maybe_parse(|parser| parser.parse_derived_table_factor(NotLateral))
                );
            }

            // A parsing error from `parse_derived_table_factor` indicates that the '(' we've
            // recently consumed does not start a derived table (cases 1, 2, or 4).
            // `maybe_parse` will ignore such an error and rewind to be after the opening '('.

            // Inside the parentheses we expect to find an (A) table factor
            // followed by some joins or (B) another level of nesting.
            let table_and_joins = self.parse_table_and_joins()?;

            #[allow(clippy::if_same_then_else)]
            if !table_and_joins.joins.is_empty() {
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins))) // (A)
            } else if let TableFactor::NestedJoin(_) = &table_and_joins.relation {
                // (B): `table_and_joins` (what we found inside the parentheses)
                // is a nested join `(foo JOIN bar)`, not followed by other joins.
                self.expect_token(&Token::RParen)?;
                Ok(TableFactor::NestedJoin(Box::new(table_and_joins)))
            } else {
                // The SQL spec prohibits derived tables and bare tables from
                // appearing alone in parentheses (e.g. `FROM (mytable)`)
                parser_err!(format!(
                    "Expected joined table, found: {table_and_joins}, next_token: {}",
                    self.peek_token()
                ))
            }
        } else {
            let name = self.parse_object_name()?;
            // Postgres,table-valued functions:
            if self.consume_token(&Token::LParen) {
                let (args, order_by) = self.parse_optional_args()?;
                // Table-valued functions do not support ORDER BY, should return error if it appears
                if !order_by.is_empty() {
                    return parser_err!("Table-valued functions do not support ORDER BY clauses");
                }
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::TableFunction { name, alias, args })
            } else {
                let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
                Ok(TableFactor::Table { name, alias })
            }
        }
    }

    pub fn parse_derived_table_factor(
        &mut self,
        lateral: IsLateral,
    ) -> Result<TableFactor, ParserError> {
        let subquery = Box::new(self.parse_query()?);
        self.expect_token(&Token::RParen)?;
        let alias = self.parse_optional_table_alias(keywords::RESERVED_FOR_TABLE_ALIAS)?;
        Ok(TableFactor::Derived {
            lateral: match lateral {
                Lateral => true,
                NotLateral => false,
            },
            subquery,
            alias,
        })
    }

    fn parse_join_constraint(&mut self, natural: bool) -> Result<JoinConstraint, ParserError> {
        if natural {
            Ok(JoinConstraint::Natural)
        } else if self.parse_keyword(Keyword::ON) {
            let constraint = self.parse_expr()?;
            Ok(JoinConstraint::On(constraint))
        } else if self.parse_keyword(Keyword::USING) {
            let columns = self.parse_parenthesized_column_list(Mandatory)?;
            Ok(JoinConstraint::Using(columns))
        } else {
            Ok(JoinConstraint::None)
            // self.expected("ON, or USING after JOIN", self.peek_token())
        }
    }

    /// Parse a GRANT statement.
    pub fn parse_grant(&mut self) -> Result<Statement, ParserError> {
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::TO)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let with_grant_option =
            self.parse_keywords(&[Keyword::WITH, Keyword::GRANT, Keyword::OPTION]);

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        Ok(Statement::Grant {
            privileges,
            objects,
            grantees,
            with_grant_option,
            granted_by,
        })
    }

    fn parse_grant_revoke_privileges_objects(
        &mut self,
    ) -> Result<(Privileges, GrantObjects), ParserError> {
        let privileges = if self.parse_keyword(Keyword::ALL) {
            Privileges::All {
                with_privileges_keyword: self.parse_keyword(Keyword::PRIVILEGES),
            }
        } else {
            Privileges::Actions(
                self.parse_comma_separated(Parser::parse_grant_permission)?
                    .into_iter()
                    .map(|(kw, columns)| match kw {
                        Keyword::CONNECT => Action::Connect,
                        Keyword::CREATE => Action::Create,
                        Keyword::DELETE => Action::Delete,
                        Keyword::INSERT => Action::Insert { columns },
                        Keyword::REFERENCES => Action::References { columns },
                        Keyword::SELECT => Action::Select { columns },
                        Keyword::TRIGGER => Action::Trigger,
                        Keyword::TRUNCATE => Action::Truncate,
                        Keyword::UPDATE => Action::Update { columns },
                        Keyword::USAGE => Action::Usage,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        };

        self.expect_keyword(Keyword::ON)?;

        let objects = if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::TABLES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllTablesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SEQUENCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSequencesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::SOURCES,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllSourcesInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[
            Keyword::ALL,
            Keyword::MATERIALIZED,
            Keyword::VIEWS,
            Keyword::IN,
            Keyword::SCHEMA,
        ]) {
            GrantObjects::AllMviewsInSchema {
                schemas: self.parse_comma_separated(Parser::parse_object_name)?,
            }
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            GrantObjects::Mviews(self.parse_comma_separated(Parser::parse_object_name)?)
        } else {
            let object_type = self.parse_one_of_keywords(&[
                Keyword::SEQUENCE,
                Keyword::DATABASE,
                Keyword::SCHEMA,
                Keyword::TABLE,
                Keyword::SOURCE,
            ]);
            let objects = self.parse_comma_separated(Parser::parse_object_name);
            match object_type {
                Some(Keyword::DATABASE) => GrantObjects::Databases(objects?),
                Some(Keyword::SCHEMA) => GrantObjects::Schemas(objects?),
                Some(Keyword::SEQUENCE) => GrantObjects::Sequences(objects?),
                Some(Keyword::SOURCE) => GrantObjects::Sources(objects?),
                Some(Keyword::TABLE) | None => GrantObjects::Tables(objects?),
                _ => unreachable!(),
            }
        };

        Ok((privileges, objects))
    }

    fn parse_grant_permission(&mut self) -> Result<(Keyword, Option<Vec<Ident>>), ParserError> {
        if let Some(kw) = self.parse_one_of_keywords(&[
            Keyword::CONNECT,
            Keyword::CREATE,
            Keyword::DELETE,
            Keyword::EXECUTE,
            Keyword::INSERT,
            Keyword::REFERENCES,
            Keyword::SELECT,
            Keyword::TEMPORARY,
            Keyword::TRIGGER,
            Keyword::TRUNCATE,
            Keyword::UPDATE,
            Keyword::USAGE,
        ]) {
            let columns = match kw {
                Keyword::INSERT | Keyword::REFERENCES | Keyword::SELECT | Keyword::UPDATE => {
                    let columns = self.parse_parenthesized_column_list(Optional)?;
                    if columns.is_empty() {
                        None
                    } else {
                        Some(columns)
                    }
                }
                _ => None,
            };
            Ok((kw, columns))
        } else {
            self.expected("a privilege keyword", self.peek_token())?
        }
    }

    /// Parse a REVOKE statement
    pub fn parse_revoke(&mut self) -> Result<Statement, ParserError> {
        let revoke_grant_option =
            self.parse_keywords(&[Keyword::GRANT, Keyword::OPTION, Keyword::FOR]);
        let (privileges, objects) = self.parse_grant_revoke_privileges_objects()?;

        self.expect_keyword(Keyword::FROM)?;
        let grantees = self.parse_comma_separated(Parser::parse_identifier)?;

        let granted_by = self
            .parse_keywords(&[Keyword::GRANTED, Keyword::BY])
            .then(|| self.parse_identifier().unwrap());

        let cascade = self.parse_keyword(Keyword::CASCADE);
        let restrict = self.parse_keyword(Keyword::RESTRICT);
        if cascade && restrict {
            return parser_err!("Cannot specify both CASCADE and RESTRICT in REVOKE");
        }

        Ok(Statement::Revoke {
            privileges,
            objects,
            grantees,
            granted_by,
            revoke_grant_option,
            cascade,
        })
    }

    /// Parse an INSERT statement
    pub fn parse_insert(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::INTO)?;

        let table_name = self.parse_object_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;

        let source = Box::new(self.parse_query()?);

        Ok(Statement::Insert {
            table_name,
            columns,
            source,
        })
    }

    pub fn parse_update(&mut self) -> Result<Statement, ParserError> {
        let table = self.parse_table_and_joins()?;
        self.expect_keyword(Keyword::SET)?;
        let assignments = self.parse_comma_separated(Parser::parse_assignment)?;
        let selection = if self.parse_keyword(Keyword::WHERE) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(Statement::Update {
            table,
            assignments,
            selection,
        })
    }

    /// Parse a `var = expr` assignment, used in an UPDATE statement
    pub fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let id = self.parse_identifiers_non_keywords()?;
        self.expect_token(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(Assignment { id, value })
    }

    fn parse_function_args(&mut self) -> Result<FunctionArg, ParserError> {
        if self.peek_nth_token(1) == Token::RArrow {
            let name = self.parse_identifier()?;

            self.expect_token(&Token::RArrow)?;
            let arg = self.parse_wildcard_expr()?.into();

            Ok(FunctionArg::Named { name, arg })
        } else {
            Ok(FunctionArg::Unnamed(self.parse_wildcard_expr()?.into()))
        }
    }

    pub fn parse_optional_args(
        &mut self,
    ) -> Result<(Vec<FunctionArg>, Vec<OrderByExpr>), ParserError> {
        if self.consume_token(&Token::RParen) {
            Ok((vec![], vec![]))
        } else {
            let args = self.parse_comma_separated(Parser::parse_function_args)?;
            let order_by = if self.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                self.parse_comma_separated(Parser::parse_order_by_expr)?
            } else {
                vec![]
            };
            self.expect_token(&Token::RParen)?;
            Ok((args, order_by))
        }
    }

    /// Parse a comma-delimited list of projections after SELECT
    pub fn parse_select_item(&mut self) -> Result<SelectItem, ParserError> {
        match self.parse_wildcard_expr()? {
            WildcardExpr::Expr(expr) => self
                .parse_optional_alias(keywords::RESERVED_FOR_COLUMN_ALIAS)
                .map(|alias| match alias {
                    Some(alias) => SelectItem::ExprWithAlias { expr, alias },
                    None => SelectItem::UnnamedExpr(expr),
                }),
            WildcardExpr::QualifiedWildcard(prefix) => Ok(SelectItem::QualifiedWildcard(prefix)),
            WildcardExpr::ExprQualifiedWildcard(expr, prefix) => {
                Ok(SelectItem::ExprQualifiedWildcard(expr, prefix))
            }
            WildcardExpr::Wildcard => Ok(SelectItem::Wildcard),
        }
    }

    /// Parse an expression, optionally followed by ASC or DESC (used in ORDER BY)
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parse_expr()?;

        let asc = if self.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self.parse_keywords(&[Keyword::NULLS, Keyword::FIRST]) {
            Some(true)
        } else if self.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    /// Parse a LIMIT clause
    pub fn parse_limit(&mut self) -> Result<Option<String>, ParserError> {
        if self.parse_keyword(Keyword::ALL) {
            Ok(None)
        } else {
            Ok(Some(self.parse_number_value()?))
        }
    }

    /// Parse an OFFSET clause
    pub fn parse_offset(&mut self) -> Result<String, ParserError> {
        let value = self.parse_number_value()?;
        _ = self.parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS]);
        Ok(value)
    }

    /// Parse a FETCH clause
    pub fn parse_fetch(&mut self) -> Result<Fetch, ParserError> {
        self.expect_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])?;
        let quantity = if self
            .parse_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])
            .is_some()
        {
            None
        } else {
            let quantity = self.parse_number_value()?;
            self.expect_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])?;
            Some(quantity)
        };
        let with_ties = if self.parse_keyword(Keyword::ONLY) {
            false
        } else if self.parse_keywords(&[Keyword::WITH, Keyword::TIES]) {
            true
        } else {
            return self.expected("one of ONLY or WITH TIES", self.peek_token());
        };
        Ok(Fetch {
            with_ties,
            quantity,
        })
    }

    pub fn parse_values(&mut self) -> Result<Values, ParserError> {
        let values = self.parse_comma_separated(|parser| {
            parser.expect_token(&Token::LParen)?;
            let exprs = parser.parse_comma_separated(Parser::parse_expr)?;
            parser.expect_token(&Token::RParen)?;
            Ok(exprs)
        })?;
        Ok(Values(values))
    }

    pub fn parse_start_transaction(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::TRANSACTION)?;
        Ok(Statement::StartTransaction {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_begin(&mut self) -> Result<Statement, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        Ok(Statement::BEGIN {
            modes: self.parse_transaction_modes()?,
        })
    }

    pub fn parse_transaction_modes(&mut self) -> Result<Vec<TransactionMode>, ParserError> {
        let mut modes = vec![];
        let mut required = false;
        loop {
            let mode = if self.parse_keywords(&[Keyword::ISOLATION, Keyword::LEVEL]) {
                let iso_level = if self.parse_keywords(&[Keyword::READ, Keyword::UNCOMMITTED]) {
                    TransactionIsolationLevel::ReadUncommitted
                } else if self.parse_keywords(&[Keyword::READ, Keyword::COMMITTED]) {
                    TransactionIsolationLevel::ReadCommitted
                } else if self.parse_keywords(&[Keyword::REPEATABLE, Keyword::READ]) {
                    TransactionIsolationLevel::RepeatableRead
                } else if self.parse_keyword(Keyword::SERIALIZABLE) {
                    TransactionIsolationLevel::Serializable
                } else {
                    self.expected("isolation level", self.peek_token())?
                };
                TransactionMode::IsolationLevel(iso_level)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::ONLY]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadOnly)
            } else if self.parse_keywords(&[Keyword::READ, Keyword::WRITE]) {
                TransactionMode::AccessMode(TransactionAccessMode::ReadWrite)
            } else if required {
                self.expected("transaction mode", self.peek_token())?
            } else {
                break;
            };
            modes.push(mode);
            // ANSI requires a comma after each transaction mode, but
            // PostgreSQL, for historical reasons, does not. We follow
            // PostgreSQL in making the comma optional, since that is strictly
            // more general.
            required = self.consume_token(&Token::Comma);
        }
        Ok(modes)
    }

    pub fn parse_commit(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Commit {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_rollback(&mut self) -> Result<Statement, ParserError> {
        Ok(Statement::Rollback {
            chain: self.parse_commit_rollback_chain()?,
        })
    }

    pub fn parse_commit_rollback_chain(&mut self) -> Result<bool, ParserError> {
        let _ = self.parse_one_of_keywords(&[Keyword::TRANSACTION, Keyword::WORK]);
        if self.parse_keyword(Keyword::AND) {
            let chain = !self.parse_keyword(Keyword::NO);
            self.expect_keyword(Keyword::CHAIN)?;
            Ok(chain)
        } else {
            Ok(false)
        }
    }

    fn parse_deallocate(&mut self) -> Result<Statement, ParserError> {
        let prepare = self.parse_keyword(Keyword::PREPARE);
        let name = self.parse_identifier()?;
        Ok(Statement::Deallocate { name, prepare })
    }

    fn parse_execute(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let mut parameters = vec![];
        if self.consume_token(&Token::LParen) {
            parameters = self.parse_comma_separated(Parser::parse_expr)?;
            self.expect_token(&Token::RParen)?;
        }

        Ok(Statement::Execute { name, parameters })
    }

    fn parse_prepare(&mut self) -> Result<Statement, ParserError> {
        let name = self.parse_identifier()?;

        let mut data_types = vec![];
        if self.consume_token(&Token::LParen) {
            data_types = self.parse_comma_separated(Parser::parse_data_type)?;
            self.expect_token(&Token::RParen)?;
        }

        self.expect_keyword(Keyword::AS)?;
        let statement = Box::new(self.parse_statement()?);
        Ok(Statement::Prepare {
            name,
            data_types,
            statement,
        })
    }

    fn parse_comment(&mut self) -> Result<Statement, ParserError> {
        self.expect_keyword(Keyword::ON)?;
        let token = self.next_token();

        let (object_type, object_name) = match token {
            Token::Word(w) if w.keyword == Keyword::COLUMN => {
                let object_name = self.parse_object_name()?;
                (CommentObject::Column, object_name)
            }
            Token::Word(w) if w.keyword == Keyword::TABLE => {
                let object_name = self.parse_object_name()?;
                (CommentObject::Table, object_name)
            }
            _ => self.expected("comment object_type", token)?,
        };

        self.expect_keyword(Keyword::IS)?;
        let comment = if self.parse_keyword(Keyword::NULL) {
            None
        } else {
            Some(self.parse_literal_string()?)
        };
        Ok(Statement::Comment {
            object_type,
            object_name,
            comment,
        })
    }
}

impl Word {
    pub fn to_ident(&self) -> Ident {
        Ident {
            value: self.value.clone(),
            quote_style: self.quote_style,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::run_parser_method;

    #[test]
    fn test_prev_index() {
        let sql = "SELECT version";
        run_parser_method(sql, |parser| {
            assert_eq!(parser.peek_token(), Token::make_keyword("SELECT"));
            assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
            parser.prev_token();
            assert_eq!(parser.next_token(), Token::make_keyword("SELECT"));
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            parser.prev_token();
            assert_eq!(parser.peek_token(), Token::make_word("version", None));
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            assert_eq!(parser.peek_token(), Token::EOF);
            parser.prev_token();
            assert_eq!(parser.next_token(), Token::make_word("version", None));
            assert_eq!(parser.next_token(), Token::EOF);
            assert_eq!(parser.next_token(), Token::EOF);
            parser.prev_token();
        });
    }

    #[test]
    fn test_parse_integer_min() {
        let min_bigint = "-9223372036854775808";
        run_parser_method(min_bigint, |parser| {
            assert_eq!(
                parser.parse_expr().unwrap(),
                Expr::Value(Value::Number("-9223372036854775808".to_string()))
            )
        });
    }
}
