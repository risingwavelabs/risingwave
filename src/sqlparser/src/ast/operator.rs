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

use core::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::Ident;

/// Unary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    /// Bitwise Not, e.g. `~9` (PostgreSQL-specific)
    PGBitwiseNot,
    /// Square root, e.g. `|/9` (PostgreSQL-specific)
    PGSquareRoot,
    /// Cube root, e.g. `||/27` (PostgreSQL-specific)
    PGCubeRoot,
    /// Factorial, e.g. `9!` (PostgreSQL-specific)
    PGPostfixFactorial,
    /// Factorial, e.g. `!!9` (PostgreSQL-specific)
    PGPrefixFactorial,
    /// Absolute value, e.g. `@ -9` (PostgreSQL-specific)
    PGAbs,
    /// Qualified, e.g. `OPERATOR(pg_catalog.+) 9` (PostgreSQL-specific)
    PGQualified(Box<QualifiedOperator>),
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let UnaryOperator::PGQualified(op) = self {
            return op.fmt(f);
        }
        f.write_str(match self {
            UnaryOperator::Plus => "+",
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
            UnaryOperator::PGBitwiseNot => "~",
            UnaryOperator::PGSquareRoot => "|/",
            UnaryOperator::PGCubeRoot => "||/",
            UnaryOperator::PGPostfixFactorial => "!",
            UnaryOperator::PGPrefixFactorial => "!!",
            UnaryOperator::PGAbs => "@",
            UnaryOperator::PGQualified(_) => unreachable!(),
        })
    }
}

/// Binary operators
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Concat,
    Prefix,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Spaceship,
    Eq,
    NotEq,
    And,
    Or,
    Xor,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    /// Bitwise XOR, e.g. `a # b` (PostgreSQL-specific)
    PGBitwiseXor,
    /// Bitwise shift left, e.g. `a << b` (PostgreSQL-specific)
    PGBitwiseShiftLeft,
    /// Bitwise shift right, e.g. `a >> b` (PostgreSQL-specific)
    PGBitwiseShiftRight,
    /// String matches regular expression (case sensitively), e.g. `a ~ b` (PostgreSQL-specific)
    PGRegexMatch,
    /// String matches regular expression (case insensitively), e.g. `a ~* b` (PostgreSQL-specific)
    PGRegexIMatch,
    /// String does not match regular expression (case sensitively), e.g. `a !~ b` (PostgreSQL-specific)
    PGRegexNotMatch,
    /// String does not match regular expression (case insensitively), e.g. `a !~* b` (PostgreSQL-specific)
    PGRegexNotIMatch,
    /// String matches pattern (case sensitively), e.g. `a ~~ b` (PostgreSQL-specific)
    PGLikeMatch,
    /// String matches pattern (case insensitively), e.g. `a ~~* b` (PostgreSQL-specific)
    PGILikeMatch,
    /// String does not match pattern (case sensitively), e.g. `a !~~ b` (PostgreSQL-specific)
    PGNotLikeMatch,
    /// String does not match pattern (case insensitively), e.g. `a !~~* b` (PostgreSQL-specific)
    PGNotILikeMatch,
    /// String "starts with", eg: `a ^@ b` (PostgreSQL-specific)
    Arrow,
    LongArrow,
    HashArrow,
    HashLongArrow,
    HashMinus,
    Contains,
    Contained,
    Exists,
    ExistsAny,
    ExistsAll,
    PathMatch,
    PathExists,
    PGQualified(Box<QualifiedOperator>),
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let BinaryOperator::PGQualified(op) = self {
            return op.fmt(f);
        }
        f.write_str(match self {
            BinaryOperator::Plus => "+",
            BinaryOperator::Minus => "-",
            BinaryOperator::Multiply => "*",
            BinaryOperator::Divide => "/",
            BinaryOperator::Modulo => "%",
            BinaryOperator::Concat => "||",
            BinaryOperator::Prefix => "^@",
            BinaryOperator::Gt => ">",
            BinaryOperator::Lt => "<",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Spaceship => "<=>",
            BinaryOperator::Eq => "=",
            BinaryOperator::NotEq => "<>",
            BinaryOperator::And => "AND",
            BinaryOperator::Or => "OR",
            BinaryOperator::Xor => "XOR",
            BinaryOperator::BitwiseOr => "|",
            BinaryOperator::BitwiseAnd => "&",
            BinaryOperator::BitwiseXor => "^",
            BinaryOperator::PGBitwiseXor => "#",
            BinaryOperator::PGBitwiseShiftLeft => "<<",
            BinaryOperator::PGBitwiseShiftRight => ">>",
            BinaryOperator::PGRegexMatch => "~",
            BinaryOperator::PGRegexIMatch => "~*",
            BinaryOperator::PGRegexNotMatch => "!~",
            BinaryOperator::PGRegexNotIMatch => "!~*",
            BinaryOperator::PGLikeMatch => "~~",
            BinaryOperator::PGILikeMatch => "~~*",
            BinaryOperator::PGNotLikeMatch => "!~~",
            BinaryOperator::PGNotILikeMatch => "!~~*",
            BinaryOperator::Arrow => "->",
            BinaryOperator::LongArrow => "->>",
            BinaryOperator::HashArrow => "#>",
            BinaryOperator::HashLongArrow => "#>>",
            BinaryOperator::HashMinus => "#-",
            BinaryOperator::Contains => "@>",
            BinaryOperator::Contained => "<@",
            BinaryOperator::Exists => "?",
            BinaryOperator::ExistsAny => "?|",
            BinaryOperator::ExistsAll => "?&",
            BinaryOperator::PathMatch => "@@",
            BinaryOperator::PathExists => "@?",
            BinaryOperator::PGQualified(_) => unreachable!(),
        })
    }
}

/// Qualified custom operator
/// <https://www.postgresql.org/docs/15/sql-expressions.html#SQL-EXPRESSIONS-OPERATOR-CALLS>
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct QualifiedOperator {
    pub schema: Option<Ident>,
    pub name: String,
}

impl fmt::Display for QualifiedOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OPERATOR(")?;
        if let Some(ident) = &self.schema {
            write!(f, "{ident}.")?;
        }
        f.write_str(&self.name)?;
        f.write_str(")")
    }
}
