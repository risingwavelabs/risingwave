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

//! SQL Abstract Syntax Tree (AST) types
mod data_type;
pub(crate) mod ddl;
mod operator;
mod query;
mod statement;
mod value;

#[cfg(not(feature = "std"))]
use alloc::{
    boxed::Box,
    string::{String, ToString},
    vec::Vec,
};
use core::fmt;

use itertools::Itertools;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

pub use self::data_type::{DataType, StructField};
pub use self::ddl::{
    AlterColumnOperation, AlterTableOperation, ColumnDef, ColumnOption, ColumnOptionDef,
    ReferentialAction, SourceWatermark, TableConstraint,
};
pub use self::operator::{BinaryOperator, UnaryOperator};
pub use self::query::{
    Cte, Distinct, Fetch, Join, JoinConstraint, JoinOperator, LateralView, OrderByExpr, Query,
    Select, SelectItem, SetExpr, SetOperator, TableAlias, TableFactor, TableWithJoins, Top, Values,
    With,
};
pub use self::statement::*;
pub use self::value::{DateTimeField, DollarQuotedString, TrimWhereField, Value};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

pub struct DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    slice: &'a [T],
    sep: &'static str,
}

impl<'a, T> fmt::Display for DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut delim = "";
        for t in self.slice {
            write!(f, "{}", delim)?;
            delim = self.sep;
            write!(f, "{}", t)?;
        }
        Ok(())
    }
}

pub fn display_separated<'a, T>(slice: &'a [T], sep: &'static str) -> DisplaySeparated<'a, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep }
}

pub fn display_comma_separated<T>(slice: &[T]) -> DisplaySeparated<'_, T>
where
    T: fmt::Display,
{
    DisplaySeparated { slice, sep: ", " }
}

/// An identifier, decomposed into its value or character data and the quote style.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Ident {
    /// The value of the identifier without quotes.
    pub(crate) value: String,
    /// The starting quote if any. Valid quote characters are the single quote,
    /// double quote, backtick, and opening square bracket.
    pub(crate) quote_style: Option<char>,
}

impl Ident {
    /// Create a new identifier with the given value and no quotes.
    /// the given value must not be a empty string.
    pub fn new_unchecked<S>(value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: None,
        }
    }

    /// Create a new quoted identifier with the given quote and value.
    /// the given value must not be a empty string and the given quote must be in ['\'', '"', '`',
    /// '['].
    pub fn with_quote_unchecked<S>(quote: char, value: S) -> Self
    where
        S: Into<String>,
    {
        Ident {
            value: value.into(),
            quote_style: Some(quote),
        }
    }

    /// Create a new quoted identifier with the given quote and value.
    /// returns ParserError when the given string is empty or the given quote is illegal.
    pub fn with_quote_check<S>(quote: char, value: S) -> Result<Ident, ParserError>
    where
        S: Into<String>,
    {
        let value_str = value.into();
        if value_str.is_empty() {
            return Err(ParserError::ParserError(format!(
                "zero-length delimited identifier at or near \"{value_str}\""
            )));
        }

        if !(quote == '\'' || quote == '"' || quote == '`' || quote == '[') {
            return Err(ParserError::ParserError(
                "unexpected quote style".to_string(),
            ));
        }

        Ok(Ident {
            value: value_str,
            quote_style: Some(quote),
        })
    }

    /// Value after considering quote style
    /// In certain places, double quotes can force case-sensitive, but not always
    /// e.g. session variables.
    pub fn real_value(&self) -> String {
        match self.quote_style {
            Some('"') => self.value.clone(),
            _ => self.value.to_lowercase(),
        }
    }
}

impl From<&str> for Ident {
    fn from(value: &str) -> Self {
        Ident {
            value: value.to_string(),
            quote_style: None,
        }
    }
}

impl ParseTo for Ident {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        parser.parse_identifier()
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote_style {
            Some(q) if q == '"' || q == '\'' || q == '`' => write!(f, "{}{}{}", q, self.value, q),
            Some(q) if q == '[' => write!(f, "[{}]", self.value),
            None => f.write_str(&self.value),
            _ => panic!("unexpected quote style"),
        }
    }
}

/// A name of a table, view, custom type, etc., possibly multi-part, i.e. db.schema.obj
///
/// Is is ensured to be non-empty.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ObjectName(pub Vec<Ident>);

impl ObjectName {
    pub fn real_value(&self) -> String {
        self.0
            .iter()
            .map(|ident| ident.real_value())
            .collect::<Vec<_>>()
            .join(".")
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", display_separated(&self.0, "."))
    }
}

impl ParseTo for ObjectName {
    fn parse_to(p: &mut Parser) -> Result<Self, ParserError> {
        p.parse_object_name()
    }
}

impl From<Vec<Ident>> for ObjectName {
    fn from(value: Vec<Ident>) -> Self {
        Self(value)
    }
}

/// For array type `ARRAY[..]` or `[..]`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Array {
    /// The list of expressions between brackets
    pub elem: Vec<Expr>,

    /// `true` for  `ARRAY[..]`, `false` for `[..]`
    pub named: bool,
}

impl fmt::Display for Array {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}[{}]",
            if self.named { "ARRAY" } else { "" },
            display_comma_separated(&self.elem)
        )
    }
}

/// An SQL expression of any type.
///
/// The parser does not distinguish between expressions of different types
/// (e.g. boolean vs string), so the caller must handle expressions of
/// inappropriate type, like `WHERE 1` or `SELECT 1=1`, as necessary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Expr {
    /// Identifier e.g. table name or column name
    Identifier(Ident),
    /// Multi-part identifier, e.g. `table_alias.column` or `schema.table.col`
    CompoundIdentifier(Vec<Ident>),
    /// Struct-field identifier.
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are consecutive field accesses.
    /// e.g. `(table.v1).v2` or `(table).v1.v2`
    ///
    /// It must contain parentheses to be distinguished from a [`Expr::CompoundIdentifier`].
    /// See also <https://www.postgresql.org/docs/current/rowtypes.html#ROWTYPES-ACCESSING>
    ///
    /// The left parentheses must be put at the beginning of the expression.
    /// The first parenthesized part is the `expr` part, and the rest are flattened into `idents`.
    /// e.g., `((v1).v2.v3).v4` is equivalent to `(v1).v2.v3.v4`.
    FieldIdentifier(Box<Expr>, Vec<Ident>),
    /// `IS NULL` operator
    IsNull(Box<Expr>),
    /// `IS NOT NULL` operator
    IsNotNull(Box<Expr>),
    /// `IS TRUE` operator
    IsTrue(Box<Expr>),
    /// `IS NOT TRUE` operator
    IsNotTrue(Box<Expr>),
    /// `IS FALSE` operator
    IsFalse(Box<Expr>),
    /// `IS NOT FALSE` operator
    IsNotFalse(Box<Expr>),
    /// `IS DISTINCT FROM` operator
    IsDistinctFrom(Box<Expr>, Box<Expr>),
    /// `IS NOT DISTINCT FROM` operator
    IsNotDistinctFrom(Box<Expr>, Box<Expr>),
    /// `[ NOT ] IN (val1, val2, ...)`
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `<expr> [ NOT ] BETWEEN <low> AND <high>`
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    /// Binary operation e.g. `1 + 1` or `foo > bar`
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Some operation e.g. `foo > Some(bar)`, It will be wrapped in the right side of BinaryExpr
    SomeOp(Box<Expr>),
    /// ALL operation e.g. `foo > ALL(bar)`, It will be wrapped in the right side of BinaryExpr
    AllOp(Box<Expr>),
    /// Unary operation e.g. `NOT foo`
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// CAST an expression to a different data type e.g. `CAST(foo AS VARCHAR)`
    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// TRY_CAST an expression to a different data type e.g. `TRY_CAST(foo AS VARCHAR)`
    //  this differs from CAST in the choice of how to implement invalid conversions
    TryCast {
        expr: Box<Expr>,
        data_type: DataType,
    },
    /// AT TIME ZONE converts `timestamp without time zone` to/from `timestamp with time zone` with
    /// explicitly specified zone
    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: String,
    },
    /// EXTRACT(DateTimeField FROM <expr>)
    Extract { field: String, expr: Box<Expr> },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        expr: Box<Expr>,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
    },
    /// OVERLAY(<expr> PLACING <expr> FROM <expr> [ FOR <expr> ])
    Overlay {
        expr: Box<Expr>,
        new_substring: Box<Expr>,
        start: Box<Expr>,
        count: Option<Box<Expr>>,
    },
    /// TRIM([BOTH | LEADING | TRAILING] <expr> [FROM <expr>])\
    /// Or\
    /// TRIM(<expr>)
    Trim {
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhereField, Box<Expr>)>,
    },
    /// `expr COLLATE collation`
    Collate {
        expr: Box<Expr>,
        collation: ObjectName,
    },
    /// Nested expression e.g. `(foo > bar)` or `(1)`
    Nested(Box<Expr>),
    /// A literal value, such as string, number, date or NULL
    Value(Value),
    /// Parameter Symbol e.g. `$1`, `$1::int`
    Parameter { index: u64 },
    /// A constant of form `<data_type> 'value'`.
    /// This can represent ANSI SQL `DATE`, `TIME`, and `TIMESTAMP` literals (such as `DATE
    /// '2020-01-01'`), as well as constants of other types (a non-standard PostgreSQL extension).
    TypedString { data_type: DataType, value: String },
    /// Scalar function call e.g. `LEFT(foo, 5)`
    Function(Function),
    /// `CASE [<operand>] WHEN <condition> THEN <result> ... [ELSE <result>] END`
    ///
    /// Note we only recognize a complete single expression as `<condition>`,
    /// not `< 0` nor `1, 2, 3` as allowed in a `<simple when clause>` per
    /// <https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html#simple-when-clause>
    Case {
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// An exists expression `EXISTS(SELECT ...)`, used in expressions like
    /// `WHERE EXISTS (SELECT ...)`.
    Exists(Box<Query>),
    /// A parenthesized subquery `(SELECT ...)`, used in expression like
    /// `SELECT (subquery) AS x` or `WHERE (subquery) = x`
    Subquery(Box<Query>),
    /// The `GROUPING SETS` expr.
    GroupingSets(Vec<Vec<Expr>>),
    /// The `CUBE` expr.
    Cube(Vec<Vec<Expr>>),
    /// The `ROLLUP` expr.
    Rollup(Vec<Vec<Expr>>),
    /// The `ROW` expr. The `ROW` keyword can be omitted,
    Row(Vec<Expr>),
    /// The `ARRAY` expr. Alternative syntax for `ARRAY` is by utilizing curly braces,
    /// e.g. {1, 2, 3},
    Array(Array),
    /// An array index expression e.g. `(ARRAY[1, 2])[1]` or `(current_schemas(FALSE))[1]`
    ArrayIndex { obj: Box<Expr>, index: Box<Expr> },
}

impl fmt::Display for Expr {
    #[expect(clippy::disallowed_methods, reason = "use zip_eq")]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Identifier(s) => write!(f, "{}", s),
            Expr::CompoundIdentifier(s) => write!(f, "{}", display_separated(s, ".")),
            Expr::FieldIdentifier(ast, s) => write!(f, "({}).{}", ast, display_separated(s, ".")),
            Expr::IsNull(ast) => write!(f, "{} IS NULL", ast),
            Expr::IsNotNull(ast) => write!(f, "{} IS NOT NULL", ast),
            Expr::IsTrue(ast) => write!(f, "{} IS TRUE", ast),
            Expr::IsNotTrue(ast) => write!(f, "{} IS NOT TRUE", ast),
            Expr::IsFalse(ast) => write!(f, "{} IS FALSE", ast),
            Expr::IsNotFalse(ast) => write!(f, "{} IS NOT FALSE", ast),
            Expr::InList {
                expr,
                list,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                display_comma_separated(list)
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => write!(
                f,
                "{} {}IN ({})",
                expr,
                if *negated { "NOT " } else { "" },
                subquery
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => write!(
                f,
                "{} {}BETWEEN {} AND {}",
                expr,
                if *negated { "NOT " } else { "" },
                low,
                high
            ),
            Expr::BinaryOp { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::SomeOp(expr) => write!(f, "SOME({})", expr),
            Expr::AllOp(expr) => write!(f, "ALL({})", expr),
            Expr::UnaryOp { op, expr } => {
                if op == &UnaryOperator::PGPostfixFactorial {
                    write!(f, "{}{}", expr, op)
                } else {
                    write!(f, "{} {}", op, expr)
                }
            }
            Expr::Cast { expr, data_type } => write!(f, "CAST({} AS {})", expr, data_type),
            Expr::TryCast { expr, data_type } => write!(f, "TRY_CAST({} AS {})", expr, data_type),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => write!(f, "{} AT TIME ZONE '{}'", timestamp, time_zone),
            Expr::Extract { field, expr } => write!(f, "EXTRACT({} FROM {})", field, expr),
            Expr::Collate { expr, collation } => write!(f, "{} COLLATE {}", expr, collation),
            Expr::Nested(ast) => write!(f, "({})", ast),
            Expr::Value(v) => write!(f, "{}", v),
            Expr::Parameter { index } => write!(f, "${}", index),
            Expr::TypedString { data_type, value } => {
                write!(f, "{}", data_type)?;
                write!(f, " '{}'", &value::escape_single_quote_string(value))
            }
            Expr::Function(fun) => write!(f, "{}", fun),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                write!(f, "CASE")?;
                if let Some(operand) = operand {
                    write!(f, " {}", operand)?;
                }
                for (c, r) in conditions.iter().zip_eq(results) {
                    write!(f, " WHEN {} THEN {}", c, r)?;
                }

                if let Some(else_result) = else_result {
                    write!(f, " ELSE {}", else_result)?;
                }
                write!(f, " END")
            }
            Expr::Exists(s) => write!(f, "EXISTS ({})", s),
            Expr::Subquery(s) => write!(f, "({})", s),
            Expr::GroupingSets(sets) => {
                write!(f, "GROUPING SETS (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    write!(f, "({})", display_comma_separated(set))?;
                }
                write!(f, ")")
            }
            Expr::Cube(sets) => {
                write!(f, "CUBE (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Rollup(sets) => {
                write!(f, "ROLLUP (")?;
                let mut sep = "";
                for set in sets {
                    write!(f, "{}", sep)?;
                    sep = ", ";
                    if set.len() == 1 {
                        write!(f, "{}", set[0])?;
                    } else {
                        write!(f, "({})", display_comma_separated(set))?;
                    }
                }
                write!(f, ")")
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => {
                write!(f, "SUBSTRING({}", expr)?;
                if let Some(from_part) = substring_from {
                    write!(f, " FROM {}", from_part)?;
                }
                if let Some(from_part) = substring_for {
                    write!(f, " FOR {}", from_part)?;
                }

                write!(f, ")")
            }
            Expr::Overlay {
                expr,
                new_substring,
                start,
                count,
            } => {
                write!(f, "OVERLAY({}", expr)?;
                write!(f, " PLACING {}", new_substring)?;
                write!(f, " FROM {}", start)?;

                if let Some(count_expr) = count {
                    write!(f, " FOR {}", count_expr)?;
                }

                write!(f, ")")
            }
            Expr::IsDistinctFrom(a, b) => write!(f, "{} IS DISTINCT FROM {}", a, b),
            Expr::IsNotDistinctFrom(a, b) => write!(f, "{} IS NOT DISTINCT FROM {}", a, b),
            Expr::Trim { expr, trim_where } => {
                write!(f, "TRIM(")?;
                if let Some((ident, trim_char)) = trim_where {
                    write!(f, "{} {} FROM {}", ident, trim_char, expr)?;
                } else {
                    write!(f, "{}", expr)?;
                }

                write!(f, ")")
            }
            Expr::Row(exprs) => write!(
                f,
                "ROW({})",
                exprs
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .as_slice()
                    .join(", ")
            ),
            Expr::ArrayIndex { obj, index } => {
                write!(f, "{}[{}]", obj, index)?;
                Ok(())
            }
            Expr::Array(exprs) => write!(f, "{}", exprs),
        }
    }
}

/// A window specification (i.e. `OVER (PARTITION BY .. ORDER BY .. etc.)`)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut delim = "";
        if !self.partition_by.is_empty() {
            delim = " ";
            write!(
                f,
                "PARTITION BY {}",
                display_comma_separated(&self.partition_by)
            )?;
        }
        if !self.order_by.is_empty() {
            f.write_str(delim)?;
            delim = " ";
            write!(f, "ORDER BY {}", display_comma_separated(&self.order_by))?;
        }
        if let Some(window_frame) = &self.window_frame {
            f.write_str(delim)?;
            window_frame.fmt(f)?;
        }
        Ok(())
    }
}

/// Specifies the data processed by a window function, e.g.
/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
///
/// Note: The parser does not validate the specified bounds; the caller should
/// reject invalid bounds like `ROWS UNBOUNDED FOLLOWING` before execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    /// The right bound of the `BETWEEN .. AND` clause. The end bound of `None`
    /// indicates the shorthand form (e.g. `ROWS 1 PRECEDING`), which must
    /// behave the same as `end_bound = WindowFrameBound::CurrentRow`.
    pub end_bound: Option<WindowFrameBound>,
    // TBD: EXCLUDE
}

impl Default for WindowFrame {
    /// returns default value for window frame
    ///
    /// see <https://www.sqlite.org/windowfunctions.html#frame_specifications>
    fn default() -> Self {
        Self {
            units: WindowFrameUnits::Range,
            start_bound: WindowFrameBound::Preceding(None),
            end_bound: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

impl fmt::Display for WindowFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(end_bound) = &self.end_bound {
            write!(
                f,
                "{} BETWEEN {} AND {}",
                self.units, self.start_bound, end_bound
            )
        } else {
            write!(f, "{} {}", self.units, self.start_bound)
        }
    }
}

impl fmt::Display for WindowFrameUnits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        })
    }
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<u64>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<u64>),
}

impl fmt::Display for WindowFrameBound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowFrameBound::CurrentRow => f.write_str("CURRENT ROW"),
            WindowFrameBound::Preceding(None) => f.write_str("UNBOUNDED PRECEDING"),
            WindowFrameBound::Following(None) => f.write_str("UNBOUNDED FOLLOWING"),
            WindowFrameBound::Preceding(Some(n)) => write!(f, "{} PRECEDING", n),
            WindowFrameBound::Following(Some(n)) => write!(f, "{} FOLLOWING", n),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AddDropSync {
    ADD,
    DROP,
    SYNC,
}

impl fmt::Display for AddDropSync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddDropSync::SYNC => f.write_str("SYNC PARTITIONS"),
            AddDropSync::DROP => f.write_str("DROP PARTITIONS"),
            AddDropSync::ADD => f.write_str("ADD PARTITIONS"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowObject {
    Table { schema: Option<Ident> },
    InternalTable { schema: Option<Ident> },
    Database,
    Schema,
    View { schema: Option<Ident> },
    MaterializedView { schema: Option<Ident> },
    Source { schema: Option<Ident> },
    Sink { schema: Option<Ident> },
    Columns { table: ObjectName },
}

impl fmt::Display for ShowObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_schema(schema: &Option<Ident>) -> String {
            if let Some(schema) = schema {
                format!(" FROM {}", schema.value)
            } else {
                "".to_string()
            }
        }

        match self {
            ShowObject::Database => f.write_str("DATABASES"),
            ShowObject::Schema => f.write_str("SCHEMAS"),
            ShowObject::Table { schema } => {
                write!(f, "TABLES{}", fmt_schema(schema))
            }
            ShowObject::InternalTable { schema } => {
                write!(f, "INTERNAL TABLES{}", fmt_schema(schema))
            }
            ShowObject::View { schema } => {
                write!(f, "VIEWS{}", fmt_schema(schema))
            }
            ShowObject::MaterializedView { schema } => {
                write!(f, "MATERIALIZED VIEWS{}", fmt_schema(schema))
            }
            ShowObject::Source { schema } => write!(f, "SOURCES{}", fmt_schema(schema)),
            ShowObject::Sink { schema } => write!(f, "SINKS{}", fmt_schema(schema)),
            ShowObject::Columns { table } => write!(f, "COLUMNS FROM {}", table),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowCreateType {
    Table,
    MaterializedView,
    View,
    Index,
    Source,
    Sink,
    Function,
}

impl fmt::Display for ShowCreateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShowCreateType::Table => f.write_str("TABLE"),
            ShowCreateType::MaterializedView => f.write_str("MATERIALIZED VIEW"),
            ShowCreateType::View => f.write_str("VIEW"),
            ShowCreateType::Index => f.write_str("INDEX"),
            ShowCreateType::Source => f.write_str("SOURCE"),
            ShowCreateType::Sink => f.write_str("SINK"),
            ShowCreateType::Function => f.write_str("FUNCTION"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CommentObject {
    Column,
    Table,
}

impl fmt::Display for CommentObject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommentObject::Column => f.write_str("COLUMN"),
            CommentObject::Table => f.write_str("TABLE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ExplainType {
    Logical,
    Physical,
    DistSql,
}

impl fmt::Display for ExplainType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainType::Logical => f.write_str("Logical"),
            ExplainType::Physical => f.write_str("Physical"),
            ExplainType::DistSql => f.write_str("DistSQL"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ExplainOptions {
    /// Display additional information regarding the plan.
    pub verbose: bool,
    // Trace plan transformation of the optimizer step by step
    pub trace: bool,
    // explain's plan type
    pub explain_type: ExplainType,
}
impl Default for ExplainOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            trace: false,
            explain_type: ExplainType::Physical,
        }
    }
}
impl fmt::Display for ExplainOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let default = Self::default();
        if *self == default {
            Ok(())
        } else {
            let mut option_strs = vec![];
            if self.verbose {
                option_strs.push("VERBOSE".to_string());
            }
            if self.trace {
                option_strs.push("TRACE".to_string());
            }
            if self.explain_type == default.explain_type {
                option_strs.push(self.explain_type.to_string());
            }
            write!(f, "{}", option_strs.iter().format(","))
        }
    }
}

/// A top-level statement (SELECT, INSERT, CREATE, etc.)
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Statement {
    /// Analyze (Hive)
    Analyze { table_name: ObjectName },
    /// Truncate (Hive)
    Truncate { table_name: ObjectName },
    /// SELECT
    Query(Box<Query>),
    /// INSERT
    Insert {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// A SQL query that specifies what to insert
        source: Box<Query>,
        /// Define output of this insert statement
        returning: Vec<SelectItem>,
    },
    Copy {
        /// TABLE
        table_name: ObjectName,
        /// COLUMNS
        columns: Vec<Ident>,
        /// VALUES a vector of values to be copied
        values: Vec<Option<String>>,
    },
    /// UPDATE
    Update {
        /// TABLE
        table_name: ObjectName,
        /// Column assignments
        assignments: Vec<Assignment>,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Vec<SelectItem>,
    },
    /// DELETE
    Delete {
        /// FROM
        table_name: ObjectName,
        /// WHERE
        selection: Option<Expr>,
        /// RETURNING
        returning: Vec<SelectItem>,
    },
    /// CREATE VIEW
    CreateView {
        or_replace: bool,
        materialized: bool,
        /// View name
        name: ObjectName,
        columns: Vec<Ident>,
        query: Box<Query>,
        emit_mode: Option<EmitMode>,
        with_options: Vec<SqlOption>,
    },
    /// CREATE TABLE
    CreateTable {
        or_replace: bool,
        temporary: bool,
        if_not_exists: bool,
        /// Table name
        name: ObjectName,
        /// Optional schema
        columns: Vec<ColumnDef>,
        constraints: Vec<TableConstraint>,
        with_options: Vec<SqlOption>,
        /// Optional schema of the external source with which the table is created
        source_schema: Option<SourceSchema>,
        /// `AS ( query )`
        query: Option<Box<Query>>,
    },
    /// CREATE INDEX
    CreateIndex {
        /// index name
        name: ObjectName,
        table_name: ObjectName,
        columns: Vec<OrderByExpr>,
        include: Vec<Ident>,
        distributed_by: Vec<Ident>,
        unique: bool,
        if_not_exists: bool,
    },
    /// CREATE SOURCE
    CreateSource { stmt: CreateSourceStatement },
    /// CREATE SINK
    CreateSink { stmt: CreateSinkStatement },
    /// CREATE FUNCTION
    ///
    /// Postgres: https://www.postgresql.org/docs/15/sql-createfunction.html
    CreateFunction {
        or_replace: bool,
        temporary: bool,
        name: ObjectName,
        args: Option<Vec<OperateFunctionArg>>,
        return_type: Option<DataType>,
        /// Optional parameters.
        params: CreateFunctionBody,
    },
    /// ALTER TABLE
    AlterTable {
        /// Table name
        name: ObjectName,
        operation: AlterTableOperation,
    },
    /// DESCRIBE TABLE OR SOURCE
    Describe {
        /// Table or Source name
        name: ObjectName,
    },
    /// SHOW OBJECT COMMAND
    ShowObjects(ShowObject),
    /// SHOW CREATE COMMAND
    ShowCreateObject {
        /// Show create object type
        create_type: ShowCreateType,
        /// Show create object name
        name: ObjectName,
    },
    /// DROP
    Drop(DropStatement),
    /// DROP Function
    DropFunction {
        if_exists: bool,
        /// One or more function to drop
        func_desc: Vec<DropFunctionDesc>,
        /// `CASCADE` or `RESTRICT`
        option: Option<ReferentialAction>,
    },
    /// SET <variable>
    ///
    /// Note: this is not a standard SQL statement, but it is supported by at
    /// least MySQL and PostgreSQL. Not all MySQL-specific syntactic forms are
    /// supported yet.
    SetVariable {
        local: bool,
        variable: Ident,
        value: Vec<SetVariableValue>,
    },
    /// SHOW <variable>
    ///
    /// Note: this is a PostgreSQL-specific statement.
    ShowVariable { variable: Vec<Ident> },
    /// `START TRANSACTION ...`
    StartTransaction { modes: Vec<TransactionMode> },
    /// `BEGIN [ TRANSACTION | WORK ]`
    BEGIN { modes: Vec<TransactionMode> },
    /// ABORT
    Abort,
    /// `SET TRANSACTION ...`
    SetTransaction {
        modes: Vec<TransactionMode>,
        snapshot: Option<Value>,
        session: bool,
    },
    /// `COMMENT ON ...`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Comment {
        object_type: CommentObject,
        object_name: ObjectName,
        comment: Option<String>,
    },
    /// `COMMIT [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Commit { chain: bool },
    /// `ROLLBACK [ TRANSACTION | WORK ] [ AND [ NO ] CHAIN ]`
    Rollback { chain: bool },
    /// CREATE SCHEMA
    CreateSchema {
        schema_name: ObjectName,
        if_not_exists: bool,
    },
    /// CREATE DATABASE
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
    },
    /// GRANT privileges ON objects TO grantees
    Grant {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        with_grant_option: bool,
        granted_by: Option<Ident>,
    },
    /// REVOKE privileges ON objects FROM grantees
    Revoke {
        privileges: Privileges,
        objects: GrantObjects,
        grantees: Vec<Ident>,
        granted_by: Option<Ident>,
        revoke_grant_option: bool,
        cascade: bool,
    },
    /// `DEALLOCATE [ PREPARE ] { name | ALL }`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Deallocate { name: Ident, prepare: bool },
    /// `EXECUTE name [ ( parameter [, ...] ) ]`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Execute { name: Ident, parameters: Vec<Expr> },
    /// `PREPARE name [ ( data_type [, ...] ) ] AS statement`
    ///
    /// Note: this is a PostgreSQL-specific statement.
    Prepare {
        name: Ident,
        data_types: Vec<DataType>,
        statement: Box<Statement>,
    },
    /// EXPLAIN / DESCRIBE for select_statement
    Explain {
        /// Carry out the command and show actual run times and other statistics.
        analyze: bool,
        /// A SQL query that specifies what to explain
        statement: Box<Statement>,
        /// options of the explain statement
        options: ExplainOptions,
    },
    /// CREATE USER
    CreateUser(CreateUserStatement),
    /// ALTER USER
    AlterUser(AlterUserStatement),
    /// ALTER SYSTEM SET configuration_parameter { TO | = } { value | 'value' | DEFAULT }
    AlterSystem {
        param: Ident,
        value: SetVariableValue,
    },
    /// FLUSH the current barrier.
    ///
    /// Note: RisingWave specific statement.
    Flush,
}

impl fmt::Display for Statement {
    // Clippy thinks this function is too complicated, but it is painful to
    // split up without extracting structs for each `Statement` variant.
    #[allow(clippy::cognitive_complexity)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Explain {
                analyze,
                statement,
                options,
            } => {
                write!(f, "EXPLAIN ")?;

                if *analyze {
                    write!(f, "ANALYZE ")?;
                }
                options.fmt(f)?;

                write!(f, "{}", statement)
            }
            Statement::Query(s) => write!(f, "{}", s),
            Statement::Truncate { table_name } => {
                write!(f, "TRUNCATE TABLE {}", table_name)?;
                Ok(())
            }
            Statement::Analyze { table_name } => {
                write!(f, "ANALYZE TABLE {}", table_name)?;
                Ok(())
            }
            Statement::Describe { name } => {
                write!(f, "DESCRIBE {}", name)?;
                Ok(())
            }
            Statement::ShowObjects(show_object) => {
                write!(f, "SHOW {}", show_object)?;
                Ok(())
            }
            Statement::ShowCreateObject{ create_type: show_type, name } => {
                write!(f, "SHOW CREATE {} {}", show_type, name)?;
                Ok(())
            }
            Statement::Insert {
                table_name,
                columns,
                source,
                returning,
            } => {
                write!(f, "INSERT INTO {table_name} ", table_name = table_name,)?;
                if !columns.is_empty() {
                    write!(f, "({}) ", display_comma_separated(columns))?;
                }
                write!(f, "{}", source)?;
                if !returning.is_empty() {
                    write!(f, " RETURNING ({})", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::Copy {
                table_name,
                columns,
                values,
            } => {
                write!(f, "COPY {}", table_name)?;
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " FROM stdin; ")?;
                if !values.is_empty() {
                    writeln!(f)?;
                    let mut delim = "";
                    for v in values {
                        write!(f, "{}", delim)?;
                        delim = "\t";
                        if let Some(v) = v {
                            write!(f, "{}", v)?;
                        } else {
                            write!(f, "\\N")?;
                        }
                    }
                }
                write!(f, "\n\\.")
            }
            Statement::Update {
                table_name,
                assignments,
                selection,
                returning,
            } => {
                write!(f, "UPDATE {}", table_name)?;
                if !assignments.is_empty() {
                    write!(f, " SET {}", display_comma_separated(assignments))?;
                }
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                if !returning.is_empty() {
                    write!(f, " RETURNING ({})", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::Delete {
                table_name,
                selection,
                returning,
            } => {
                write!(f, "DELETE FROM {}", table_name)?;
                if let Some(selection) = selection {
                    write!(f, " WHERE {}", selection)?;
                }
                if !returning.is_empty() {
                    write!(f, " RETURNING {}", display_comma_separated(returning))?;
                }
                Ok(())
            }
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
            } => {
                write!(f, "CREATE DATABASE")?;
                if *if_not_exists {
                    write!(f, " IF NOT EXISTS")?;
                }
                write!(f, " {}", db_name)?;
                Ok(())
            }
            Statement::CreateFunction {
                or_replace,
                temporary,
                name,
                args,
                return_type,
                params,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{temp}FUNCTION {name}",
                    temp = if *temporary { "TEMPORARY " } else { "" },
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                )?;
                if let Some(args) = args {
                    write!(f, "({})", display_comma_separated(args))?;
                }
                if let Some(return_type) = return_type {
                    write!(f, " RETURNS {}", return_type)?;
                }
                write!(f, "{params}")?;
                Ok(())
            }
            Statement::CreateView {
                name,
                or_replace,
                columns,
                query,
                materialized,
                with_options,
                emit_mode,
            } => {
                write!(
                    f,
                    "CREATE {or_replace}{materialized}VIEW {name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    materialized = if *materialized { "MATERIALIZED " } else { "" },
                    name = name
                )?;
                if let Some(emit_mode) = emit_mode {
                    write!(f, " EMIT {}", emit_mode)?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if !columns.is_empty() {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
                write!(f, " AS {}", query)
            }
            Statement::CreateTable {
                name,
                columns,
                constraints,
                with_options,
                or_replace,
                if_not_exists,
                temporary,
                source_schema,
                query,
            } => {
                // We want to allow the following options
                // Empty column list, allowed by PostgreSQL:
                //   `CREATE TABLE t ()`
                // No columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t AS SELECT a from t2`
                // Columns provided for CREATE TABLE AS:
                //   `CREATE TABLE t (a INT) AS SELECT a from t2`
                write!(
                    f,
                    "CREATE {or_replace}{temporary}TABLE {if_not_exists}{name}",
                    or_replace = if *or_replace { "OR REPLACE " } else { "" },
                    if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    temporary = if *temporary { "TEMPORARY " } else { "" },
                    name = name,
                )?;
                if !columns.is_empty() || !constraints.is_empty() {
                    write!(f, " ({}", display_comma_separated(columns))?;
                    if !columns.is_empty() && !constraints.is_empty() {
                        write!(f, ", ")?;
                    }
                    write!(f, "{})", display_comma_separated(constraints))?;
                } else if query.is_none() {
                    // PostgreSQL allows `CREATE TABLE t ();`, but requires empty parens
                    write!(f, " ()")?;
                }
                if !with_options.is_empty() {
                    write!(f, " WITH ({})", display_comma_separated(with_options))?;
                }
                if let Some(source_schema) = source_schema {
                    write!(f, " ROW FORMAT {}", source_schema)?;
                }
                if let Some(query) = query {
                    write!(f, " AS {}", query)?;
                }
                Ok(())
            }
            Statement::CreateIndex {
                name,
                table_name,
                columns,
                include,
                distributed_by,
                unique,
                if_not_exists,
            } => write!(
                f,
                "CREATE {unique}INDEX {if_not_exists}{name} ON {table_name}({columns}){include}{distributed_by}",
                unique = if *unique { "UNIQUE " } else { "" },
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = name,
                table_name = table_name,
                columns = display_separated(columns, ","),
                include = if include.is_empty() {
                    "".to_string()
                } else {
                    format!(" INCLUDE({})", display_separated(include, ","))
                },
                distributed_by = if distributed_by.is_empty() {
                    "".to_string()
                } else {
                    format!(" DISTRIBUTED BY({})", display_separated(distributed_by, ","))
                }
            ),
            Statement::CreateSource {
                stmt,
            } => write!(
                f,
                "CREATE SOURCE {}",
                stmt,
            ),
            Statement::CreateSink { stmt } => write!(f, "CREATE SINK {}", stmt,),
            Statement::AlterTable { name, operation } => {
                write!(f, "ALTER TABLE {} {}", name, operation)
            }
            Statement::Drop(stmt) => write!(f, "DROP {}", stmt),
            Statement::DropFunction {
                if_exists,
                func_desc,
                option,
            } => {
                write!(
                    f,
                    "DROP FUNCTION{} {}",
                    if *if_exists { " IF EXISTS" } else { "" },
                    display_comma_separated(func_desc),
                )?;
                if let Some(op) = option {
                    write!(f, " {}", op)?;
                }
                Ok(())
            }
            Statement::SetVariable {
                local,
                variable,
                value,
            } => {
                f.write_str("SET ")?;
                if *local {
                    f.write_str("LOCAL ")?;
                }
                write!(
                    f,
                    "{name} = {value}",
                    name = variable,
                    value = display_comma_separated(value)
                )
            }
            Statement::ShowVariable { variable } => {
                write!(f, "SHOW")?;
                if !variable.is_empty() {
                    write!(f, " {}", display_separated(variable, " "))?;
                }
                Ok(())
            }
            Statement::StartTransaction { modes } => {
                write!(f, "START TRANSACTION")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
            Statement::Abort => {
                write!(f, "ABORT")?;
                Ok(())
            }
            Statement::SetTransaction {
                modes,
                snapshot,
                session,
            } => {
                if *session {
                    write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION")?;
                } else {
                    write!(f, "SET TRANSACTION")?;
                }
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                if let Some(snapshot_id) = snapshot {
                    write!(f, " SNAPSHOT {}", snapshot_id)?;
                }
                Ok(())
            }
            Statement::Commit { chain } => {
                write!(f, "COMMIT{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::Rollback { chain } => {
                write!(f, "ROLLBACK{}", if *chain { " AND CHAIN" } else { "" },)
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
            } => write!(
                f,
                "CREATE SCHEMA {if_not_exists}{name}",
                if_not_exists = if *if_not_exists { "IF NOT EXISTS " } else { "" },
                name = schema_name
            ),
            Statement::Grant {
                privileges,
                objects,
                grantees,
                with_grant_option,
                granted_by,
            } => {
                write!(f, "GRANT {} ", privileges)?;
                write!(f, "ON {} ", objects)?;
                write!(f, "TO {}", display_comma_separated(grantees))?;
                if *with_grant_option {
                    write!(f, " WITH GRANT OPTION")?;
                }
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                Ok(())
            }
            Statement::Revoke {
                privileges,
                objects,
                grantees,
                granted_by,
                revoke_grant_option,
                cascade,
            } => {
                write!(
                    f,
                    "REVOKE {}{} ",
                    if *revoke_grant_option {
                        "GRANT OPTION FOR "
                    } else {
                        ""
                    },
                    privileges
                )?;
                write!(f, "ON {} ", objects)?;
                write!(f, "FROM {}", display_comma_separated(grantees))?;
                if let Some(grantor) = granted_by {
                    write!(f, " GRANTED BY {}", grantor)?;
                }
                write!(f, " {}", if *cascade { "CASCADE" } else { "RESTRICT" })?;
                Ok(())
            }
            Statement::Deallocate { name, prepare } => write!(
                f,
                "DEALLOCATE {prepare}{name}",
                prepare = if *prepare { "PREPARE " } else { "" },
                name = name,
            ),
            Statement::Execute { name, parameters } => {
                write!(f, "EXECUTE {}", name)?;
                if !parameters.is_empty() {
                    write!(f, "({})", display_comma_separated(parameters))?;
                }
                Ok(())
            }
            Statement::Prepare {
                name,
                data_types,
                statement,
            } => {
                write!(f, "PREPARE {} ", name)?;
                if !data_types.is_empty() {
                    write!(f, "({}) ", display_comma_separated(data_types))?;
                }
                write!(f, "AS {}", statement)
            }
            Statement::Comment {
                object_type,
                object_name,
                comment,
            } => {
                write!(f, "COMMENT ON {} {} IS ", object_type, object_name)?;
                if let Some(c) = comment {
                    write!(f, "'{}'", c)
                } else {
                    write!(f, "NULL")
                }
            }
            Statement::CreateUser(statement) => {
                write!(f, "CREATE USER {}", statement)
            }
            Statement::AlterUser(statement) => {
                write!(f, "ALTER USER {}", statement)
            }
            Statement::AlterSystem{param, value} => {
                f.write_str("ALTER SYSTEM SET ")?;
                write!(
                    f,
                    "{param} = {value}",
                )
            }
            Statement::Flush => {
                write!(f, "FLUSH")
            }
            Statement::BEGIN { modes } => {
                write!(f, "BEGIN")?;
                if !modes.is_empty() {
                    write!(f, " {}", display_comma_separated(modes))?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[non_exhaustive]
pub enum OnInsert {
    /// ON DUPLICATE KEY UPDATE (MySQL when the key already exists, then execute an update instead)
    DuplicateKeyUpdate(Vec<Assignment>),
}

impl fmt::Display for OnInsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKeyUpdate(expr) => write!(
                f,
                " ON DUPLICATE KEY UPDATE {}",
                display_comma_separated(expr)
            ),
        }
    }
}

/// Privileges granted in a GRANT statement or revoked in a REVOKE statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Privileges {
    /// All privileges applicable to the object type
    All {
        /// Optional keyword from the spec, ignored in practice
        with_privileges_keyword: bool,
    },
    /// Specific privileges (e.g. `SELECT`, `INSERT`)
    Actions(Vec<Action>),
}

impl fmt::Display for Privileges {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Privileges::All {
                with_privileges_keyword,
            } => {
                write!(
                    f,
                    "ALL{}",
                    if *with_privileges_keyword {
                        " PRIVILEGES"
                    } else {
                        ""
                    }
                )
            }
            Privileges::Actions(actions) => {
                write!(f, "{}", display_comma_separated(actions))
            }
        }
    }
}

/// A privilege on a database object (table, sequence, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Action {
    Connect,
    Create,
    Delete,
    Execute,
    Insert { columns: Option<Vec<Ident>> },
    References { columns: Option<Vec<Ident>> },
    Select { columns: Option<Vec<Ident>> },
    Temporary,
    Trigger,
    Truncate,
    Update { columns: Option<Vec<Ident>> },
    Usage,
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Action::Connect => f.write_str("CONNECT")?,
            Action::Create => f.write_str("CREATE")?,
            Action::Delete => f.write_str("DELETE")?,
            Action::Execute => f.write_str("EXECUTE")?,
            Action::Insert { .. } => f.write_str("INSERT")?,
            Action::References { .. } => f.write_str("REFERENCES")?,
            Action::Select { .. } => f.write_str("SELECT")?,
            Action::Temporary => f.write_str("TEMPORARY")?,
            Action::Trigger => f.write_str("TRIGGER")?,
            Action::Truncate => f.write_str("TRUNCATE")?,
            Action::Update { .. } => f.write_str("UPDATE")?,
            Action::Usage => f.write_str("USAGE")?,
        };
        match self {
            Action::Insert { columns }
            | Action::References { columns }
            | Action::Select { columns }
            | Action::Update { columns } => {
                if let Some(columns) = columns {
                    write!(f, " ({})", display_comma_separated(columns))?;
                }
            }
            _ => (),
        };
        Ok(())
    }
}

/// Objects on which privileges are granted in a GRANT statement.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum GrantObjects {
    /// Grant privileges on `ALL SEQUENCES IN SCHEMA <schema_name> [, ...]`
    AllSequencesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL TABLES IN SCHEMA <schema_name> [, ...]`
    AllTablesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL SOURCES IN SCHEMA <schema_name> [, ...]`
    AllSourcesInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on `ALL MATERIALIZED VIEWS IN SCHEMA <schema_name> [, ...]`
    AllMviewsInSchema { schemas: Vec<ObjectName> },
    /// Grant privileges on specific databases
    Databases(Vec<ObjectName>),
    /// Grant privileges on specific schemas
    Schemas(Vec<ObjectName>),
    /// Grant privileges on specific sources
    Sources(Vec<ObjectName>),
    /// Grant privileges on specific materialized views
    Mviews(Vec<ObjectName>),
    /// Grant privileges on specific sequences
    Sequences(Vec<ObjectName>),
    /// Grant privileges on specific tables
    Tables(Vec<ObjectName>),
    /// Grant privileges on specific sinks
    Sinks(Vec<ObjectName>),
}

impl fmt::Display for GrantObjects {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GrantObjects::Sequences(sequences) => {
                write!(f, "SEQUENCE {}", display_comma_separated(sequences))
            }
            GrantObjects::Schemas(schemas) => {
                write!(f, "SCHEMA {}", display_comma_separated(schemas))
            }
            GrantObjects::Tables(tables) => {
                write!(f, "{}", display_comma_separated(tables))
            }
            GrantObjects::AllSequencesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SEQUENCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllTablesInSchema { schemas } => {
                write!(
                    f,
                    "ALL TABLES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllSourcesInSchema { schemas } => {
                write!(
                    f,
                    "ALL SOURCES IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::AllMviewsInSchema { schemas } => {
                write!(
                    f,
                    "ALL MATERIALIZED VIEWS IN SCHEMA {}",
                    display_comma_separated(schemas)
                )
            }
            GrantObjects::Databases(databases) => {
                write!(f, "DATABASE {}", display_comma_separated(databases))
            }
            GrantObjects::Sources(sources) => {
                write!(f, "SOURCE {}", display_comma_separated(sources))
            }
            GrantObjects::Mviews(mviews) => {
                write!(f, "MATERIALIZED VIEW {}", display_comma_separated(mviews))
            }
            GrantObjects::Sinks(sinks) => {
                write!(f, "SINK {}", display_comma_separated(sinks))
            }
        }
    }
}

/// SQL assignment `foo = expr` as used in SQLUpdate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Assignment {
    pub id: Vec<Ident>,
    pub value: Expr,
}

impl fmt::Display for Assignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", display_separated(&self.id, "."), self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionArgExpr {
    Expr(Expr),
    /// Expr is an arbitrary expression, returning either a table or a column.
    /// Idents are the prefix of `*`, which are consecutive field accesses.
    /// e.g. `(table.v1).*` or `(table).v1.*`
    ExprQualifiedWildcard(Expr, Vec<Ident>),
    /// Qualified wildcard, e.g. `alias.*` or `schema.table.*`.
    QualifiedWildcard(ObjectName),
    /// An unqualified `*`
    Wildcard,
}

impl fmt::Display for FunctionArgExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArgExpr::Expr(expr) => write!(f, "{}", expr),
            FunctionArgExpr::ExprQualifiedWildcard(expr, prefix) => {
                write!(
                    f,
                    "({}){}.*",
                    expr,
                    prefix
                        .iter()
                        .format_with("", |i, f| f(&format_args!(".{i}")))
                )
            }
            FunctionArgExpr::QualifiedWildcard(prefix) => write!(f, "{}.*", prefix),
            FunctionArgExpr::Wildcard => f.write_str("*"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionArg {
    Named { name: Ident, arg: FunctionArgExpr },
    Unnamed(FunctionArgExpr),
}

impl FunctionArg {
    pub fn get_expr(&self) -> FunctionArgExpr {
        match self {
            FunctionArg::Named { name: _, arg } => arg.clone(),
            FunctionArg::Unnamed(arg) => arg.clone(),
        }
    }
}

impl fmt::Display for FunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionArg::Named { name, arg } => write!(f, "{} => {}", name, arg),
            FunctionArg::Unnamed(unnamed_arg) => write!(f, "{}", unnamed_arg),
        }
    }
}

/// A function call
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    pub over: Option<WindowSpec>,
    // aggregate functions may specify eg `COUNT(DISTINCT x)`
    pub distinct: bool,
    // aggregate functions may contain order_by_clause
    pub order_by: Vec<OrderByExpr>,
    pub filter: Option<Box<Expr>>,
}

impl Function {
    pub fn no_arg(name: ObjectName) -> Self {
        Self {
            name,
            args: vec![],
            over: None,
            distinct: false,
            order_by: vec![],
            filter: None,
        }
    }
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}{}{}{})",
            self.name,
            if self.distinct { "DISTINCT " } else { "" },
            display_comma_separated(&self.args),
            if !self.order_by.is_empty() {
                " ORDER BY "
            } else {
                ""
            },
            display_comma_separated(&self.order_by),
        )?;
        if let Some(o) = &self.over {
            write!(f, " OVER ({})", o)?;
        }
        if let Some(filter) = &self.filter {
            write!(f, " FILTER(WHERE {})", filter)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObjectType {
    Table,
    View,
    MaterializedView,
    Index,
    Schema,
    Source,
    Sink,
    Database,
    User,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::View => "VIEW",
            ObjectType::MaterializedView => "MATERIALIZED VIEW",
            ObjectType::Index => "INDEX",
            ObjectType::Schema => "SCHEMA",
            ObjectType::Source => "SOURCE",
            ObjectType::Sink => "SINK",
            ObjectType::Database => "DATABASE",
            ObjectType::User => "USER",
        })
    }
}

impl ParseTo for ObjectType {
    fn parse_to(parser: &mut Parser) -> Result<Self, ParserError> {
        let object_type = if parser.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if parser.parse_keyword(Keyword::VIEW) {
            ObjectType::View
        } else if parser.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            ObjectType::MaterializedView
        } else if parser.parse_keyword(Keyword::SOURCE) {
            ObjectType::Source
        } else if parser.parse_keyword(Keyword::SINK) {
            ObjectType::Sink
        } else if parser.parse_keyword(Keyword::INDEX) {
            ObjectType::Index
        } else if parser.parse_keyword(Keyword::SCHEMA) {
            ObjectType::Schema
        } else if parser.parse_keyword(Keyword::DATABASE) {
            ObjectType::Database
        } else if parser.parse_keyword(Keyword::USER) {
            ObjectType::User
        } else {
            return parser.expected(
                "TABLE, VIEW, INDEX, MATERIALIZED VIEW, SOURCE, SINK, SCHEMA, DATABASE or USER after DROP",
                parser.peek_token(),
            );
        };
        Ok(object_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SqlOption {
    pub name: ObjectName,
    pub value: Value,
}

impl fmt::Display for SqlOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.name, self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum EmitMode {
    Immediately,
    OnWindowClose,
}

impl fmt::Display for EmitMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            EmitMode::Immediately => "IMMEDIATELY",
            EmitMode::OnWindowClose => "ON WINDOW CLOSE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionMode {
    AccessMode(TransactionAccessMode),
    IsolationLevel(TransactionIsolationLevel),
}

impl fmt::Display for TransactionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionMode::*;
        match self {
            AccessMode(access_mode) => write!(f, "{}", access_mode),
            IsolationLevel(iso_level) => write!(f, "ISOLATION LEVEL {}", iso_level),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionAccessMode {
    ReadOnly,
    ReadWrite,
}

impl fmt::Display for TransactionAccessMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionAccessMode::*;
        f.write_str(match self {
            ReadOnly => "READ ONLY",
            ReadWrite => "READ WRITE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl fmt::Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use TransactionIsolationLevel::*;
        f.write_str(match self {
            ReadUncommitted => "READ UNCOMMITTED",
            ReadCommitted => "READ COMMITTED",
            RepeatableRead => "REPEATABLE READ",
            Serializable => "SERIALIZABLE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ShowStatementFilter {
    Like(String),
    ILike(String),
    Where(Expr),
}

impl fmt::Display for ShowStatementFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ShowStatementFilter::*;
        match self {
            Like(pattern) => write!(f, "LIKE '{}'", value::escape_single_quote_string(pattern)),
            ILike(pattern) => write!(f, "ILIKE {}", value::escape_single_quote_string(pattern)),
            Where(expr) => write!(f, "WHERE {}", expr),
        }
    }
}

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DropFunctionOption {
    Restrict,
    Cascade,
}

impl fmt::Display for DropFunctionOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DropFunctionOption::Restrict => write!(f, "RESTRICT "),
            DropFunctionOption::Cascade => write!(f, "CASCADE  "),
        }
    }
}

/// Function describe in DROP FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct DropFunctionDesc {
    pub name: ObjectName,
    pub args: Option<Vec<OperateFunctionArg>>,
}

impl fmt::Display for DropFunctionDesc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;
        if let Some(args) = &self.args {
            write!(f, "({})", display_comma_separated(args))?;
        }
        Ok(())
    }
}

/// Function argument in CREATE FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct OperateFunctionArg {
    pub mode: Option<ArgMode>,
    pub name: Option<Ident>,
    pub data_type: DataType,
    pub default_expr: Option<Expr>,
}

impl OperateFunctionArg {
    /// Returns an unnamed argument.
    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            mode: None,
            name: None,
            data_type,
            default_expr: None,
        }
    }

    /// Returns an argument with name.
    pub fn with_name(name: &str, data_type: DataType) -> Self {
        Self {
            mode: None,
            name: Some(name.into()),
            data_type,
            default_expr: None,
        }
    }
}

impl fmt::Display for OperateFunctionArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(mode) = &self.mode {
            write!(f, "{} ", mode)?;
        }
        if let Some(name) = &self.name {
            write!(f, "{} ", name)?;
        }
        write!(f, "{}", self.data_type)?;
        if let Some(default_expr) = &self.default_expr {
            write!(f, " = {}", default_expr)?;
        }
        Ok(())
    }
}

/// The mode of an argument in CREATE FUNCTION.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ArgMode {
    In,
    Out,
    InOut,
}

impl fmt::Display for ArgMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArgMode::In => write!(f, "IN"),
            ArgMode::Out => write!(f, "OUT"),
            ArgMode::InOut => write!(f, "INOUT"),
        }
    }
}

/// These attributes inform the query optimizer about the behavior of the function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionBehavior {
    Immutable,
    Stable,
    Volatile,
}

impl fmt::Display for FunctionBehavior {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionBehavior::Immutable => write!(f, "IMMUTABLE"),
            FunctionBehavior::Stable => write!(f, "STABLE"),
            FunctionBehavior::Volatile => write!(f, "VOLATILE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum FunctionDefinition {
    SingleQuotedDef(String),
    DoubleDollarDef(String),
}

impl fmt::Display for FunctionDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FunctionDefinition::SingleQuotedDef(s) => write!(f, "'{s}'")?,
            FunctionDefinition::DoubleDollarDef(s) => write!(f, "$${s}$$")?,
        }
        Ok(())
    }
}

/// Postgres specific feature.
///
/// See [Postgresdocs](https://www.postgresql.org/docs/15/sql-createfunction.html)
/// for more details
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateFunctionBody {
    /// LANGUAGE lang_name
    pub language: Option<Ident>,
    /// IMMUTABLE | STABLE | VOLATILE
    pub behavior: Option<FunctionBehavior>,
    /// AS 'definition'
    ///
    /// Note that Hive's `AS class_name` is also parsed here.
    pub as_: Option<FunctionDefinition>,
    /// RETURN expression
    pub return_: Option<Expr>,
}

impl fmt::Display for CreateFunctionBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(language) = &self.language {
            write!(f, " LANGUAGE {language}")?;
        }
        if let Some(behavior) = &self.behavior {
            write!(f, " {behavior}")?;
        }
        if let Some(definition) = &self.as_ {
            write!(f, " AS {definition}")?;
        }
        if let Some(expr) = &self.return_ {
            write!(f, " RETURN {expr}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum SetVariableValue {
    Ident(Ident),
    Literal(Value),
    Default,
}

impl fmt::Display for SetVariableValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use SetVariableValue::*;
        match self {
            Ident(ident) => write!(f, "{}", ident),
            Literal(literal) => write!(f, "{}", literal),
            Default => write!(f, "DEFAULT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_frame_default() {
        let window_frame = WindowFrame::default();
        assert_eq!(WindowFrameBound::Preceding(None), window_frame.start_bound);
    }

    #[test]
    fn test_grouping_sets_display() {
        // a and b in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("GROUPING SETS ((a), (b))", format!("{}", grouping_sets));

        // a and b in the same group
        let grouping_sets = Expr::GroupingSets(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("GROUPING SETS ((a, b))", format!("{}", grouping_sets));

        // (a, b) and (c, d) in different group
        let grouping_sets = Expr::GroupingSets(vec![
            vec![
                Expr::Identifier(Ident::new_unchecked("a")),
                Expr::Identifier(Ident::new_unchecked("b")),
            ],
            vec![
                Expr::Identifier(Ident::new_unchecked("c")),
                Expr::Identifier(Ident::new_unchecked("d")),
            ],
        ]);
        assert_eq!(
            "GROUPING SETS ((a, b), (c, d))",
            format!("{}", grouping_sets)
        );
    }

    #[test]
    fn test_rollup_display() {
        let rollup = Expr::Rollup(vec![vec![Expr::Identifier(Ident::new_unchecked("a"))]]);
        assert_eq!("ROLLUP (a)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("ROLLUP ((a, b))", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("ROLLUP (a, b)", format!("{}", rollup));

        let rollup = Expr::Rollup(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![
                Expr::Identifier(Ident::new_unchecked("b")),
                Expr::Identifier(Ident::new_unchecked("c")),
            ],
            vec![Expr::Identifier(Ident::new_unchecked("d"))],
        ]);
        assert_eq!("ROLLUP (a, (b, c), d)", format!("{}", rollup));
    }

    #[test]
    fn test_cube_display() {
        let cube = Expr::Cube(vec![vec![Expr::Identifier(Ident::new_unchecked("a"))]]);
        assert_eq!("CUBE (a)", format!("{}", cube));

        let cube = Expr::Cube(vec![vec![
            Expr::Identifier(Ident::new_unchecked("a")),
            Expr::Identifier(Ident::new_unchecked("b")),
        ]]);
        assert_eq!("CUBE ((a, b))", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![Expr::Identifier(Ident::new_unchecked("b"))],
        ]);
        assert_eq!("CUBE (a, b)", format!("{}", cube));

        let cube = Expr::Cube(vec![
            vec![Expr::Identifier(Ident::new_unchecked("a"))],
            vec![
                Expr::Identifier(Ident::new_unchecked("b")),
                Expr::Identifier(Ident::new_unchecked("c")),
            ],
            vec![Expr::Identifier(Ident::new_unchecked("d"))],
        ]);
        assert_eq!("CUBE (a, (b, c), d)", format!("{}", cube));
    }

    #[test]
    fn test_array_index_display() {
        let array_index = Expr::ArrayIndex {
            obj: Box::new(Expr::Identifier(Ident::new_unchecked("v1"))),
            index: Box::new(Expr::Value(Value::Number("1".into()))),
        };
        assert_eq!("v1[1]", format!("{}", array_index));

        let array_index2 = Expr::ArrayIndex {
            obj: Box::new(array_index),
            index: Box::new(Expr::Value(Value::Number("1".into()))),
        };
        assert_eq!("v1[1][1]", format!("{}", array_index2));
    }

    #[test]
    /// issue: https://github.com/risingwavelabs/risingwave/issues/7635
    fn test_nested_op_display() {
        let binary_op = Expr::BinaryOp {
            left: Box::new(Expr::Value(Value::Boolean(true))),
            op: BinaryOperator::Or,
            right: Box::new(Expr::IsNotFalse(Box::new(Expr::Value(Value::Boolean(
                true,
            ))))),
        };
        assert_eq!("true OR true IS NOT FALSE", format!("{}", binary_op));

        let unary_op = Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(Expr::IsNotFalse(Box::new(Expr::Value(Value::Boolean(
                true,
            ))))),
        };
        assert_eq!("NOT true IS NOT FALSE", format!("{}", unary_op));
    }
}
