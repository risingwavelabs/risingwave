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
use std::str::{from_utf8, Chars};

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, DateTimeField, Decimal, Interval, ScalarImpl};
use risingwave_sqlparser::ast::{DateTimeField as AstDateTimeField, Expr, Value};

use crate::binder::Binder;
use crate::expr::{align_types, Expr as _, ExprImpl, ExprType, FunctionCall, Literal};

impl Binder {
    pub fn bind_value(&mut self, value: Value) -> Result<Literal> {
        match value {
            Value::Number(s) => self.bind_number(s),
            Value::SingleQuotedString(s) => self.bind_string(s),
            Value::CstyleEscapesString(s) => self.bind_string(unescape_c_style(&s)?),
            Value::Boolean(b) => self.bind_bool(b),
            // Both null and string literal will be treated as `unknown` during type inference.
            // See [`ExprImpl::is_unknown`].
            Value::Null => Ok(Literal::new(None, DataType::Varchar)),
            Value::Interval {
                value,
                leading_field,
                // TODO: support more interval types.
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => self.bind_interval(value, leading_field),
            _ => Err(ErrorCode::NotImplemented(format!("value: {:?}", value), None.into()).into()),
        }
    }

    pub(super) fn bind_string(&mut self, s: String) -> Result<Literal> {
        Ok(Literal::new(
            Some(ScalarImpl::Utf8(s.into())),
            DataType::Varchar,
        ))
    }

    fn bind_bool(&mut self, b: bool) -> Result<Literal> {
        Ok(Literal::new(Some(ScalarImpl::Bool(b)), DataType::Boolean))
    }

    fn bind_number(&mut self, s: String) -> Result<Literal> {
        let (data, data_type) = if let Ok(int_32) = s.parse::<i32>() {
            (Some(ScalarImpl::Int32(int_32)), DataType::Int32)
        } else if let Ok(int_64) = s.parse::<i64>() {
            (Some(ScalarImpl::Int64(int_64)), DataType::Int64)
        } else if let Ok(decimal) = s.parse::<Decimal>() {
            // Notice: when the length of decimal exceeds 29(>= 30), it will be rounded up.
            (Some(ScalarImpl::Decimal(decimal)), DataType::Decimal)
        } else if let Some(scientific) = Decimal::from_scientific(&s) {
            (Some(ScalarImpl::Decimal(scientific)), DataType::Decimal)
        } else {
            return Err(ErrorCode::BindError(format!("Number {s} overflows")).into());
        };
        Ok(Literal::new(data, data_type))
    }

    fn bind_interval(
        &mut self,
        s: String,
        leading_field: Option<AstDateTimeField>,
    ) -> Result<Literal> {
        let interval =
            Interval::parse_with_fields(&s, leading_field.map(Self::bind_date_time_field))?;
        let datum = Some(ScalarImpl::Interval(interval));
        let literal = Literal::new(datum, DataType::Interval);

        Ok(literal)
    }

    fn bind_date_time_field(field: AstDateTimeField) -> DateTimeField {
        // This is a binder function rather than `impl From<AstDateTimeField> for DateTimeField`,
        // so that the `sqlparser` crate and the `common` crate are kept independent.
        match field {
            AstDateTimeField::Year => DateTimeField::Year,
            AstDateTimeField::Month => DateTimeField::Month,
            AstDateTimeField::Day => DateTimeField::Day,
            AstDateTimeField::Hour => DateTimeField::Hour,
            AstDateTimeField::Minute => DateTimeField::Minute,
            AstDateTimeField::Second => DateTimeField::Second,
        }
    }

    /// `ARRAY[...]` is represented as an function call at the binder stage.
    pub(super) fn bind_array(&mut self, exprs: Vec<Expr>) -> Result<ExprImpl> {
        if exprs.is_empty() {
            return Err(ErrorCode::BindError("cannot determine type of empty array\nHINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[].".into()).into());
        }
        let mut exprs = exprs
            .into_iter()
            .map(|e| self.bind_expr_inner(e))
            .collect::<Result<Vec<ExprImpl>>>()?;
        let element_type = align_types(exprs.iter_mut())?;
        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            exprs,
            DataType::List(Box::new(element_type)),
        )
        .into();
        Ok(expr)
    }

    pub(super) fn bind_array_cast(&mut self, exprs: Vec<Expr>, ty: DataType) -> Result<ExprImpl> {
        if exprs.is_empty() {
            let lhs: ExprImpl = FunctionCall::new_unchecked(
                ExprType::Array,
                vec![],
                // Treat `array[]` as `varchar[]` temporarily before applying cast.
                DataType::List(Box::new(DataType::Varchar)),
            )
            .into();
            return lhs.cast_explicit(ty).map_err(Into::into);
        }
        let inner_type = if let DataType::List(datatype) = &ty {
            *datatype.clone()
        } else {
            return Err(ErrorCode::BindError(format!(
                "cannot cast array to non-array type {}",
                ty
            ))
            .into());
        };

        let exprs = exprs
            .into_iter()
            .map(|e| self.bind_cast_inner(e, inner_type.clone()))
            .collect::<Result<Vec<ExprImpl>>>()?;

        let expr: ExprImpl = FunctionCall::new_unchecked(ExprType::Array, exprs, ty).into();
        Ok(expr)
    }

    pub(super) fn bind_array_index(&mut self, obj: Expr, index: Expr) -> Result<ExprImpl> {
        let obj = self.bind_expr_inner(obj)?;
        match obj.return_type() {
            DataType::List(return_type) => Ok(FunctionCall::new_unchecked(
                ExprType::ArrayAccess,
                vec![obj, self.bind_expr_inner(index)?],
                *return_type,
            )
            .into()),
            data_type => Err(ErrorCode::BindError(format!(
                "array index applied to type {}, which is not a composite type",
                data_type
            ))
            .into()),
        }
    }

    pub(super) fn bind_array_range_index(
        &mut self,
        obj: Expr,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    ) -> Result<ExprImpl> {
        let obj = self.bind_expr_inner(obj)?;
        let start = match start {
            None => ExprImpl::literal_int(1),
            Some(expr) => self
                .bind_expr_inner(*expr)?
                .cast_implicit(DataType::Int32)?,
        };
        // Don't worry, the backend implementation will stop iterating once it encounters the end
        // of the array.
        let end = match end {
            None => ExprImpl::literal_int(i32::MAX),
            Some(expr) => self
                .bind_expr_inner(*expr)?
                .cast_implicit(DataType::Int32)?,
        };
        match obj.return_type() {
            DataType::List(return_type) => Ok(FunctionCall::new_unchecked(
                ExprType::ArrayRangeAccess,
                vec![obj, start, end],
                DataType::List(return_type),
            )
            .into()),
            data_type => Err(ErrorCode::BindError(format!(
                "array range index applied to type {}, which is not a composite type",
                data_type
            ))
            .into()),
        }
    }

    /// `Row(...)` is represented as an function call at the binder stage.
    pub(super) fn bind_row(&mut self, exprs: Vec<Expr>) -> Result<ExprImpl> {
        let exprs = exprs
            .into_iter()
            .map(|e| self.bind_expr_inner(e))
            .collect::<Result<Vec<ExprImpl>>>()?;
        let data_type =
            DataType::new_struct(exprs.iter().map(|e| e.return_type()).collect_vec(), vec![]);
        let expr: ExprImpl = FunctionCall::new_unchecked(ExprType::Row, exprs, data_type).into();
        Ok(expr)
    }
}

/// Helper function used to convert string with c-style escapes into a normal string
/// e.g. 'hello\x3fworld' -> 'hello?world'
///
/// Detail of c-style escapes refer from:
/// <https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-UESCAPE:~:text=4.1.2.2.%C2%A0String%20Constants%20With%20C%2DStyle%20Escapes>
fn unescape_c_style(s: &str) -> Result<String> {
    let mut chars = s.chars().peekable();

    let mut res = String::with_capacity(s.len());

    let hex_byte_process = |chars: &mut Peekable<Chars<'_>>,
                            res: &mut String,
                            len: usize,
                            default_char: char| {
        let mut unicode_seq: String = String::with_capacity(len);
        for _ in 0..len {
            if let Some(c) = chars.peek() && c.is_ascii_hexdigit() {
                unicode_seq.push(chars.next().unwrap());
            } else {
                break;
            }
        }

        if unicode_seq.is_empty() && len == 2 {
            res.push(default_char);
            return Ok::<(), RwError>(());
        } else if unicode_seq.len() < len && len != 2 {
            return Err(ErrorCode::BindError(
                "invalid unicode sequence: must be \\uXXXX or \\UXXXXXXXX".to_string(),
            )
            .into());
        }

        if len == 2 {
            let number = [u8::from_str_radix(&unicode_seq, 16)
                .map_err(|e| ErrorCode::BindError(format!("invalid unicode sequence: {}", e)))?];

            res.push(
                from_utf8(&number)
                    .map_err(|err| {
                        ErrorCode::BindError(format!("invalid unicode sequence: {}", err))
                    })?
                    .chars()
                    .next()
                    .unwrap(),
            );
        } else {
            let number = u32::from_str_radix(&unicode_seq, 16)
                .map_err(|e| ErrorCode::BindError(format!("invalid unicode sequence: {}", e)))?;
            res.push(char::from_u32(number).ok_or_else(|| {
                ErrorCode::BindError(format!("invalid unicode sequence: {}", unicode_seq))
            })?);
        }
        Ok(())
    };

    let octal_byte_process = |chars: &mut Peekable<Chars<'_>>, res: &mut String, digit: char| {
        let mut unicode_seq: String = String::with_capacity(3);
        unicode_seq.push(digit);
        for _ in 0..2 {
            if let Some(c) = chars.peek() && matches!(*c, '0'..='7') {
                unicode_seq.push(chars.next().unwrap());
            } else {
                break;
            }
        }

        let number = [u8::from_str_radix(&unicode_seq, 8)
            .map_err(|e| ErrorCode::BindError(format!("invalid unicode sequence: {}", e)))?];

        res.push(
            from_utf8(&number)
                .map_err(|err| ErrorCode::BindError(format!("invalid unicode sequence: {}", err)))?
                .chars()
                .next()
                .unwrap(),
        );
        Ok::<(), RwError>(())
    };

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                None => {
                    return Err(ErrorCode::BindError("unterminated escape sequence".into()).into());
                }
                Some(next_c) => match next_c {
                    'b' => res.push('\u{08}'),
                    'f' => res.push('\u{0C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'x' => hex_byte_process(&mut chars, &mut res, 2, 'x')?,
                    'u' => hex_byte_process(&mut chars, &mut res, 4, 'u')?,
                    'U' => hex_byte_process(&mut chars, &mut res, 8, 'U')?,
                    digit @ '0'..='7' => octal_byte_process(&mut chars, &mut res, digit)?,
                    _ => res.push(next_c),
                },
            }
        } else {
            res.push(c);
        }
    }

    Ok(res)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_prost;
    use risingwave_sqlparser::ast::Value::Number;

    use crate::binder::test_utils::mock_binder;
    use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall};

    #[tokio::test]
    async fn test_bind_value() {
        use std::str::FromStr;

        use super::*;

        let mut binder = mock_binder();
        let values = vec![
            "1",
            "111111111111111",
            "111111111.111111",
            "111111111111111111111111",
            "0.111111",
            "-0.01",
        ];
        let data = vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Int64(111111111111111)),
            Some(ScalarImpl::Decimal(
                Decimal::from_str("111111111.111111").unwrap(),
            )),
            Some(ScalarImpl::Decimal(
                Decimal::from_str("111111111111111111111111").unwrap(),
            )),
            Some(ScalarImpl::Decimal(Decimal::from_str("0.111111").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("-0.01").unwrap())),
        ];
        let data_type = vec![
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
        ];

        for i in 0..values.len() {
            let value = Value::Number(String::from(values[i]));
            let res = binder.bind_value(value).unwrap();
            let ans = Literal::new(data[i].clone(), data_type[i].clone());
            assert_eq!(res, ans);
        }
    }

    #[tokio::test]
    async fn test_bind_scientific_number() {
        use std::str::FromStr;

        use super::*;

        let mut binder = mock_binder();
        let values = vec![
            ("1e6"),
            ("1.25e6"),
            ("1.25e1"),
            ("1e-2"),
            ("1.25e-2"),
            ("1e15"),
        ];
        let data = vec![
            Some(ScalarImpl::Decimal(Decimal::from_str("1000000").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("1250000").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("12.5").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("0.01").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("0.0125").unwrap())),
            Some(ScalarImpl::Decimal(
                Decimal::from_str("1000000000000000").unwrap(),
            )),
        ];
        let data_type = vec![
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
        ];

        for i in 0..values.len() {
            let res = binder.bind_value(Number(values[i].to_string())).unwrap();
            let ans = Literal::new(data[i].clone(), data_type[i].clone());
            assert_eq!(res, ans);
        }
    }

    #[test]
    fn test_array_expr() {
        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            vec![ExprImpl::literal_int(11)],
            DataType::List(Box::new(DataType::Int32)),
        )
        .into();
        let expr_pb = expr.to_expr_proto();
        let expr = build_from_prost(&expr_pb).unwrap();
        match expr.return_type() {
            DataType::List(datatype) => {
                assert_eq!(datatype, Box::new(DataType::Int32));
            }
            _ => panic!("unexpected type"),
        };
    }

    #[test]
    fn test_array_index_expr() {
        let array_expr = FunctionCall::new_unchecked(
            ExprType::Array,
            vec![ExprImpl::literal_int(11), ExprImpl::literal_int(22)],
            DataType::List(Box::new(DataType::Int32)),
        )
        .into();

        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::ArrayAccess,
            vec![array_expr, ExprImpl::literal_int(1)],
            DataType::Int32,
        )
        .into();

        let expr_pb = expr.to_expr_proto();
        let expr = build_from_prost(&expr_pb).unwrap();
        assert_eq!(expr.return_type(), DataType::Int32);
    }

    #[tokio::test]
    async fn test_bind_interval() {
        use super::*;

        let mut binder = mock_binder();
        let values = vec![
            "1 hour",
            "1 h",
            "1 year",
            "6 second",
            "2 minutes",
            "1 month",
        ];
        let data = vec![
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(60))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(60))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_ymd(1, 0, 0))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_millis(6 * 1000))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(2))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(Interval::from_month(1))),
                DataType::Interval,
            )),
        ];

        for i in 0..values.len() {
            let value = Value::Interval {
                value: values[i].to_string(),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            assert_eq!(binder.bind_value(value), data[i]);
        }
    }
}
