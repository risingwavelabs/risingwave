// Copyright 2022 Singularity Data
//
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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{DataType, Decimal, IntervalUnit, ScalarImpl};
use risingwave_expr::vector_op::cast::str_parse;
use risingwave_sqlparser::ast::{DateTimeField, Expr, Value};

use crate::binder::Binder;
use crate::expr::{align_types, Expr as _, ExprImpl, ExprType, FunctionCall, Literal};

impl Binder {
    pub fn bind_value(&mut self, value: Value) -> Result<Literal> {
        match value {
            Value::Number(s, b) => self.bind_number(s, b),
            Value::SingleQuotedString(s) => self.bind_string(s),
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
        Ok(Literal::new(Some(ScalarImpl::Utf8(s)), DataType::Varchar))
    }

    fn bind_bool(&mut self, b: bool) -> Result<Literal> {
        Ok(Literal::new(Some(ScalarImpl::Bool(b)), DataType::Boolean))
    }

    fn bind_number(&mut self, s: String, _b: bool) -> Result<Literal> {
        let (data, data_type) = if let Ok(int_32) = s.parse::<i32>() {
            (Some(ScalarImpl::Int32(int_32)), DataType::Int32)
        } else if let Ok(int_64) = s.parse::<i64>() {
            (Some(ScalarImpl::Int64(int_64)), DataType::Int64)
        } else {
            // Notice: when the length of decimal exceeds 29(>= 30), it will be rounded up.
            let decimal = str_parse::<Decimal>(&s)?;
            (Some(ScalarImpl::Decimal(decimal)), DataType::Decimal)
        };
        Ok(Literal::new(data, data_type))
    }

    fn bind_interval(
        &mut self,
        s: String,
        leading_field: Option<DateTimeField>,
    ) -> Result<Literal> {
        // > INTERVAL '1' means 1 second.
        // https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT
        let default_unit = leading_field.unwrap_or(DateTimeField::Second);
        use DateTimeField::*;
        let tokens = parse_interval(&s)?;
        
        // Parse tokens into (number, unit) pairs
        let mut pairs = Vec::new();
        let mut i = 0;
        while i < tokens.len() {
            let num = match tokens.get(i) {
                Some(TimeStrToken::Num(num)) => *num,
                _ => {
                    return Err(
                        ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", &s)).into(),
                    );
                }
            };
            i += 1;
            
            let unit = match tokens.get(i) {
                Some(TimeStrToken::TimeUnit(u)) => {
                    i += 1;
                    *u
                }
                _ => default_unit,
            };
            
            pairs.push((num, unit));
        }
        
        if pairs.is_empty() {
            return Err(ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", &s)).into());
        }
        
        // Accumulate all interval components
        let mut total_months = 0i32;
        let mut total_days = 0i32;
        let mut total_ms = 0i64;
        
        for (num, unit) in pairs {
            match unit {
                Year => {
                    let months = num.checked_mul(12).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax(format!("Interval overflow: {} years", num)))
                    })?;
                    total_months = total_months.checked_add(months as i32).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many months"))
                    })?;
                }
                Month => {
                    total_months = total_months.checked_add(num as i32).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many months"))
                    })?;
                }
                Day => {
                    total_days = total_days.checked_add(num as i32).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many days"))
                    })?;
                }
                Hour => {
                    let ms = num.checked_mul(3600 * 1000).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many hours"))
                    })?;
                    total_ms = total_ms.checked_add(ms).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many milliseconds"))
                    })?;
                }
                Minute => {
                    let ms = num.checked_mul(60 * 1000).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many minutes"))
                    })?;
                    total_ms = total_ms.checked_add(ms).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many milliseconds"))
                    })?;
                }
                Second => {
                    let ms = num.checked_mul(1000).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many seconds"))
                    })?;
                    total_ms = total_ms.checked_add(ms).ok_or_else(|| {
                        RwError::from(ErrorCode::InvalidInputSyntax("Interval overflow: too many milliseconds"))
                    })?;
                }
            }
        }
        
        let interval = IntervalUnit::new(total_months, total_days, total_ms);

        let datum = Some(ScalarImpl::Interval(interval));
        let literal = Literal::new(datum, DataType::Interval);

        Ok(literal)
    }

    /// `ARRAY[...]` is represented as an function call at the binder stage.
    pub(super) fn bind_array(&mut self, exprs: Vec<Expr>) -> Result<ExprImpl> {
        let mut exprs = exprs
            .into_iter()
            .map(|e| self.bind_expr(e))
            .collect::<Result<Vec<ExprImpl>>>()?;
        let element_type = align_types(exprs.iter_mut())?;
        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            exprs,
            DataType::List {
                datatype: Box::new(element_type),
            },
        )
        .into();
        Ok(expr)
    }

    pub(super) fn bind_array_index(&mut self, obj: Expr, indexs: Vec<Expr>) -> Result<ExprImpl> {
        let obj = self.bind_expr(obj)?;
        match obj.return_type() {
            DataType::List {
                datatype: return_type,
            } => {
                let mut indexs = indexs
                    .into_iter()
                    .map(|e| self.bind_expr(e))
                    .collect::<Result<Vec<ExprImpl>>>()?;
                indexs.insert(0, obj);

                let expr: ExprImpl =
                    FunctionCall::new_unchecked(ExprType::ArrayAccess, indexs, *return_type).into();
                Ok(expr)
            }
            _ => panic!("Should be a List"),
        }
    }

    /// `Row(...)` is represented as an function call at the binder stage.
    pub(super) fn bind_row(&mut self, exprs: Vec<Expr>) -> Result<ExprImpl> {
        let exprs = exprs
            .into_iter()
            .map(|e| self.bind_expr(e))
            .collect::<Result<Vec<ExprImpl>>>()?;
        let data_type = DataType::Struct {
            fields: exprs.iter().map(|e| e.return_type()).collect_vec().into(),
        };
        let expr: ExprImpl = FunctionCall::new_unchecked(ExprType::Row, exprs, data_type).into();
        Ok(expr)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimeStrToken {
    Num(i64),
    TimeUnit(DateTimeField),
}

fn convert_digit(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<()> {
    if !c.is_empty() {
        match c.parse::<i64>() {
            Ok(num) => {
                t.push(TimeStrToken::Num(num));
            }
            Err(_) => {
                return Err(
                    ErrorCode::InvalidInputSyntax(format!("Invalid interval: {}", c)).into(),
                );
            }
        }
        c.clear();
    }
    Ok(())
}

fn convert_unit(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<()> {
    if !c.is_empty() {
        t.push(TimeStrToken::TimeUnit(c.parse()?));
        c.clear();
    }
    Ok(())
}

pub fn parse_interval(s: &str) -> Result<Vec<TimeStrToken>> {
    let s = s.trim();
    let mut tokens = Vec::new();
    let mut num_buf = "".to_string();
    let mut char_buf = "".to_string();
    let mut expect_number = true; // Track whether we expect a number next
    
    for (i, c) in s.chars().enumerate() {
        match c {
            '-' if expect_number => {
                // This is a negative sign for a number
                num_buf.push(c);
            }
            '-' => {
                // This is a subtraction operator, treat it as starting a new negative number
                convert_unit(&mut char_buf, &mut tokens)?;
                convert_digit(&mut num_buf, &mut tokens)?;
                num_buf.push(c);
                expect_number = true;
            }
            '+' => {
                // This is an addition operator, just skip it
                convert_unit(&mut char_buf, &mut tokens)?;
                convert_digit(&mut num_buf, &mut tokens)?;
                expect_number = true;
            }
            c if c.is_ascii_digit() => {
                convert_unit(&mut char_buf, &mut tokens)?;
                num_buf.push(c);
                expect_number = false;
            }
            c if c.is_ascii_alphabetic() => {
                convert_digit(&mut num_buf, &mut tokens)?;
                char_buf.push(c);
                expect_number = false;
            }
            chr if chr.is_ascii_whitespace() => {
                convert_unit(&mut char_buf, &mut tokens)?;
                convert_digit(&mut num_buf, &mut tokens)?;
                // After whitespace, we might expect either a number or a unit
                // Don't change expect_number here
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "Invalid character at offset {} in {}: {:?}. Only support digit or alphabetic now",
                    i, s, c
                )).into());
            }
        }
    }
    convert_digit(&mut num_buf, &mut tokens)?;
    convert_unit(&mut char_buf, &mut tokens)?;

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::build_from_prost;

    use crate::binder::test_utils::mock_binder;
    use crate::expr::{Expr, ExprImpl, ExprType, FunctionCall};

    #[test]
    fn test_bind_value() {
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
            let value = Value::Number(String::from(values[i]), false);
            let res = binder.bind_value(value).unwrap();
            let ans = Literal::new(data[i].clone(), data_type[i].clone());
            assert_eq!(res, ans);
        }
    }

    #[test]
    fn test_array_expr() {
        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            vec![ExprImpl::literal_int(11)],
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
        )
        .into();
        let expr_pb = expr.to_expr_proto();
        let expr = build_from_prost(&expr_pb).unwrap();
        match expr.return_type() {
            DataType::List { datatype } => {
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
            DataType::List {
                datatype: Box::new(DataType::Int32),
            },
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

    #[test]
    fn test_bind_interval() {
        use super::*;

        let mut binder = mock_binder();
        let values = vec![
            "1 hour",
            "1 h",
            "1 year",
            "6 second",
            "2 minutes",
            "1 month",
            "1 month - 1 day",
            "1 day + 1 hour",
            "2 months - 5 days",
        ];
        let data = vec![
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_minutes(60))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_minutes(60))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_ymd(1, 0, 0))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_millis(6 * 1000))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_minutes(2))),
                DataType::Interval,
            )),
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::from_month(1))),
                DataType::Interval,
            )),
            // 1 month - 1 day = 1 month, -1 days, 0 ms
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::new(1, -1, 0))),
                DataType::Interval,
            )),
            // 1 day + 1 hour = 0 months, 1 days, 3600000 ms
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::new(0, 1, 3600000))),
                DataType::Interval,
            )),
            // 2 months - 5 days = 2 months, -5 days, 0 ms
            Ok(Literal::new(
                Some(ScalarImpl::Interval(IntervalUnit::new(2, -5, 0))),
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
