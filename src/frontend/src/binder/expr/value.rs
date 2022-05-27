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
            // We just bind a dummy type (Boolean) for null here, and its type will be changed
            // according to its context later.
            Value::Null => Ok(Literal::new(None, DataType::Boolean)),
            Value::Interval {
                value,
                leading_field,
                // TODO: support more interval types.
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => self.bind_interval(value, leading_field),
            _ => Err(ErrorCode::NotImplemented(format!("{:?}", value), None.into()).into()),
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
        let unit = leading_field.unwrap_or(DateTimeField::Second);
        use DateTimeField::*;
        let interval = (|| match unit {
            Year => {
                let years = s.parse::<i32>().ok()?;
                let months = years.checked_mul(12)?;
                Some(IntervalUnit::from_month(months))
            }
            Month => {
                let months = s.parse::<i32>().ok()?;
                Some(IntervalUnit::from_month(months))
            }
            Day => {
                let days = s.parse::<i32>().ok()?;
                Some(IntervalUnit::from_days(days))
            }
            Hour => {
                let hours = s.parse::<i64>().ok()?;
                let ms = hours.checked_mul(3600 * 1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
            Minute => {
                let minutes = s.parse::<i64>().ok()?;
                let ms = minutes.checked_mul(60 * 1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
            Second => {
                let seconds = s.parse::<i64>().ok()?;
                let ms = seconds.checked_mul(1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
        })()
        .ok_or_else(|| {
            RwError::from(ErrorCode::InvalidInputSyntax(format!(
                "Invalid interval {}.",
                s
            )))
        })?;

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
    
    pub(super) fn bind_array_index(&mut self, obj: Box<Expr>, indexs: Vec<Expr>) -> Result<ExprImpl> {

        let obj = self.bind_expr(*obj)?;
        match obj.return_type() {
            DataType::List { datatype: return_type } => {
                let mut indexs = indexs
                    .into_iter()
                    .map(|e| self.bind_expr(e))
                    .collect::<Result<Vec<ExprImpl>>>()?;
                indexs.insert(0, obj);
                
                let expr: ExprImpl = FunctionCall::new_unchecked(
                    ExprType::ArrayAccess,
                    indexs,
                    *return_type
                )
                .into();
                Ok(expr)
            },
            _ => panic!("Should be a List")
        }
    }
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
            DataType::Int32
        )
        .into();

        let expr_pb = expr.to_expr_proto();
        let expr = build_from_prost(&expr_pb).unwrap();
        assert_eq!(expr.return_type(), DataType::Int32);
    }
}
