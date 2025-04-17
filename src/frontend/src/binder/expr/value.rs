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

use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::{
    DataType, DateTimeField, Decimal, Interval, MapType, ScalarImpl, StructType,
};
use risingwave_sqlparser::ast::{DateTimeField as AstDateTimeField, Expr, Value};
use thiserror_ext::AsReport;

use crate::binder::Binder;
use crate::error::{ErrorCode, Result};
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall, Literal, align_types};

impl Binder {
    pub fn bind_value(&mut self, value: Value) -> Result<Literal> {
        match value {
            Value::Number(s) => self.bind_number(s),
            Value::SingleQuotedString(s) => self.bind_string(s),
            Value::CstyleEscapedString(s) => self.bind_string(s.value),
            Value::Boolean(b) => self.bind_bool(b),
            // Both null and string literal will be treated as `unknown` during type inference.
            // See [`ExprImpl::is_unknown`].
            Value::Null => Ok(Literal::new_untyped(None)),
            Value::Interval {
                value,
                leading_field,
                // TODO: support more interval types.
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            } => self.bind_interval(value, leading_field),
            _ => bail_not_implemented!("value: {:?}", value),
        }
    }

    pub(super) fn bind_string(&mut self, s: String) -> Result<Literal> {
        Ok(Literal::new_untyped(Some(s)))
    }

    fn bind_bool(&mut self, b: bool) -> Result<Literal> {
        Ok(Literal::new(Some(ScalarImpl::Bool(b)), DataType::Boolean))
    }

    fn bind_number(&mut self, mut s: String) -> Result<Literal> {
        let prefix_start = match s.starts_with('-') {
            true => 1,
            false => 0,
        };
        let base = match prefix_start + 2 <= s.len() {
            true => match &s[prefix_start..prefix_start + 2] {
                // tokenizer already converts them to lowercase
                "0x" => 16,
                "0o" => 8,
                "0b" => 2,
                _ => 10,
            },
            false => 10,
        };
        if base != 10 {
            s.replace_range(prefix_start..prefix_start + 2, "");
        }

        let (data, data_type) = if let Ok(int_32) = i32::from_str_radix(&s, base) {
            (Some(ScalarImpl::Int32(int_32)), DataType::Int32)
        } else if let Ok(int_64) = i64::from_str_radix(&s, base) {
            (Some(ScalarImpl::Int64(int_64)), DataType::Int64)
        } else if let Ok(decimal) = Decimal::from_str_radix(&s, base) {
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
            Interval::parse_with_fields(&s, leading_field.map(Self::bind_date_time_field))
                .map_err(|e| ErrorCode::BindError(e.to_report_string()))?;
        let datum = Some(ScalarImpl::Interval(interval));
        let literal = Literal::new(datum, DataType::Interval);

        Ok(literal)
    }

    pub(crate) fn bind_date_time_field(field: AstDateTimeField) -> DateTimeField {
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

    pub(super) fn bind_map(&mut self, entries: Vec<(Expr, Expr)>) -> Result<ExprImpl> {
        if entries.is_empty() {
            return Err(ErrorCode::BindError("cannot determine type of empty map\nHINT:  Explicitly cast to the desired type, for example MAP{}::map(int,int).".into()).into());
        }
        let mut keys = Vec::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            keys.push(self.bind_expr_inner(k)?);
            values.push(self.bind_expr_inner(v)?);
        }
        let key_type = align_types(keys.iter_mut())?;
        let value_type = align_types(values.iter_mut())?;

        let keys: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            keys,
            DataType::List(Box::new(key_type.clone())),
        )
        .into();
        let values: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            values,
            DataType::List(Box::new(value_type.clone())),
        )
        .into();

        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::MapFromKeyValues,
            vec![keys, values],
            DataType::Map(MapType::from_kv(key_type, value_type)),
        )
        .into();
        Ok(expr)
    }

    pub(super) fn bind_array_cast(
        &mut self,
        exprs: Vec<Expr>,
        element_type: Box<DataType>,
    ) -> Result<ExprImpl> {
        let exprs = exprs
            .into_iter()
            .map(|e| self.bind_cast_inner(e, *element_type.clone()))
            .collect::<Result<Vec<ExprImpl>>>()?;

        let expr: ExprImpl =
            FunctionCall::new_unchecked(ExprType::Array, exprs, DataType::List(element_type))
                .into();
        Ok(expr)
    }

    pub(super) fn bind_map_cast(
        &mut self,
        entries: Vec<(Expr, Expr)>,
        map_type: MapType,
    ) -> Result<ExprImpl> {
        let mut keys = Vec::with_capacity(entries.len());
        let mut values = Vec::with_capacity(entries.len());
        for (k, v) in entries {
            keys.push(self.bind_cast_inner(k, map_type.key().clone())?);
            values.push(self.bind_cast_inner(v, map_type.value().clone())?);
        }

        let keys: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            keys,
            DataType::List(Box::new(map_type.key().clone())),
        )
        .into();
        let values: ExprImpl = FunctionCall::new_unchecked(
            ExprType::Array,
            values,
            DataType::List(Box::new(map_type.value().clone())),
        )
        .into();

        let expr: ExprImpl = FunctionCall::new_unchecked(
            ExprType::MapFromKeyValues,
            vec![keys, values],
            DataType::Map(map_type),
        )
        .into();
        Ok(expr)
    }

    pub(super) fn bind_index(&mut self, obj: Expr, index: Expr) -> Result<ExprImpl> {
        let obj = self.bind_expr_inner(obj)?;
        match obj.return_type() {
            DataType::List(return_type) => Ok(FunctionCall::new_unchecked(
                ExprType::ArrayAccess,
                vec![obj, self.bind_expr_inner(index)?],
                *return_type,
            )
            .into()),
            DataType::Map(m) => Ok(FunctionCall::new_unchecked(
                ExprType::MapAccess,
                vec![obj, self.bind_expr_inner(index)?],
                m.value().clone(),
            )
            .into()),
            data_type => Err(ErrorCode::BindError(format!(
                "index operator applied to type {}, which is not a list or map",
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
                "array range index applied to type {}, which is not a list",
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
            StructType::unnamed(exprs.iter().map(|e| e.return_type()).collect_vec()).into();
        let expr: ExprImpl = FunctionCall::new_unchecked(ExprType::Row, exprs, data_type).into();
        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_expr::expr::build_from_prost;
    use risingwave_sqlparser::ast::Value::Number;

    use super::*;
    use crate::binder::test_utils::mock_binder;
    use crate::expr::Expr;

    #[tokio::test]
    async fn test_bind_value() {
        use std::str::FromStr;

        let mut binder = mock_binder();
        let values = [
            "1",
            "111111111111111",
            "111111111.111111",
            "111111111111111111111111",
            "0.111111",
            "-0.01",
        ];
        let data = [
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
        let data_type = [
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
    async fn test_bind_radix() {
        let mut binder = mock_binder();

        for (input, expected) in [
            ("0x42e3", ScalarImpl::Int32(0x42e3)),
            ("-0x40", ScalarImpl::Int32(-0x40)),
            ("0b1101", ScalarImpl::Int32(0b1101)),
            ("-0b101", ScalarImpl::Int32(-0b101)),
            ("0o664", ScalarImpl::Int32(0o664)),
            ("-0o755", ScalarImpl::Int32(-0o755)),
            ("2147483647", ScalarImpl::Int32(2147483647)),
            ("2147483648", ScalarImpl::Int64(2147483648)),
            ("-2147483648", ScalarImpl::Int32(-2147483648)),
            ("0x7fffffff", ScalarImpl::Int32(0x7fffffff)),
            ("0x80000000", ScalarImpl::Int64(0x80000000)),
            ("-0x80000000", ScalarImpl::Int32(-0x80000000)),
        ] {
            let lit = binder.bind_number(input.into()).unwrap();
            assert_eq!(lit.get_data().as_ref().unwrap(), &expected);
        }
    }

    #[tokio::test]
    async fn test_bind_scientific_number() {
        use std::str::FromStr;

        let mut binder = mock_binder();
        let values = [
            ("1e6"),
            ("1.25e6"),
            ("1.25e1"),
            ("1e-2"),
            ("1.25e-2"),
            ("1e15"),
        ];
        let data = [
            Some(ScalarImpl::Decimal(Decimal::from_str("1000000").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("1250000").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("12.5").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("0.01").unwrap())),
            Some(ScalarImpl::Decimal(Decimal::from_str("0.0125").unwrap())),
            Some(ScalarImpl::Decimal(
                Decimal::from_str("1000000000000000").unwrap(),
            )),
        ];
        let data_type = [
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
            DataType::Decimal,
        ];

        for i in 0..values.len() {
            let res = binder.bind_value(Number(values[i].to_owned())).unwrap();
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
        let mut binder = mock_binder();
        let values = [
            "1 hour",
            "1 h",
            "1 year",
            "6 second",
            "2 minutes",
            "1 month",
        ];
        let data = vec![
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(60))),
                DataType::Interval,
            ),
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(60))),
                DataType::Interval,
            ),
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_ymd(1, 0, 0))),
                DataType::Interval,
            ),
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_millis(6 * 1000))),
                DataType::Interval,
            ),
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_minutes(2))),
                DataType::Interval,
            ),
            Literal::new(
                Some(ScalarImpl::Interval(Interval::from_month(1))),
                DataType::Interval,
            ),
        ];

        for i in 0..values.len() {
            let value = Value::Interval {
                value: values[i].to_owned(),
                leading_field: None,
                leading_precision: None,
                last_field: None,
                fractional_seconds_precision: None,
            };
            assert_eq!(binder.bind_value(value).unwrap(), data[i]);
        }
    }
}
