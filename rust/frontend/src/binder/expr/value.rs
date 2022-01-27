use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataTypeKind, Decimal, ScalarImpl};
use risingwave_sqlparser::ast::Value;

use crate::binder::Binder;
use crate::expr::BoundLiteral;

impl Binder {
    pub(super) fn bind_value(&mut self, value: Value) -> Result<BoundLiteral> {
        match value {
            Value::Number(s, b) => self.bind_number(s, b),
            Value::SingleQuotedString(s) => Ok(BoundLiteral::new(
                Some(ScalarImpl::Utf8(s)),
                DataTypeKind::Varchar,
            )),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", value)).into()),
        }
    }

    fn bind_number(&mut self, s: String, _b: bool) -> Result<BoundLiteral> {
        let (data, data_type) = if let Ok(int_32) = s.parse::<i32>() {
            (Some(ScalarImpl::Int32(int_32)), DataTypeKind::Int32)
        } else if let Ok(int_64) = s.parse::<i64>() {
            (Some(ScalarImpl::Int64(int_64)), DataTypeKind::Int64)
        } else {
            // Notice: when the length of decimal exceeds 29(>= 30), it will be rounded up.
            let decimal = s
                .parse::<Decimal>()
                .map_err(|e| ErrorCode::ParseError(Box::new(e)))?;
            let scale = decimal.scale() as u32;
            let prec = decimal.precision();
            (
                Some(ScalarImpl::Decimal(decimal)),
                DataTypeKind::Decimal { prec, scale },
            )
        };
        Ok(BoundLiteral::new(data, data_type))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_bind_value() {
        use std::str::FromStr;

        use super::*;

        let mut binder = Binder {};
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
            DataTypeKind::Int32,
            DataTypeKind::Int64,
            DataTypeKind::Decimal { prec: 15, scale: 6 },
            DataTypeKind::Decimal { prec: 24, scale: 0 },
            DataTypeKind::Decimal { prec: 7, scale: 6 },
            DataTypeKind::Decimal { prec: 3, scale: 2 },
        ];

        for i in 0..values.len() {
            let value = Value::Number(String::from(values[i]), false);
            let res = binder.bind_value(value).unwrap();
            let ans = BoundLiteral::new(data[i].clone(), data_type[i]);
            assert_eq!(res, ans);
        }
    }
}
