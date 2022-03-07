use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataType, Decimal, ScalarImpl};
use risingwave_sqlparser::ast::Value;

use crate::binder::Binder;
use crate::expr::Literal;

impl Binder {
    pub(super) fn bind_value(&mut self, value: Value) -> Result<Literal> {
        match value {
            Value::Number(s, b) => self.bind_number(s, b),
            Value::SingleQuotedString(s) => {
                Ok(Literal::new(Some(ScalarImpl::Utf8(s)), DataType::Varchar))
            }
            Value::Boolean(b) => self.bind_bool(b),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", value)).into()),
        }
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
            let decimal = s
                .parse::<Decimal>()
                .map_err(|e| ErrorCode::ParseError(Box::new(e)))?;
            (Some(ScalarImpl::Decimal(decimal)), DataType::Decimal)
        };
        Ok(Literal::new(data, data_type))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::catalog::database_catalog::DatabaseCatalog;

    #[test]
    fn test_bind_value() {
        use std::str::FromStr;

        use super::*;

        let catalog = DatabaseCatalog::new(0);
        let mut binder = Binder::new(Arc::new(catalog));
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
}
