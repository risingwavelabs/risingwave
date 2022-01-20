use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{DataTypeKind, ScalarImpl};
use risingwave_sqlparser::ast::Value;

use crate::binder::Binder;
use crate::expr::BoundLiteral;

impl Binder {
    pub(super) fn bind_value(&mut self, value: Value) -> Result<BoundLiteral> {
        match value {
            Value::Number(s, b) => self.bind_number(s, b),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", value)).into()),
        }
    }

    fn bind_number(&mut self, s: String, _b: bool) -> Result<BoundLiteral> {
        let n: i32 = s.parse().map_err(|e| ErrorCode::ParseError(Box::new(e)))?;
        let data = Some(ScalarImpl::Int32(n));
        let data_type = DataTypeKind::Int32;
        Ok(BoundLiteral::new(data, data_type))
    }
}
