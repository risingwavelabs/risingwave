use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Values;

use crate::binder::Binder;

#[derive(Debug)]
pub struct BoundValues {}

impl Binder {
    pub(super) fn bind_values(&mut self, _values: Values) -> Result<BoundValues> {
        Ok(BoundValues {})
    }
}
