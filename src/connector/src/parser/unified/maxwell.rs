use risingwave_common::types::{DataType, ScalarImpl};

use super::{Access, ChangeEvent};
use crate::parser::unified::ChangeEventOperation;

pub const MAXWELL_INSERT_OP: &str = "insert";
pub const MAXWELL_UPDATE_OP: &str = "update";
pub const MAXWELL_DELETE_OP: &str = "delete";

pub struct MaxwellChangeEvent<A>(A);

impl<A> MaxwellChangeEvent<A> {
    pub fn new(accessor: A) -> Self {
        Self(accessor)
    }
}

impl<A> ChangeEvent for MaxwellChangeEvent<A>
where
    A: Access,
{
    fn op(&self) -> std::result::Result<super::ChangeEventOperation, super::AccessError> {
        const OP: &str = "type";
        if let Some(ScalarImpl::Utf8(op)) = self.0.access(&[OP], Some(&DataType::Varchar))? {
            match op.as_ref() {
                MAXWELL_INSERT_OP | MAXWELL_UPDATE_OP => return Ok(ChangeEventOperation::Upsert),
                MAXWELL_DELETE_OP => return Ok(ChangeEventOperation::Delete),
                _ => (),
            }
        }
        Err(super::AccessError::Undefined {
            name: "op".into(),
            path: Default::default(),
        })
    }

    fn access_field(&self, name: &str, type_expected: &DataType) -> super::AccessResult {
        const DATA: &str = "data";
        self.0.access(&[DATA, name], Some(type_expected))
    }
}
