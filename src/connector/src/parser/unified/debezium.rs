use risingwave_common::types::{DataType, ScalarImpl};

use super::{Access, ChangeEvent, ChangeEventOperation};

pub struct DebeziumAdapter<A> {
    accessor: A,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";

pub const DEBEZIUM_READ_OP: &str = "r";
pub const DEBEZIUM_CREATE_OP: &str = "c";
pub const DEBEZIUM_UPDATE_OP: &str = "u";
pub const DEBEZIUM_DELETE_OP: &str = "d";

impl<A> DebeziumAdapter<A> {
    pub fn new(accessor: A) -> Self
    where
        A: Access,
    {
        Self { accessor }
    }
}

impl<A> ChangeEvent for DebeziumAdapter<A>
where
    A: Access,
{
    fn access_field(
        &self,
        name: &str,
        type_expected: &risingwave_common::types::DataType,
    ) -> super::AccessResult {
        match self.op()? {
            ChangeEventOperation::Delete => {
                self.accessor.access(&[BEFORE, name], Some(type_expected))
            }
            _ => self.accessor.access(&[AFTER, name], Some(type_expected)),
        }
    }

    fn op(&self) -> std::result::Result<ChangeEventOperation, super::AccessError> {
        if let Some(ScalarImpl::Utf8(op)) = self.accessor.access(&[OP], Some(&DataType::Varchar))? {
            match op.as_ref() {
                DEBEZIUM_READ_OP | DEBEZIUM_CREATE_OP | DEBEZIUM_UPDATE_OP => {
                    return Ok(ChangeEventOperation::Upsert)
                }
                DEBEZIUM_DELETE_OP => return Ok(ChangeEventOperation::Delete),
                _ => (),
            }
        }
        Err(super::AccessError::Undefined {
            name: "op".into(),
            path: Default::default(),
        })
    }
}
