use risingwave_common::types::{DataType, ScalarImpl};

use super::{Access, OperateRow, RowOperation};

pub struct DebeziumAdapter<A> {
    pub accessor: A,
}
const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";
pub const DEBEZIUM_READ_OP: &str = "r";
pub const DEBEZIUM_CREATE_OP: &str = "c";
pub const DEBEZIUM_UPDATE_OP: &str = "u";
pub const DEBEZIUM_DELETE_OP: &str = "d";

impl<A> OperateRow for DebeziumAdapter<A>
where
    A: Access,
{
    fn access_field(
        &self,
        name: &str,
        shape: &risingwave_common::types::DataType,
    ) -> super::AccessResult {
        self.accessor.access(&[AFTER, name], shape.clone())
    }

    fn access_before(
        &self,
        name: &str,
        shape: &risingwave_common::types::DataType,
    ) -> super::AccessResult {
        self.accessor.access(&[BEFORE, name], shape.clone())
    }

    fn op(&self) -> std::result::Result<RowOperation, super::AccessError> {
        if let Some(ScalarImpl::Utf8(op)) = self.accessor.access(&[OP], DataType::Varchar)? {
            match op.as_ref() {
                DEBEZIUM_READ_OP | DEBEZIUM_CREATE_OP => return Ok(RowOperation::Insert),
                DEBEZIUM_UPDATE_OP => return Ok(RowOperation::Update),
                DEBEZIUM_DELETE_OP => return Ok(RowOperation::Delete),
                _ => (),
            }
        }
        Err(super::AccessError::Undefined {
            name: "op".into(),
            path: Default::default(),
        })
    }
}
