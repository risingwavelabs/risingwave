use crate::array::BoxedArrayBuilder;
use crate::types::{DataType, DataTypeKind};
use std::sync::Arc;

/// [BoolType] is not a primitive type because we use a bit for each bool value, not a [bool].
pub(crate) struct BoolType {
    nullable: bool,
}

impl DataType for BoolType {
    fn data_type_kind(&self) -> DataTypeKind {
        DataTypeKind::Boolean
    }

    fn is_nullable(&self) -> bool {
        self.nullable
    }

    fn create_array_builder(
        self: Arc<Self>,
        _capacity: usize,
    ) -> crate::error::Result<BoxedArrayBuilder> {
        todo!()
    }
}
