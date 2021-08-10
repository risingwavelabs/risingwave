use crate::types::{DataType, NativeType};

/// Data types whose value contains only one buffer.
pub(crate) trait PrimitiveDataType: DataType {
    type N: NativeType;
}
