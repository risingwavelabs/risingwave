use crate::types::{DataType, DataTypeKind, NativeType};

/// Data types whose value contains only one buffer, and value can be represented by some rust
/// native type.
pub(crate) trait PrimitiveDataType: DataType + Default {
    const DATA_TYPE_KIND: DataTypeKind;
    type N: NativeType;
}
