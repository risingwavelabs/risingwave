use crate::types::{DataType, NativeType};

/// Data types whose value contains only one buffer, and value can be represented by some rust
/// native type.
pub(crate) trait PrimitiveDataType: DataType {
    type N: NativeType;
}
