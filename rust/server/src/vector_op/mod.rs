use crate::array::ArrayRef;
use crate::error::Result;
use crate::types::{DataTypeKind, DataTypeRef};

pub(crate) fn vector_cast(
    un_casted_array: ArrayRef,
    src_type: DataTypeRef,
    dst_type: DataTypeRef,
) -> Result<ArrayRef> {
    match (src_type.data_type_kind(), dst_type.data_type_kind()) {
        (DataTypeKind::Char, DataTypeKind::Date) => cast_string_to_date(un_casted_array),

        _ => unimplemented!(),
    }
}

fn cast_string_to_date(_input: ArrayRef) -> Result<ArrayRef> {
    todo!()
}
