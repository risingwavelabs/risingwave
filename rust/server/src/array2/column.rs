use crate::array2::value_reader::{
    F32ValueReader, F64ValueReader, I16ValueReader, I32ValueReader, I64ValueReader, ValueReader,
};
use crate::array2::{
    ArrayBuilder, ArrayImpl, ArrayRef, PrimitiveArrayBuilder, PrimitiveArrayItemType,
};
use crate::buffer::Bitmap;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::types::{build_from_proto, DataType, DataTypeRef};
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::{Column as ColumnProto, DataType_TypeName};
use std::io::Cursor;
use std::sync::Arc;

/// Column is owned by DataChunk. It consists of logic data type and physical array implementation.
#[derive(Clone, Debug)]
pub struct Column {
    array: ArrayRef,
    data_type: DataTypeRef,
}

fn read_numeric_column<T: PrimitiveArrayItemType, R: ValueReader<T>>(
    col: &ColumnProto,
    cardinality: usize,
) -> Result<ArrayRef> {
    ensure!(
        col.get_values().len() == 1,
        "Must have only 1 buffer in a numeric column"
    );

    let buf = col.get_values()[0].get_body();
    let value_size = std::mem::size_of::<T>();
    ensure!(
        buf.len() % value_size == 0,
        "Unexpected memory layout of numeric column"
    );

    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality)?;
    let bitmap = Bitmap::from_protobuf(col.get_null_bitmap())?;
    let mut cursor = Cursor::new(buf);
    for not_null in bitmap.iter() {
        if not_null {
            let v = R::read(&mut cursor)?;
            builder.append(Some(v))?;
        } else {
            builder.append(None)?;
        }
    }
    let arr = builder.finish()?;
    Ok(Arc::new(arr.into()))
}

impl Column {
    pub fn new(array: ArrayRef, data_type: DataTypeRef) -> Column {
        Column { array, data_type }
    }

    pub fn to_protobuf(&self) -> Result<AnyProto> {
        let mut column = ColumnProto::new();
        let proto_data_type = self.data_type.to_protobuf()?;
        column.set_column_type(proto_data_type);
        column.set_null_bitmap(self.array.null_bitmap().to_protobuf()?);
        let values = self.array.to_protobuf()?;
        for (_idx, buf) in values.into_iter().enumerate() {
            column.mut_values().push(buf);
        }

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }

    pub fn from_protobuf(col: ColumnProto, cardinality: usize) -> Result<Self> {
        let array = match col.get_column_type().get_type_name() {
            DataType_TypeName::INT16 => {
                read_numeric_column::<i16, I16ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT32 => {
                read_numeric_column::<i32, I32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT64 => {
                read_numeric_column::<i64, I64ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::FLOAT => {
                read_numeric_column::<f32, F32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::DOUBLE => {
                read_numeric_column::<f64, F64ValueReader>(&col, cardinality)?
            }
            _ => {
                return Err(RwError::from(InternalError(
                    "unsupported conversion from Column to Array".to_string(),
                )))
            }
        };
        let data_type = build_from_proto(col.get_column_type())?;
        Ok(Self { array, data_type })
    }

    pub fn array(&self) -> ArrayRef {
        self.array.clone()
    }

    pub fn array_ref(&self) -> &ArrayImpl {
        &*self.array
    }

    pub fn data_type(&self) -> DataTypeRef {
        self.data_type.clone()
    }

    pub fn data_type_ref(&self) -> &dyn DataType {
        &*self.data_type
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::{Array, I32Array, I32ArrayBuilder};
    use crate::error::{ErrorCode, Result};
    use crate::types::{DataTypeKind, Int32Type};
    use protobuf::Message;
    use risingwave_proto::data::Column as ColumnProto;

    // Convert a column to protobuf, then convert it back to column, and ensures the two are identical.
    #[test]
    fn test_column_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = I32ArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some(i as i32)).unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(
            Arc::new(ArrayImpl::from(builder.finish().unwrap())),
            Arc::new(Int32Type::new(true)),
        );
        let col_proto = unpack_from_any!(col.to_protobuf().unwrap(), ColumnProto);
        assert!(col_proto.get_column_type().get_is_nullable());
        assert_eq!(
            col_proto.get_column_type().get_type_name(),
            DataType_TypeName::INT32
        );

        let new_col = Column::from_protobuf(col_proto, cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        assert_eq!(new_col.data_type.data_type_kind(), DataTypeKind::Int32);
        assert!(new_col.data_type.is_nullable());
        let arr: &I32Array = new_col.array_ref().as_int32();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(i as i32, x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }
}
