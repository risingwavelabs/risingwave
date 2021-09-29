use crate::array2::column_proto_readers::{read_numeric_column, read_string_column};
use crate::array2::value_reader::{
    DecimalValueReader, F32ValueReader, F64ValueReader, I16ValueReader, I32ValueReader,
    I64ValueReader, Utf8ValueReader,
};
use crate::array2::{ArrayImpl, ArrayRef, DecimalArrayBuilder, UTF8ArrayBuilder};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::types::{build_from_proto, DataType, DataTypeRef};
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::{Column as ColumnProto, DataType_TypeName};

/// Column is owned by DataChunk. It consists of logic data type and physical array implementation.
#[derive(Clone, Debug)]
pub struct Column {
    array: ArrayRef,
    data_type: DataTypeRef,
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
        let type_name = col.get_column_type().get_type_name();
        let array = match type_name {
            DataType_TypeName::INT16 => {
                read_numeric_column::<i16, I16ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT32 | DataType_TypeName::DATE => {
                read_numeric_column::<i32, I32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::INT64
            | DataType_TypeName::TIMESTAMP
            | DataType_TypeName::TIME
            | DataType_TypeName::TIMESTAMPZ => {
                read_numeric_column::<i64, I64ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::FLOAT => {
                read_numeric_column::<f32, F32ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::DOUBLE => {
                read_numeric_column::<f64, F64ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::VARCHAR | DataType_TypeName::CHAR => {
                read_string_column::<UTF8ArrayBuilder, Utf8ValueReader>(&col, cardinality)?
            }
            DataType_TypeName::DECIMAL => {
                read_string_column::<DecimalArrayBuilder, DecimalValueReader>(&col, cardinality)?
            }
            _ => {
                return Err(RwError::from(InternalError(format!(
                    "unsupported conversion from Column to Array: {:?}",
                    type_name
                ))))
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
    use crate::array2::{Array, ArrayBuilder, I32Array, I32ArrayBuilder, UTF8Array};
    use crate::error::{ErrorCode, Result};
    use crate::types::{DataTypeKind, Int32Type, StringType};
    use protobuf::Message;
    use risingwave_proto::data::Column as ColumnProto;
    use std::sync::Arc;

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

    #[test]
    fn test_utf8_column_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = UTF8ArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some("abc")).unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(
            Arc::new(ArrayImpl::from(builder.finish().unwrap())),
            StringType::create(true, 0, DataTypeKind::Varchar),
        );
        let col_proto = unpack_from_any!(col.to_protobuf().unwrap(), ColumnProto);
        let new_col = Column::from_protobuf(col_proto, cardinality).unwrap();
        let arr: &UTF8Array = new_col.array_ref().as_utf8();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!("abc", x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }
}
