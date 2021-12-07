use crate::array::column_proto_readers::{
    read_bool_column, read_numeric_column, read_string_column,
};
use crate::array::value_reader::{
    DecimalValueReader, F32ValueReader, F64ValueReader, I16ValueReader, I32ValueReader,
    I64ValueReader, Utf8ValueReader,
};
use crate::array::{ArrayImpl, ArrayRef, DecimalArrayBuilder, Utf8ArrayBuilder};
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::types::{build_from_prost, DataType, DataTypeRef};
use crate::util::prost::pack_to_any;
use prost_types::Any as ProstAny;
use risingwave_pb::data::{data_type::TypeName, Column as ProstColumn};

/// Column is owned by `DataChunk`. It consists of logic data type and physical array
/// implementation.
#[derive(Clone, Debug)]
pub struct Column {
    array: ArrayRef,
    data_type: DataTypeRef,
}

impl Column {
    pub fn new(array: ArrayRef, data_type: DataTypeRef) -> Column {
        Column { array, data_type }
    }

    pub fn to_protobuf(&self) -> Result<ProstAny> {
        let mut column = ProstColumn {
            column_type: Some(self.data_type.to_protobuf()?),
            null_bitmap: Some(self.array.null_bitmap().to_protobuf()?),
            ..Default::default()
        };
        let values = self.array.to_protobuf()?;
        let values_ref = &mut column.values;
        for (_idx, buf) in values.into_iter().enumerate() {
            values_ref.push(buf);
        }

        Ok(pack_to_any(&column))
    }

    pub fn from_protobuf(col: &ProstColumn, cardinality: usize) -> Result<Self> {
        let type_name = col.get_column_type().get_type_name();
        let array = match type_name {
            TypeName::Int16 => read_numeric_column::<i16, I16ValueReader>(col, cardinality)?,
            TypeName::Int32 | TypeName::Date => {
                read_numeric_column::<i32, I32ValueReader>(col, cardinality)?
            }
            TypeName::Int64 | TypeName::Timestamp | TypeName::Time | TypeName::Timestampz => {
                read_numeric_column::<i64, I64ValueReader>(col, cardinality)?
            }
            TypeName::Float => read_numeric_column::<f32, F32ValueReader>(col, cardinality)?,
            TypeName::Double => read_numeric_column::<f64, F64ValueReader>(col, cardinality)?,
            TypeName::Boolean => read_bool_column(col, cardinality)?,
            TypeName::Varchar | TypeName::Char => {
                read_string_column::<Utf8ArrayBuilder, Utf8ValueReader>(col, cardinality)?
            }
            TypeName::Decimal => {
                read_string_column::<DecimalArrayBuilder, DecimalValueReader>(col, cardinality)?
            }
            _ => {
                return Err(RwError::from(InternalError(format!(
                    "unsupported conversion from Column to Array: {:?}",
                    type_name
                ))))
            }
        };
        let data_type = build_from_prost(col.get_column_type())?;
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
    use crate::array::{
        Array, ArrayBuilder, BoolArray, BoolArrayBuilder, I32Array, I32ArrayBuilder, Utf8Array,
    };
    use crate::error::Result;
    use crate::types::{BoolType, DataTypeKind, Int32Type, StringType};
    use crate::util::prost::unpack_from_any;
    use std::sync::Arc;

    // Convert a column to protobuf, then convert it back to column, and ensures the two are
    // identical.
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
            Int32Type::create(true),
        );
        let col_proto = unpack_from_any::<ProstColumn>(&col.to_protobuf().unwrap()).unwrap();
        assert!(col_proto.get_column_type().get_is_nullable());
        assert_eq!(col_proto.get_column_type().get_type_name(), TypeName::Int32);

        let new_col = Column::from_protobuf(&col_proto, cardinality).unwrap();
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
    fn test_bool_column_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = BoolArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            match i % 3 {
                0 => builder.append(Some(false)).unwrap(),
                1 => builder.append(Some(true)).unwrap(),
                _ => builder.append(None).unwrap(),
            }
        }
        let col = Column::new(
            Arc::new(ArrayImpl::from(builder.finish().unwrap())),
            BoolType::create(true),
        );
        let col_proto = unpack_from_any::<ProstColumn>(&col.to_protobuf().unwrap()).unwrap();
        assert!(col_proto.get_column_type().get_is_nullable());
        assert_eq!(
            col_proto.get_column_type().get_type_name(),
            TypeName::Boolean
        );

        let new_col = Column::from_protobuf(&col_proto, cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        assert_eq!(new_col.data_type.data_type_kind(), DataTypeKind::Boolean);
        assert!(new_col.data_type.is_nullable());
        let arr: &BoolArray = new_col.array_ref().into();
        arr.iter().enumerate().for_each(|(i, x)| match i % 3 {
            0 => assert_eq!(Some(false), x),
            1 => assert_eq!(Some(true), x),
            _ => assert_eq!(None, x),
        });
        Ok(())
    }

    #[test]
    fn test_utf8_column_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = Utf8ArrayBuilder::new(cardinality).unwrap();
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
        let col_proto = unpack_from_any::<ProstColumn>(&col.to_protobuf().unwrap()).unwrap();
        let new_col = Column::from_protobuf(&col_proto, cardinality).unwrap();
        let arr: &Utf8Array = new_col.array_ref().as_utf8();
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
