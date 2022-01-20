use risingwave_pb::data::{ArrayType, Column as ProstColumn};

use crate::array::column_proto_readers::{read_bool_array, read_numeric_array, read_string_array};
use crate::array::value_reader::{
    DecimalValueReader, F32ValueReader, F64ValueReader, I16ValueReader, I32ValueReader,
    I64ValueReader, Utf8ValueReader,
};
use crate::array::{ArrayImpl, ArrayRef, DecimalArrayBuilder, Utf8ArrayBuilder};
use crate::error::Result;
use crate::types::{OrderedF32, OrderedF64};

/// Column is owned by `DataChunk`. It consists of logic data type and physical array
/// implementation.
#[derive(Clone, Debug)]
pub struct Column {
    array: ArrayRef,
}

impl Column {
    pub fn new(array: ArrayRef) -> Column {
        Column { array }
    }

    pub fn to_protobuf(&self) -> Result<ProstColumn> {
        let array = self.array.to_protobuf()?;
        let column = ProstColumn { array: Some(array) };
        Ok(column)
    }

    pub fn from_protobuf(col: &ProstColumn, cardinality: usize) -> Result<Self> {
        let array = col.get_array()?;
        let array = match array.array_type() {
            ArrayType::Int16 => read_numeric_array::<i16, I16ValueReader>(array, cardinality)?,
            ArrayType::Int32 => read_numeric_array::<i32, I32ValueReader>(array, cardinality)?,
            ArrayType::Int64 => read_numeric_array::<i64, I64ValueReader>(array, cardinality)?,
            ArrayType::Float32 => {
                read_numeric_array::<OrderedF32, F32ValueReader>(array, cardinality)?
            }
            ArrayType::Float64 => {
                read_numeric_array::<OrderedF64, F64ValueReader>(array, cardinality)?
            }
            ArrayType::Bool => read_bool_array(array, cardinality)?,
            ArrayType::Utf8 => {
                read_string_array::<Utf8ArrayBuilder, Utf8ValueReader>(array, cardinality)?
            }
            ArrayType::Decimal => {
                read_string_array::<DecimalArrayBuilder, DecimalValueReader>(array, cardinality)?
            }
        };
        Ok(Self { array })
    }

    pub fn array(&self) -> ArrayRef {
        self.array.clone()
    }

    pub fn array_ref(&self) -> &ArrayImpl {
        &*self.array
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::{
        Array, ArrayBuilder, BoolArray, BoolArrayBuilder, I32Array, I32ArrayBuilder, Utf8Array,
    };
    use crate::error::Result;

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
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf()?, cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
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
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf()?, cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
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
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf()?, cardinality).unwrap();
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
