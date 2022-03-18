use std::sync::Arc;

use risingwave_pb::data::Column as ProstColumn;

use crate::array::{ArrayImpl, ArrayRef};
use crate::error::Result;

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

    pub fn to_protobuf(&self) -> ProstColumn {
        let array = self.array.to_protobuf();
        ProstColumn { array: Some(array) }
    }

    pub fn from_protobuf(col: &ProstColumn, cardinality: usize) -> Result<Self> {
        Ok(Column {
            array: Arc::new(ArrayImpl::from_protobuf(col.get_array()?, cardinality)?),
        })
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

    use num_traits::FromPrimitive;

    use super::*;
    use crate::array::{
        Array, ArrayBuilder, BoolArray, BoolArrayBuilder, DecimalArray, DecimalArrayBuilder,
        I32Array, I32ArrayBuilder, NaiveDateArray, NaiveDateArrayBuilder, NaiveDateTimeArray,
        NaiveDateTimeArrayBuilder, NaiveTimeArray, NaiveTimeArrayBuilder, Utf8Array,
        Utf8ArrayBuilder,
    };
    use crate::error::Result;
    use crate::types::{Decimal, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};

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
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
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
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
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
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
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

    #[test]
    fn test_decimal_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = DecimalArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Decimal::from_usize(i)).unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        let arr: &DecimalArray = new_col.array_ref().as_decimal();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(Decimal::from_usize(i).unwrap(), x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_naivedate_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = NaiveDateArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder
                    .append(NaiveDateWrapper::new_with_days(i as i32).ok())
                    .unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        let arr: &NaiveDateArray = new_col.array_ref().as_naivedate();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    NaiveDateWrapper::new_with_days(i as i32).ok().unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_naivetime_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = NaiveTimeArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder
                    .append(NaiveTimeWrapper::new_with_secs_nano(i as u32, i as u32 * 1000).ok())
                    .unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        let arr: &NaiveTimeArray = new_col.array_ref().as_naivetime();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    NaiveTimeWrapper::new_with_secs_nano(i as u32, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_naivedatetime_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = NaiveDateTimeArrayBuilder::new(cardinality).unwrap();
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder
                    .append(
                        NaiveDateTimeWrapper::new_with_secs_nsecs(i as i64, i as u32 * 1000).ok(),
                    )
                    .unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        let col = Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())));
        let new_col = Column::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.array.len(), cardinality);
        let arr: &NaiveDateTimeArray = new_col.array_ref().as_naivedatetime();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    NaiveDateTimeWrapper::new_with_secs_nsecs(i as i64, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }
}
