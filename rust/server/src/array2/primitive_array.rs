use super::{Array, ArrayBuilder, ArrayIterator};

use crate::buffer::Bitmap;
use crate::error::ErrorCode::ProtobufError;
use crate::error::Result;
use crate::error::RwError;
use crate::types::{DataType, Int16Type, Int32Type, Int64Type, NativeType};
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::{Buffer as BufferProto, Buffer_CompressionType, Column};
use std::mem::size_of;
/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(Debug)]
pub struct PrimitiveArray<T: NativeType> {
    bitmap: Vec<bool>,
    data: Vec<T>,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_slice(data: &[Option<T>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    type Builder = PrimitiveArrayBuilder<T>;
    type RefItem<'a> = T;
    type OwnedItem = T;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<T> {
        if self.bitmap[idx] {
            Some(self.data[idx])
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> crate::error::Result<AnyProto> {
        let mut column = Column::new();
        // FIXME: remove this hack
        let proto_data_type = match std::mem::size_of::<T>() {
            2 => Int16Type::new(false).to_protobuf()?,
            4 => Int32Type::new(false).to_protobuf()?,
            8 => Int64Type::new(false).to_protobuf()?,
            _ => unimplemented!(),
        };
        column.set_column_type(proto_data_type);
        // FIXME: remove Bitmap or made it into array.
        column.set_null_bitmap(Bitmap::from_vec(self.bitmap.clone())?.to_protobuf()?);
        let values = {
            let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<T>());

            for v in self.iter() {
                v.map(|node| node.to_protobuf(&mut output_buffer));
            }

            let mut b = BufferProto::new();
            b.set_compression(Buffer_CompressionType::NONE);
            b.set_body(output_buffer);
            b
        };

        column.mut_values().push(values);

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
pub struct PrimitiveArrayBuilder<T: NativeType> {
    bitmap: Vec<bool>,
    data: Vec<T>,
}

impl<T: NativeType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type ArrayType = PrimitiveArray<T>;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.push(false);
                self.data.push(T::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &PrimitiveArray<T>) -> Result<()> {
        self.bitmap.extend_from_slice(&other.bitmap);
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> Result<PrimitiveArray<T>> {
        Ok(PrimitiveArray {
            bitmap: self.bitmap,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn helper_test_builder<T: NativeType>(data: Vec<Option<T>>) -> Result<PrimitiveArray<T>> {
        let mut builder = PrimitiveArrayBuilder::<T>::new(data.len())?;
        for d in data {
            builder.append(d)?;
        }
        builder.finish()
    }

    #[test]
    fn test_i16_builder() {
        helper_test_builder::<i16>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
    }

    #[test]
    fn test_i32_builder() {
        helper_test_builder::<i32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
    }

    #[test]
    fn test_i64_builder() {
        helper_test_builder::<i64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
    }

    #[test]
    fn test_f32_builder() {
        helper_test_builder::<f32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x as f32) })
                .collect(),
        )
        .unwrap();
    }

    #[test]
    fn test_f64_builder() {
        helper_test_builder::<f64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x as f64) })
                .collect(),
        )
        .unwrap();
    }
}
