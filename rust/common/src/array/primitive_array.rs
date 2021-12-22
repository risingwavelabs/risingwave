use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::mem::size_of;

use risingwave_pb::data::buffer::CompressionType;
use risingwave_pb::data::{Array as ProstArray, ArrayType, Buffer};

use super::{Array, ArrayBuilder, ArrayIterator, NULL_VAL_FOR_HASH};
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;
use crate::for_all_native_types;
use crate::types::{NativeType, Scalar, ScalarRef};

/// Physical type of array items. It differs from `NativeType` with more limited type set.
/// Specifically, it doesn't support u8/u16/u32/u64.
#[rustfmt::skip]
pub trait PrimitiveArrayItemType
where
  for<'a> Self: NativeType + Scalar<ScalarRefType<'a> = Self> + ScalarRef<'a, ScalarType = Self>,
{
  /// A helper to convert a primitive array to `ArrayImpl`.
  fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl;

  /// A helper to convert `ArrayImpl` to self.
  fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>>;

  /// A helper to convert `ArrayImpl` to self.
  fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>>;

  /// Returns array type of the primitive array
  fn array_type() -> ArrayType;

  /// Creates an `ArrayBuilder` for this primitive type
  fn create_array_builder(capacity: usize) -> Result<ArrayBuilderImpl>;
}

macro_rules! impl_primitive_array_item_type {
  ([], $({ $scalar_type:ty, $variant_type:ident } ),*) => {
    $(
      impl PrimitiveArrayItemType for $scalar_type {
        fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl {
          ArrayImpl::$variant_type(arr)
        }

        fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>> {
          match arr {
            ArrayImpl::$variant_type(inner) => Some(inner),
            _ => None,
          }
        }

        fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>> {
          match arr {
            ArrayImpl::$variant_type(inner) => Some(inner),
            _ => None,
          }
        }

        fn array_type() -> ArrayType {
          ArrayType::$variant_type
        }

        fn create_array_builder(capacity: usize) -> Result<ArrayBuilderImpl> {
          let array_builder = PrimitiveArrayBuilder::<$scalar_type>::new(capacity)?;
          Ok(ArrayBuilderImpl::$variant_type(array_builder))
        }
      }
    )*
  };
}

for_all_native_types! { impl_primitive_array_item_type }

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(Debug)]
pub struct PrimitiveArray<T: PrimitiveArrayItemType> {
    bitmap: Bitmap,
    data: Vec<T>,
}

impl<T: PrimitiveArrayItemType> PrimitiveArray<T> {
    pub fn from_slice(data: &[Option<T>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl<T: PrimitiveArrayItemType> Array for PrimitiveArray<T> {
    type Builder = PrimitiveArrayBuilder<T>;
    type RefItem<'a> = T;
    type OwnedItem = T;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<T> {
        if !self.is_null(idx) {
            Some(self.data[idx])
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn iter(&self) -> Self::Iter<'_> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> Result<ProstArray> {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<T>());

        for v in self.iter() {
            v.map(|node| node.to_protobuf(&mut output_buffer));
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf()?;
        Ok(ProstArray {
            null_bitmap: Some(null_bitmap),
            values: vec![buffer],
            array_type: T::array_type() as i32,
        })
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            self.data[idx].hash_wrapper(state);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }

    fn create_builder(&self, capacity: usize) -> Result<ArrayBuilderImpl> {
        T::create_array_builder(capacity)
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
#[derive(Debug)]
pub struct PrimitiveArrayBuilder<T: PrimitiveArrayItemType> {
    bitmap: BitmapBuilder,
    data: Vec<T>,
}

impl<T: PrimitiveArrayItemType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type ArrayType = PrimitiveArray<T>;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<T>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.append(false);
                self.data.push(T::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &PrimitiveArray<T>) -> Result<()> {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(mut self) -> Result<PrimitiveArray<T>> {
        Ok(PrimitiveArray {
            bitmap: self.bitmap.finish(),
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OrderedF32, OrderedF64};

    fn helper_test_builder<T: PrimitiveArrayItemType>(
        data: Vec<Option<T>>,
    ) -> Result<PrimitiveArray<T>> {
        let mut builder = PrimitiveArrayBuilder::<T>::new(data.len())?;
        for d in data {
            builder.append(d)?;
        }
        builder.finish()
    }

    #[test]
    fn test_i16_builder() {
        let arr = helper_test_builder::<i16>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int16(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i32_builder() {
        let arr = helper_test_builder::<i32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i64_builder() {
        let arr = helper_test_builder::<i64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int64(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f32_builder() {
        let arr = helper_test_builder::<OrderedF32>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f32).into())
                    }
                })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f64_builder() {
        let arr = helper_test_builder::<OrderedF64>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f64).into())
                    }
                })
                .collect(),
        )
        .unwrap();
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float64(_)) {
            unreachable!()
        }
    }
}
