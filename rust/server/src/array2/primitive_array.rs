use super::{Array, ArrayBuilder, ArrayIterator};
use crate::types::NativeType;

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
pub struct PrimitiveArray<T: NativeType> {
    bitmap: Vec<bool>,
    data: Vec<T>,
}

impl<T: NativeType> PrimitiveArray<T> {
    pub fn from_slice(data: &[Option<T>]) -> Self {
        let mut builder = <Self as Array>::Builder::new(data.len());
        for i in data {
            builder.append(*i);
        }
        builder.finish()
    }
}

impl<T: NativeType> Array for PrimitiveArray<T> {
    type Builder = PrimitiveArrayBuilder<T>;
    type RefItem<'a> = T;
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
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
pub struct PrimitiveArrayBuilder<T: NativeType> {
    bitmap: Vec<bool>,
    data: Vec<T>,
}

impl<T: NativeType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type ArrayType = PrimitiveArray<T>;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, value: Option<T>) {
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
    }

    fn append_array(&mut self, other: &PrimitiveArray<T>) {
        self.bitmap.extend_from_slice(&other.bitmap);
        self.data.extend_from_slice(&other.data);
    }

    fn finish(self) -> PrimitiveArray<T> {
        PrimitiveArray {
            bitmap: self.bitmap,
            data: self.data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn helper_test_builder<T: NativeType>(data: Vec<Option<T>>) -> PrimitiveArray<T> {
        let mut builder = PrimitiveArrayBuilder::<T>::new(data.len());
        for d in data {
            builder.append(d);
        }
        builder.finish()
    }

    #[test]
    fn test_i16_builder() {
        helper_test_builder::<i16>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
    }

    #[test]
    fn test_i32_builder() {
        helper_test_builder::<i32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
    }

    #[test]
    fn test_i64_builder() {
        helper_test_builder::<i64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
    }

    #[test]
    fn test_f32_builder() {
        helper_test_builder::<f32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x as f32) })
                .collect(),
        );
    }

    #[test]
    fn test_f64_builder() {
        helper_test_builder::<f64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x as f64) })
                .collect(),
        );
    }
}
