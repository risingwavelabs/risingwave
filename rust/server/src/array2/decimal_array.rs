use super::{Array, ArrayBuilder, ArrayIterator};
use crate::buffer::Bitmap;
use crate::error::Result;

use risingwave_proto::data::Buffer;
use rust_decimal::Decimal;

#[derive(Debug)]
pub struct DecimalArray {
    bitmap: Bitmap,
    data: Vec<Decimal>,
}

impl DecimalArray {
    pub fn from_slice(data: &[Option<Decimal>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for DecimalArray {
    type Builder = DecimalArrayBuilder;
    type RefItem<'a> = Decimal;
    type OwnedItem = Decimal;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<Decimal> {
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

    fn to_protobuf(&self) -> Result<Vec<Buffer>> {
        todo!()
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }
}

/// `DecimalArrayBuilder` constructs a `DecimalArray` from `Option<Decimal>`.
pub struct DecimalArrayBuilder {
    bitmap: Vec<bool>,
    data: Vec<Decimal>,
}

impl ArrayBuilder for DecimalArrayBuilder {
    type ArrayType = DecimalArray;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<Decimal>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.push(false);
                self.data.push(Decimal::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &DecimalArray) -> Result<()> {
        self.bitmap.extend(other.bitmap.iter());
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> Result<DecimalArray> {
        Ok(DecimalArray {
            bitmap: Bitmap::from_vec(self.bitmap)?,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::FromPrimitive;

    #[test]
    fn test_decimal_builder() {
        let v = (0..1000)
            .map(Decimal::from_i64)
            .collect::<Vec<Option<Decimal>>>();
        let mut builder = DecimalArrayBuilder::new(0).unwrap();
        for i in &v {
            builder.append(*i).unwrap();
        }
        let a = builder.finish().unwrap();
        let res = v.iter().zip(a.iter()).all(|(a, b)| *a == b);
        assert_eq!(res, true);
    }
}
