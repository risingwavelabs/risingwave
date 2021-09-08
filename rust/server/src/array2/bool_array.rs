use super::{Array, ArrayBuilder, ArrayIterator};
use crate::error::Result;
use protobuf::well_known_types::Any as AnyProto;

#[derive(Debug)]
pub struct BoolArray {
    bitmap: Vec<bool>,
    data: Vec<bool>,
}

impl BoolArray {
    pub fn from_slice(data: &[Option<bool>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

impl Array for BoolArray {
    type Builder = BoolArrayBuilder;
    type RefItem<'a> = bool;
    type OwnedItem = bool;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<bool> {
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

    fn to_protobuf(&self) -> Result<AnyProto> {
        todo!()
    }
}

/// `BoolArrayBuilder` constructs a `BoolArray` from `Option<Bool>`.
pub struct BoolArrayBuilder {
    bitmap: Vec<bool>,
    data: Vec<bool>,
}

impl ArrayBuilder for BoolArrayBuilder {
    type ArrayType = BoolArray;

    fn new(capacity: usize) -> Result<Self> {
        Ok(Self {
            bitmap: Vec::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        })
    }

    fn append(&mut self, value: Option<bool>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.push(x);
            }
            None => {
                self.bitmap.push(false);
                self.data.push(bool::default());
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &BoolArray) -> Result<()> {
        self.bitmap.extend_from_slice(&other.bitmap);
        self.data.extend_from_slice(&other.data);
        Ok(())
    }

    fn finish(self) -> Result<BoolArray> {
        Ok(BoolArray {
            bitmap: self.bitmap,
            data: self.data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn helper_test_builder(data: Vec<Option<bool>>) -> BoolArray {
        let mut builder = BoolArrayBuilder::new(data.len()).unwrap();
        for d in data {
            builder.append(d).unwrap();
        }
        builder.finish().unwrap()
    }

    #[test]
    fn test_bool_builder() {
        let v = (0..1000)
            .map(|x| {
                if x % 2 == 0 {
                    None
                } else if x % 3 == 0 {
                    Some(true)
                } else {
                    Some(false)
                }
            })
            .collect::<Vec<Option<bool>>>();
        let array = helper_test_builder(v.clone());
        let res = v.iter().zip(array.iter()).all(|(a, b)| *a == b);
        assert_eq!(res, true);
    }
}
