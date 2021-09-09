use super::{Array, ArrayBuilder, ArrayIterator};
use crate::buffer::Bitmap;
use crate::error::Result;

use risingwave_proto::data::Buffer;

/// `UTF8Array` is a collection of Rust UTF8 `String`s.
#[derive(Debug)]
pub struct UTF8Array {
    offset: Vec<usize>,
    bitmap: Bitmap,
    data: Vec<u8>,
}

impl Array for UTF8Array {
    type RefItem<'a> = &'a str;
    type OwnedItem = String;
    type Builder = UTF8ArrayBuilder;
    type Iter<'a> = ArrayIterator<'a, Self>;

    fn value_at(&self, idx: usize) -> Option<&str> {
        if !self.is_null(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            Some(unsafe { std::str::from_utf8_unchecked(data_slice) })
        } else {
            None
        }
    }

    fn len(&self) -> usize {
        self.offset.len() - 1
    }

    fn iter(&self) -> ArrayIterator<'_, Self> {
        ArrayIterator::new(self)
    }

    fn to_protobuf(&self) -> crate::error::Result<Vec<Buffer>> {
        todo!()
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }
}

impl UTF8Array {
    pub fn from_slice(data: &[Option<&str>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

/// `UTF8ArrayBuilder` use `&str` to build an `UTF8Array`.
pub struct UTF8ArrayBuilder {
    offset: Vec<usize>,
    bitmap: Vec<bool>,
    data: Vec<u8>,
}

impl ArrayBuilder for UTF8ArrayBuilder {
    type ArrayType = UTF8Array;

    fn new(capacity: usize) -> Result<Self> {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Ok(Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: Vec::with_capacity(capacity),
        })
    }

    fn append<'a>(&'a mut self, value: Option<&'a str>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.push(true);
                self.data.extend_from_slice(x.as_bytes());
                self.offset.push(self.data.len())
            }
            None => {
                self.bitmap.push(false);
                self.offset.push(self.data.len())
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &UTF8Array) -> Result<()> {
        self.bitmap.extend(other.bitmap.iter());
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
        Ok(())
    }

    fn finish(self) -> Result<UTF8Array> {
        Ok(UTF8Array {
            bitmap: Bitmap::from_vec(self.bitmap)?,
            data: self.data,
            offset: self.offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_utf8_builder() {
        let mut builder = UTF8ArrayBuilder::new(0).unwrap();
        for i in 0..100 {
            if i % 2 == 0 {
                builder.append(Some(&format!("{}", i))).unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        builder.finish().unwrap();
    }
}
