use std::hash::{Hash, Hasher};
use std::iter;
use std::mem::size_of;

use risingwave_pb::data::buffer::CompressionType;
use risingwave_pb::data::Buffer as ProstBuffer;

use super::{Array, ArrayBuilder, ArrayIterator, NULL_VAL_FOR_HASH};
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::error::Result;

/// `Utf8Array` is a collection of Rust Utf8 `String`s.
#[derive(Debug)]
pub struct Utf8Array {
    offset: Vec<usize>,
    bitmap: Bitmap,
    data: Vec<u8>,
}

impl Array for Utf8Array {
    type RefItem<'a> = &'a str;
    type OwnedItem = String;
    type Builder = Utf8ArrayBuilder;
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

    fn to_protobuf(&self) -> Result<Vec<ProstBuffer>> {
        let offset_buffer = self
            .offset
            .iter()
            // length of offset is n + 1 while the length
            // of null_bitmap is n, chain iterator of null_bitmap
            // with one single true here to push the end of offset
            // to offset_buffer
            .zip(self.null_bitmap().iter().chain(iter::once(true)))
            .fold(
                Vec::<u8>::with_capacity(self.offset.len() * size_of::<usize>()),
                |mut buffer, (offset, not_null)| {
                    // TODO: force convert usize to u64, frontend will treat this offset buffer as
                    // u64
                    if not_null {
                        let offset = *offset as u64;
                        buffer.extend_from_slice(&offset.to_be_bytes());
                    }
                    buffer
                },
            );

        let data_buffer = self.data.clone();

        Ok(vec![offset_buffer, data_buffer]
            .into_iter()
            .map(|buffer| ProstBuffer {
                compression: CompressionType::None as i32,
                body: buffer,
            })
            .collect::<Vec<ProstBuffer>>())
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    #[inline(always)]
    fn hash_at<H: Hasher>(&self, idx: usize, state: &mut H) {
        if !self.is_null(idx) {
            let data_slice = &self.data[self.offset[idx]..self.offset[idx + 1]];
            state.write(data_slice);
        } else {
            NULL_VAL_FOR_HASH.hash(state);
        }
    }
}

impl Utf8Array {
    pub fn from_slice(data: &[Option<&str>]) -> Result<Self> {
        let mut builder = <Self as Array>::Builder::new(data.len())?;
        for i in data {
            builder.append(*i)?;
        }
        builder.finish()
    }
}

/// `Utf8ArrayBuilder` use `&str` to build an `Utf8Array`.
#[derive(Debug)]
pub struct Utf8ArrayBuilder {
    offset: Vec<usize>,
    bitmap: BitmapBuilder,
    data: Vec<u8>,
}

impl ArrayBuilder for Utf8ArrayBuilder {
    type ArrayType = Utf8Array;

    fn new(capacity: usize) -> Result<Self> {
        let mut offset = Vec::with_capacity(capacity + 1);
        offset.push(0);
        Ok(Self {
            offset,
            data: Vec::with_capacity(capacity),
            bitmap: BitmapBuilder::with_capacity(capacity),
        })
    }

    fn append<'a>(&'a mut self, value: Option<&'a str>) -> Result<()> {
        match value {
            Some(x) => {
                self.bitmap.append(true);
                self.data.extend_from_slice(x.as_bytes());
                self.offset.push(self.data.len())
            }
            None => {
                self.bitmap.append(false);
                self.offset.push(self.data.len())
            }
        }
        Ok(())
    }

    fn append_array(&mut self, other: &Utf8Array) -> Result<()> {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
        let start = *self.offset.last().unwrap();
        for other_offset in &other.offset[1..] {
            self.offset.push(*other_offset + start);
        }
        Ok(())
    }

    fn finish(mut self) -> Result<Utf8Array> {
        Ok(Utf8Array {
            bitmap: (self.bitmap).finish(),
            data: self.data,
            offset: self.offset,
        })
    }
}

impl Utf8ArrayBuilder {
    pub fn writer(self) -> BytesWriter {
        BytesWriter { builder: self }
    }

    /// `append_partial` will add a partial dirty data of the new record.
    /// The partial data will keep untracked until `finish_partial` was called.
    unsafe fn append_partial(&mut self, x: &str) -> Result<()> {
        self.data.extend_from_slice(x.as_bytes());
        Ok(())
    }

    /// `finish_partial` will create a new record based on the current dirty data.
    /// `finish_partial` was safe even if we don't call `append_partial`, which
    /// is equivalent to appending an empty string.
    fn finish_partial(&mut self) -> Result<()> {
        self.offset.push(self.data.len());
        self.bitmap.append(true);
        Ok(())
    }
}

/// `BytesWriter` has the ownership of the right to append only one record.
pub struct BytesWriter {
    builder: Utf8ArrayBuilder,
}

impl BytesWriter {
    /// `write_ref` will consume `BytesWriter` and pass the ownership
    /// of `builder` to `BytesGuard`.
    pub fn write_ref(mut self, value: &str) -> Result<BytesGuard> {
        self.builder.append(Some(value))?;
        Ok(BytesGuard {
            builder: self.builder,
        })
    }

    /// `begin` will create a `PartialBytesWriter`, which allow multiple
    /// appendings to create a new record.
    pub fn begin(self) -> PartialBytesWriter {
        PartialBytesWriter {
            builder: self.builder,
        }
    }
}

pub struct PartialBytesWriter {
    builder: Utf8ArrayBuilder,
}

impl PartialBytesWriter {
    /// `write_ref` will append partial dirty data to `builder`.
    /// `PartialBytesWriter::write_ref` is different from `BytesWriter::write_ref`
    /// in that it allows us to call it multiple times.
    pub fn write_ref(&mut self, value: &str) -> Result<()> {
        // SAFETY: The dirty `builder` is owned by `PartialBytesWriter`.
        // We can't access it until `finish` was called.
        unsafe { self.builder.append_partial(value) }
    }

    /// `finish` will be called while the entire record is written.
    /// Exactly one new record was appended and the `builder` can be safely used,
    /// so we move the builder to `BytesGuard`.
    pub fn finish(mut self) -> Result<BytesGuard> {
        self.builder.finish_partial()?;
        Ok(BytesGuard {
            builder: self.builder,
        })
    }
}

/// `BytesGuard` guarded that exactly one record was appendded.
/// `BytesGuard` will be produced iff the `BytesWriter` was consumed.
pub struct BytesGuard {
    builder: Utf8ArrayBuilder,
}

impl BytesGuard {
    pub fn into_inner(self) -> Utf8ArrayBuilder {
        self.builder
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::error::Result;

    #[test]
    fn test_utf8_builder() {
        let mut builder = Utf8ArrayBuilder::new(0).unwrap();
        for i in 0..100 {
            if i % 2 == 0 {
                builder.append(Some(&format!("{}", i))).unwrap();
            } else {
                builder.append(None).unwrap();
            }
        }
        builder.finish().unwrap();
    }

    #[test]
    fn test_utf8_partial_writer() -> Result<()> {
        let builder = Utf8ArrayBuilder::new(0)?;
        let writer = builder.writer();
        let mut partial_writer = writer.begin();
        for _ in 0..2 {
            partial_writer.write_ref("ran")?;
        }
        let guard = partial_writer.finish()?;
        let builder = guard.into_inner();
        let array = builder.finish()?;
        assert_eq!(array.len(), 1);
        assert_eq!(array.value_at(0), Some("ranran"));

        Ok(())
    }

    #[test]
    fn test_utf8_array() {
        let input = vec![
            Some("1"),
            Some("22"),
            None,
            Some("4444"),
            None,
            Some("666666"),
        ];

        let result_array = Utf8Array::from_slice(&input);

        assert!(result_array.is_ok());
        let array = result_array.unwrap();

        assert_eq!(array.len(), input.len());

        assert_eq!(
            array.data.len(),
            input.iter().map(|s| s.unwrap_or("").len()).sum::<usize>()
        );

        assert_eq!(input, array.iter().collect_vec());
    }

    #[test]
    fn test_utf8_array_to_protobuf() {
        let input = vec![
            Some("1"),
            Some("22"),
            None,
            Some("4444"),
            None,
            Some("666666"),
        ];

        let result_array = Utf8Array::from_slice(&input);

        assert!(result_array.is_ok());
        let array = result_array.unwrap();
        let result_buffers = array.to_protobuf();
        assert!(result_buffers.is_ok());
        let buffers = result_buffers.unwrap();
        assert!(buffers.len() >= 2);
    }

    #[test]
    fn test_utf8_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

        const ARR_NUM: usize = 3;
        const ARR_LEN: usize = 90;
        let vecs: [Vec<Option<&str>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some("1"),
                    1 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some("1"),
                    1 => Some("abc"),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Some("1"),
                    1 => Some("abc"),
                    2 => None,
                    3 => Some("ABCDEF"),
                    4 => Some("666666"),
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| Utf8Array::from_slice(v).unwrap())
            .collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter().zip(&mut states).for_each(|(x, state)| match x {
                Some(inner) => inner.hash(state),
                None => NULL_VAL_FOR_HASH.hash(state),
            })
        });
        let hashes = hash_finish(&mut states);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
