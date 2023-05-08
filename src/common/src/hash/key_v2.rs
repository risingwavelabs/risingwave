use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use bytes::{Buf, BufMut};
use educe::Educe;
use itertools::Itertools;
use tinyvec::ArrayVec;

use super::{NullBitmap, XxHash64HashCode};
use crate::array::{ArrayResult, DataChunk};
use crate::estimate_size::EstimateSize;
use crate::row::OwnedRow;
use crate::types::{DataType, ScalarRef, ScalarRefImpl};
use crate::util::hash_util::XxHash64Builder;
use crate::util::iter_util::ZipEqFast;
use crate::util::memcmp_encoding;
use crate::util::sort_util::OrderType;
use crate::util::value_encoding::{deserialize_datum, serialize_datum_into};

pub trait Buffer: 'static {
    type BufMut<'a>: BufMut
    where
        Self: 'a;
    type Sealed: AsRef<[u8]> + EstimateSize + Clone + 'static;

    fn with_capacity(cap: impl FnOnce() -> usize) -> Self;

    fn buf_mut(&mut self) -> Self::BufMut<'_>;

    fn seal(self) -> Self::Sealed;
}

impl<const N: usize> Buffer for ArrayVec<[u8; N]> {
    type BufMut<'a> = &'a mut [u8]
    where
        Self: 'a;
    type Sealed = [u8; N];

    fn with_capacity(_cap: impl FnOnce() -> usize) -> Self {
        Self::new()
    }

    fn buf_mut(&mut self) -> Self::BufMut<'_> {
        let len = self.len();
        &mut self.as_mut_slice()[len..]
    }

    fn seal(self) -> Self::Sealed {
        self.into_inner()
    }
}

impl Buffer for Vec<u8> {
    type BufMut<'a> = &'a mut Vec<u8>
    where
        Self: 'a;
    type Sealed = Box<[u8]>;

    fn with_capacity(cap: impl FnOnce() -> usize) -> Self {
        Self::with_capacity(cap())
    }

    fn buf_mut(&mut self) -> Self::BufMut<'_> {
        self
    }

    fn seal(self) -> Self::Sealed {
        self.into_boxed_slice()
    }
}

pub trait HashKeySerde: for<'a> ScalarRef<'a> {
    fn serialize_into(self, buf: impl BufMut);

    fn deserialize(
        data_type: &DataType,
        buf: impl Buf,
    ) -> ArrayResult<<Self as ScalarRef<'_>>::ScalarType>;
}

pub trait ValueEncodingHashKeySerde: HashKeySerde {}

impl<T: ValueEncodingHashKeySerde> HashKeySerde for T {
    fn serialize_into(self, mut buf: impl BufMut) {
        let scalar: ScalarRefImpl<'_> = self.into();
        serialize_datum_into(Some(scalar), &mut buf);
    }

    fn deserialize(
        data_type: &DataType,
        buf: impl Buf,
    ) -> ArrayResult<<Self as ScalarRef<'_>>::ScalarType> {
        let scalar_impl = deserialize_datum(buf, data_type).expect("de fail").unwrap(); // TODO: error handling
        Ok(scalar_impl.try_into().unwrap())
    }
}

pub trait MemCmpHashKeySerde: HashKeySerde {}

impl<T: MemCmpHashKeySerde> HashKeySerde for T {
    fn serialize_into(self, buf: impl BufMut) {
        let scalar: ScalarRefImpl<'_> = self.into();
        let mut serializer = memcomparable::Serializer::new(buf);
        memcmp_encoding::serialize_datum(Some(scalar), OrderType::ascending(), &mut serializer);
    }

    fn deserialize(
        data_type: &DataType,
        buf: impl Buf,
    ) -> ArrayResult<<Self as ScalarRef<'_>>::ScalarType> {
        let mut deserializer = memcomparable::Deserializer::new(buf);
        let scalar_impl = memcmp_encoding::deserialize_datum(
            data_type,
            OrderType::ascending(),
            &mut deserializer,
        )
        .expect("de fail")
        .unwrap(); // TODO: error handling
        Ok(scalar_impl.try_into().unwrap())
    }
}

pub trait HashKey: EstimateSize + Clone + Debug + Hash + Eq + Sized + 'static {
    type NullBitmap: NullBitmap;

    fn build(column_indices: &[usize], data_chunk: &DataChunk) -> Vec<Self>;

    fn deserialize(&self, data_types: &[DataType]) -> ArrayResult<OwnedRow>;

    fn null_bitmap(&self) -> &Self::NullBitmap;
}

#[derive(Educe)]
#[educe(Clone)]
pub struct GenericHashKey<B: Buffer, N: NullBitmap> {
    hash_code: XxHash64HashCode,
    key: B::Sealed,
    null_bitmap: N,
}

impl<B: Buffer, N: NullBitmap> Hash for GenericHashKey<B, N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: note
        state.write_u64(self.hash_code.value());
    }
}

// TODO: educe
impl<S: Buffer, N: NullBitmap> PartialEq for GenericHashKey<S, N> {
    fn eq(&self, other: &Self) -> bool {
        self.hash_code == other.hash_code
            && self.key.as_ref() == other.key.as_ref()
            && self.null_bitmap == other.null_bitmap
    }
}
impl<S: Buffer, N: NullBitmap> Eq for GenericHashKey<S, N> {}

// TODO: educe
impl<B: Buffer, N: NullBitmap> std::fmt::Debug for GenericHashKey<B, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashKey")
            .field("key", &self.key.as_ref())
            .finish_non_exhaustive()
    }
}

impl<B: Buffer, N: NullBitmap> EstimateSize for GenericHashKey<B, N> {
    fn estimated_heap_size(&self) -> usize {
        self.key.estimated_heap_size() + self.null_bitmap.estimated_heap_size()
    }
}

impl<B: Buffer, N: NullBitmap> HashKey for GenericHashKey<B, N> {
    type NullBitmap = N;

    fn build(column_indices: &[usize], data_chunk: &DataChunk) -> Vec<Self> {
        let hash_codes = data_chunk.get_hash_values(column_indices, XxHash64Builder);

        let buffers = {
            let mut estimated_key_sizes = None;

            (0..data_chunk.capacity())
                .map(|i| {
                    B::with_capacity(|| {
                        let estimated_key_sizes = estimated_key_sizes.get_or_insert_with(|| {
                            data_chunk.compute_key_sizes_by_columns(column_indices)
                        });
                        estimated_key_sizes[i]
                    })
                })
                .collect_vec()
        };

        // TODO: serialize and null bitmap

        hash_codes
            .into_iter()
            .zip_eq_fast(buffers)
            .map(|(hash_code, buffer)| GenericHashKey {
                hash_code,
                key: buffer.seal(),
                null_bitmap: N::empty(),
            })
            .collect()
    }

    fn deserialize(&self, data_types: &[DataType]) -> ArrayResult<OwnedRow> {
        let _ = data_types;
        todo!()
    }

    fn null_bitmap(&self) -> &Self::NullBitmap {
        &self.null_bitmap
    }
}
