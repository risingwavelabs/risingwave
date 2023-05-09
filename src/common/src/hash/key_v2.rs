// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use bytes::BufMut;
use educe::Educe;
use itertools::Itertools;
use tinyvec::ArrayVec;

use super::{HeapNullBitmap, NullBitmap, XxHash64HashCode};
use crate::array::{ArrayBuilderImpl, ArrayResult, DataChunk};
use crate::estimate_size::EstimateSize;
use crate::for_all_type_pairs;
use crate::hash::{HashKeyDe, HashKeySer};
use crate::row::OwnedRow;
use crate::types::{DataType, Datum, ScalarImpl, ScalarRefImpl, ToDatumRef};
use crate::util::hash_util::XxHash64Builder;
use crate::util::iter_util::ZipEqFast;

pub trait KeyStorage: 'static {
    type Key: AsRef<[u8]> + EstimateSize + Clone + Send + Sync + 'static;
    type Buffer: Buffer<Self::Key>;
}

pub trait Buffer<K>: BufMut + 'static {
    fn with_capacity(cap: impl FnOnce() -> usize) -> Self;

    fn seal(self) -> K;
}

pub struct StackStorage<const N: usize>;

impl<const N: usize> KeyStorage for StackStorage<N> {
    type Buffer = StackBuffer<N>;
    type Key = [u8; N];
}

pub struct StackBuffer<const N: usize>(ArrayVec<[u8; N]>);

unsafe impl<const N: usize> BufMut for StackBuffer<N> {
    fn remaining_mut(&self) -> usize {
        self.0.grab_spare_slice().len()
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.0.set_len(self.0.len() + cnt);
    }

    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        unsafe { &mut *(self.0.grab_spare_slice_mut().chunk_mut() as *mut _) }
    }
}

impl<const N: usize> Buffer<[u8; N]> for StackBuffer<N> {
    fn with_capacity(_cap: impl FnOnce() -> usize) -> Self {
        Self(ArrayVec::new())
    }

    fn seal(self) -> [u8; N] {
        self.0.into_inner()
    }
}

pub struct HeapStorage;

impl KeyStorage for HeapStorage {
    type Buffer = Vec<u8>;
    type Key = Box<[u8]>;
}

impl Buffer<Box<[u8]>> for Vec<u8> {
    fn with_capacity(cap: impl FnOnce() -> usize) -> Self {
        Self::with_capacity(cap())
    }

    fn seal(self) -> Box<[u8]> {
        self.into_boxed_slice()
    }
}

struct Serializer<S: KeyStorage, N: NullBitmap> {
    buffer: S::Buffer,
    null_bitmap: N,
    idx: usize,
    hash_code: XxHash64HashCode,
}

impl<S: KeyStorage, N: NullBitmap> Serializer<S, N> {
    fn new(buffer: S::Buffer, hash_code: XxHash64HashCode) -> Self {
        Self {
            buffer,
            null_bitmap: N::empty(),
            idx: 0,
            hash_code,
        }
    }

    fn serialize(&mut self, datum: impl ToDatumRef) {
        match datum.to_datum_ref() {
            Some(scalar) => {
                dispatch_all_variants!(scalar, ScalarRefImpl, scalar, {
                    HashKeySer::serialize_into(scalar, &mut self.buffer);
                })
            }
            None => self.null_bitmap.set_true(self.idx),
        }
        self.idx += 1;
    }

    fn finish(self) -> GenericHashKey<S, N> {
        GenericHashKey {
            hash_code: self.hash_code,
            key: self.buffer.seal(),
            null_bitmap: self.null_bitmap,
        }
    }
}

struct Deserializer<'a, S: KeyStorage, N: NullBitmap> {
    key: &'a [u8],
    null_bitmap: &'a N,
    idx: usize,
    _phantom: PhantomData<&'a S::Key>,
}

impl<'a, S: KeyStorage, N: NullBitmap> Deserializer<'a, S, N> {
    fn new(key: &'a S::Key, null_bitmap: &'a N) -> Self {
        Self {
            key: key.as_ref(),
            null_bitmap,
            idx: 0,
            _phantom: PhantomData,
        }
    }

    fn deserialize(&mut self, data_type: &DataType) -> ArrayResult<Datum> {
        let datum = if !self.null_bitmap.contains(self.idx) {
            macro_rules! deserialize {
                ($( { $DataType:ident, $PhysicalType:ident }),*) => {
                    match data_type {
                        $(
                            DataType::$DataType { .. } => ScalarImpl::$PhysicalType(
                                HashKeyDe::deserialize(data_type, &mut self.key)
                            ),
                        )*
                    }
                }
            }

            Some(for_all_type_pairs! { deserialize })
        } else {
            None
        };

        self.idx += 1;
        Ok(datum)
    }
}

/// Trait for different kinds of hash keys.
///
/// Current comparison implementation treats `null == null`. This is consistent with postgresql's
/// group by implementation, but not join. In pg's join implementation, `null != null`, and the join
/// executor should take care of this.
pub trait HashKey:
    EstimateSize + Clone + Debug + Hash + Eq + Sized + Send + Sync + 'static
{
    // TODO: rename to `NullBitmap`
    type Bitmap: NullBitmap;

    // TODO: remove result and rename to `build_many`
    fn build(column_indices: &[usize], data_chunk: &DataChunk) -> ArrayResult<Vec<Self>>;

    fn deserialize(&self, data_types: &[DataType]) -> ArrayResult<OwnedRow>;

    fn deserialize_to_builders(
        &self,
        array_builders: &mut [ArrayBuilderImpl],
        data_types: &[DataType],
    ) -> ArrayResult<()>;

    fn null_bitmap(&self) -> &Self::Bitmap;
}

#[derive(Educe)]
#[educe(Clone)]
pub struct GenericHashKey<S: KeyStorage, N: NullBitmap> {
    hash_code: XxHash64HashCode,
    key: S::Key,
    null_bitmap: N,
}

impl<S: KeyStorage, N: NullBitmap> Hash for GenericHashKey<S, N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: note
        state.write_u64(self.hash_code.value());
    }
}

// TODO: educe
impl<S: KeyStorage, N: NullBitmap> PartialEq for GenericHashKey<S, N> {
    fn eq(&self, other: &Self) -> bool {
        self.hash_code == other.hash_code
            && self.key.as_ref() == other.key.as_ref()
            && self.null_bitmap == other.null_bitmap
    }
}
impl<S: KeyStorage, N: NullBitmap> Eq for GenericHashKey<S, N> {}

// TODO: educe
impl<S: KeyStorage, N: NullBitmap> std::fmt::Debug for GenericHashKey<S, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashKey")
            .field("key", &self.key.as_ref())
            .finish_non_exhaustive()
    }
}

impl<S: KeyStorage, N: NullBitmap> EstimateSize for GenericHashKey<S, N> {
    fn estimated_heap_size(&self) -> usize {
        self.key.estimated_heap_size() + self.null_bitmap.estimated_heap_size()
    }
}

impl<S: KeyStorage, N: NullBitmap> HashKey for GenericHashKey<S, N> {
    type Bitmap = N;

    fn build(column_indices: &[usize], data_chunk: &DataChunk) -> ArrayResult<Vec<Self>> {
        let hash_codes = data_chunk.get_hash_values(column_indices, XxHash64Builder);

        let mut serializers = {
            let mut estimated_key_sizes = None;

            let buffers = (0..data_chunk.capacity()).map(|i| {
                S::Buffer::with_capacity(|| {
                    let estimated_key_sizes = estimated_key_sizes.get_or_insert_with(|| {
                        data_chunk.compute_key_sizes_by_columns(column_indices)
                    });
                    estimated_key_sizes[i]
                })
            });

            hash_codes
                .into_iter()
                .zip_eq_fast(buffers)
                .map(|(hash_code, buffer)| Serializer::new(buffer, hash_code))
                .collect_vec()
        };

        for &i in column_indices {
            let array = data_chunk.column_at(i).array_ref();

            for (scalar, serializer) in array.iter().zip_eq_fast(&mut serializers) {
                serializer.serialize(scalar);
            }
        }

        let hash_keys = serializers.into_iter().map(|s| s.finish()).collect();
        Ok(hash_keys)
    }

    fn deserialize(&self, data_types: &[DataType]) -> ArrayResult<OwnedRow> {
        let mut deserializer = Deserializer::<S, N>::new(&self.key, &self.null_bitmap);
        let mut row = Vec::with_capacity(data_types.len());

        for data_type in data_types {
            let datum = deserializer.deserialize(data_type)?;
            row.push(datum);
        }

        Ok(OwnedRow::new(row))
    }

    fn deserialize_to_builders(
        &self,
        array_builders: &mut [ArrayBuilderImpl],
        data_types: &[DataType],
    ) -> ArrayResult<()> {
        let mut deserializer = Deserializer::<S, N>::new(&self.key, &self.null_bitmap);

        for (data_type, array_builder) in data_types.iter().zip_eq_fast(array_builders.iter_mut()) {
            let datum = deserializer.deserialize(data_type)?;
            array_builder.append_datum(datum);
        }

        Ok(())
    }

    fn null_bitmap(&self) -> &Self::Bitmap {
        &self.null_bitmap
    }
}

pub type FixedSizeKey<const N: usize, B> = GenericHashKey<StackStorage<N>, B>;
pub type Key8<B = HeapNullBitmap> = FixedSizeKey<1, B>;
pub type Key16<B = HeapNullBitmap> = FixedSizeKey<2, B>;
pub type Key32<B = HeapNullBitmap> = FixedSizeKey<4, B>;
pub type Key64<B = HeapNullBitmap> = FixedSizeKey<8, B>;
pub type Key128<B = HeapNullBitmap> = FixedSizeKey<16, B>;
pub type Key256<B = HeapNullBitmap> = FixedSizeKey<32, B>;
pub type KeySerialized<B = HeapNullBitmap> = SerializedKey<B>;

pub type SerializedKey<B = HeapNullBitmap> = GenericHashKey<HeapStorage, B>;
