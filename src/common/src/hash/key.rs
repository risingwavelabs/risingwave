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

//! This module contains implementation for hash key serialization for
//! hash-agg, hash-join, and perhaps other hash-based operators.
//!
//! There may be multiple columns in one row being combined and encoded into
//! one single hash key.
//! For example, `SELECT sum(t.a) FROM t GROUP BY t.b, t.c`, the hash keys
//! are encoded from both `t.b` and `t.c`. If `t.b="abc"` and `t.c=1`, the hashkey may be
//! encoded in certain format of `("abc", 1)`.

use std::convert::TryInto;
use std::default::Default;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hasher};
use std::io::Read;
use std::marker::PhantomData;

use chrono::{Datelike, Timelike};
use educe::Educe;
use fixedbitset::FixedBitSet;
use smallbitset::Set64;
use static_assertions::const_assert_eq;

use crate::array::{ListRef, StructRef};
use crate::estimate_size::EstimateSize;
use crate::types::{
    Date, Decimal, Int256Ref, JsonbRef, ScalarRef, Serial, Time, Timestamp, F32, F64,
};
use crate::util::hash_util::{Crc32FastBuilder, XxHash64Builder};

/// This is determined by the stack based data structure we use,
/// `StackNullBitmap`, which can store 64 bits at most.
pub static MAX_GROUP_KEYS_ON_STACK: usize = 64;

/// Null bitmap on heap.
/// We use this for the **edge case** where group key sizes are larger than 64.
/// This is because group key null bits cannot fit into a u64 on the stack
/// if they exceed 64 bits.
/// NOTE(kwannoel): This is not really optimized as it is an edge case.
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq)]
pub struct HeapNullBitmap {
    inner: FixedBitSet,
}

impl HeapNullBitmap {
    fn with_capacity(n: usize) -> Self {
        HeapNullBitmap {
            inner: FixedBitSet::with_capacity(n),
        }
    }
}

/// Null Bitmap on stack.
/// This is specialized for the common case where group keys (<= 64).
#[repr(transparent)]
#[derive(Clone, Debug, PartialEq)]
pub struct StackNullBitmap {
    inner: Set64,
}

const_assert_eq!(
    std::mem::size_of::<StackNullBitmap>(),
    std::mem::size_of::<u64>()
);

const_assert_eq!(
    std::mem::size_of::<HeapNullBitmap>(),
    std::mem::size_of::<usize>() * 4,
);

/// We use a trait for `NullBitmap` so we can parameterize structs on it.
/// This is because `NullBitmap` is used often, and we want it to occupy
/// the minimal stack space.
///
/// ### Example
/// ```rust
/// use risingwave_common::hash::{NullBitmap, StackNullBitmap};
/// struct A<B: NullBitmap> {
///     null_bitmap: B,
/// }
/// ```
///
/// Then `A<StackNullBitmap>` occupies 64 bytes,
/// and in cases which require it,
/// `A<HeapNullBitmap>` will occupy 4 * usize bytes (on 64 bit arch that would be 256 bytes).
pub trait NullBitmap: EstimateSize + Clone + PartialEq + Debug + Send + Sync + 'static {
    fn empty() -> Self;

    fn is_empty(&self) -> bool;

    fn set_true(&mut self, idx: usize);

    fn contains(&self, x: usize) -> bool;

    fn is_subset(&self, other: &Self) -> bool;

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self;
}

impl NullBitmap for StackNullBitmap {
    fn empty() -> Self {
        StackNullBitmap {
            inner: Set64::empty(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn set_true(&mut self, idx: usize) {
        self.inner.add_inplace(idx);
    }

    fn contains(&self, x: usize) -> bool {
        self.inner.contains(x)
    }

    fn is_subset(&self, other: &Self) -> bool {
        other.inner.contains_all(self.inner)
    }

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self {
        value.into()
    }
}

impl NullBitmap for HeapNullBitmap {
    fn empty() -> Self {
        HeapNullBitmap {
            inner: FixedBitSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn set_true(&mut self, idx: usize) {
        self.inner.grow(idx + 1);
        self.inner.insert(idx)
    }

    fn contains(&self, x: usize) -> bool {
        self.inner.contains(x)
    }

    fn is_subset(&self, other: &Self) -> bool {
        self.inner.is_subset(&other.inner)
    }

    fn from_bool_vec<T: AsRef<[bool]> + IntoIterator<Item = bool>>(value: T) -> Self {
        value.into()
    }
}

impl EstimateSize for StackNullBitmap {
    fn estimated_heap_size(&self) -> usize {
        0
    }
}

impl EstimateSize for HeapNullBitmap {
    fn estimated_heap_size(&self) -> usize {
        self.inner.estimated_heap_size()
    }
}

impl<T: AsRef<[bool]> + IntoIterator<Item = bool>> From<T> for StackNullBitmap {
    fn from(value: T) -> Self {
        let mut bitmap = StackNullBitmap::empty();
        for (idx, is_true) in value.into_iter().enumerate() {
            if is_true {
                bitmap.set_true(idx);
            }
        }
        bitmap
    }
}

impl<T: AsRef<[bool]> + IntoIterator<Item = bool>> From<T> for HeapNullBitmap {
    fn from(value: T) -> Self {
        let mut bitmap = HeapNullBitmap::with_capacity(value.as_ref().len());
        for (idx, is_true) in value.into_iter().enumerate() {
            if is_true {
                bitmap.set_true(idx);
            }
        }
        bitmap
    }
}

/// A wrapper for u64 hash result. Generic over the hasher.
#[derive(Educe)]
#[educe(Default, Clone, Copy, Debug, PartialEq)]
pub struct HashCode<T: 'static + BuildHasher> {
    value: u64,
    #[educe(Debug(ignore))]
    _phantom: PhantomData<&'static T>,
}

impl<T: BuildHasher> From<u64> for HashCode<T> {
    fn from(hash_code: u64) -> Self {
        Self {
            value: hash_code,
            _phantom: PhantomData,
        }
    }
}

impl<T: BuildHasher> HashCode<T> {
    pub fn value(self) -> u64 {
        self.value
    }
}

/// Hash code from the `Crc32` hasher. Used for hash-shuffle exchange.
pub type Crc32HashCode = HashCode<Crc32FastBuilder>;
/// Hash code from the `XxHash64` hasher. Used for in-memory hash map cache.
pub type XxHash64HashCode = HashCode<XxHash64Builder>;

/// A special hasher designed for [`HashKey`], which stores a hash key from `HashKey::hash()` and
/// outputs it on `finish()`.
///
/// We need this because we compute hash keys in vectorized fashion, and we store them in this
/// hasher.
///
/// WARN: This should ONLY be used along with [`HashKey`].
#[derive(Default)]
pub struct PrecomputedHasher {
    hash_code: u64,
}

impl Hasher for PrecomputedHasher {
    fn finish(&self) -> u64 {
        self.hash_code
    }

    fn write_u64(&mut self, i: u64) {
        assert_eq!(self.hash_code, 0);
        self.hash_code = i;
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("must writes from HashKey with write_u64")
    }
}

#[derive(Default, Clone)]
pub struct PrecomputedBuildHasher;

impl BuildHasher for PrecomputedBuildHasher {
    type Hasher = PrecomputedHasher;

    fn build_hasher(&self) -> Self::Hasher {
        PrecomputedHasher::default()
    }
}

/// Trait for value types that can be serialized to or deserialized from hash keys.
///
/// Note that this trait is more like a marker suggesting that types that implement it can be
/// encoded into the hash key. The actual encoding/decoding method is not limited to
/// [`HashKeySerDe`]'s fixed-size implementation.
pub trait HashKeySerDe<'a>: ScalarRef<'a> {
    type S: AsRef<[u8]>;
    fn serialize(self) -> Self::S;
    fn deserialize<R: Read>(source: &mut R) -> Self;

    fn read_fixed_size_bytes<R: Read, const N: usize>(source: &mut R) -> [u8; N] {
        let mut buffer: [u8; N] = [0u8; N];
        source
            .read_exact(&mut buffer)
            .expect("Failed to read fixed size serialized key!");
        buffer
    }
}

impl HashKeySerDe<'_> for bool {
    type S = [u8; 1];

    fn serialize(self) -> Self::S {
        if self {
            [1u8; 1]
        } else {
            [0u8; 1]
        }
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 1>(source);
        value[0] == 1u8
    }
}

impl HashKeySerDe<'_> for i16 {
    type S = [u8; 2];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 2>(source);
        Self::from_ne_bytes(value)
    }
}

impl HashKeySerDe<'_> for i32 {
    type S = [u8; 4];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 4>(source);
        Self::from_ne_bytes(value)
    }
}

impl HashKeySerDe<'_> for i64 {
    type S = [u8; 8];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 8>(source);
        Self::from_ne_bytes(value)
    }
}

impl HashKeySerDe<'_> for F32 {
    type S = [u8; 4];

    fn serialize(self) -> Self::S {
        self.normalized().0.to_ne_bytes()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 4>(source);
        f32::from_ne_bytes(value).into()
    }
}

impl HashKeySerDe<'_> for F64 {
    type S = [u8; 8];

    fn serialize(self) -> Self::S {
        self.normalized().0.to_ne_bytes()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 8>(source);
        f64::from_ne_bytes(value).into()
    }
}

impl HashKeySerDe<'_> for Decimal {
    type S = [u8; 16];

    fn serialize(self) -> Self::S {
        Decimal::unordered_serialize(&self.normalize())
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 16>(source);
        Self::unordered_deserialize(value)
    }
}

impl<'a> HashKeySerDe<'a> for &'a str {
    type S = Vec<u8>;

    /// This should never be called
    fn serialize(self) -> Self::S {
        panic!("Should not serialize str for hash!")
    }

    /// This should never be called
    fn deserialize<R: Read>(_source: &mut R) -> Self {
        panic!("Should not serialize str for hash!")
    }
}

impl<'a> HashKeySerDe<'a> for Int256Ref<'a> {
    type S = [u8; 32];

    fn serialize(self) -> Self::S {
        unimplemented!("HashKeySerDe cannot be implemented for non-primitive types")
    }

    fn deserialize<R: Read>(_source: &mut R) -> Self {
        unimplemented!("HashKeySerDe cannot be implemented for non-primitive types")
    }
}

/// Same as str.
impl<'a> HashKeySerDe<'a> for &'a [u8] {
    type S = Vec<u8>;

    /// This should never be called
    fn serialize(self) -> Self::S {
        panic!("Should not serialize bytes for hash!")
    }

    /// This should never be called
    fn deserialize<R: Read>(_source: &mut R) -> Self {
        panic!("Should not serialize bytes for hash!")
    }
}

impl HashKeySerDe<'_> for Date {
    type S = [u8; 4];

    fn serialize(self) -> Self::S {
        let mut ret = [0; 4];
        ret[0..4].copy_from_slice(&self.0.num_days_from_ce().to_ne_bytes());

        ret
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 4>(source);
        let days = i32::from_ne_bytes(value[0..4].try_into().unwrap());
        Date::with_days(days).unwrap()
    }
}

impl HashKeySerDe<'_> for Timestamp {
    type S = [u8; 12];

    fn serialize(self) -> Self::S {
        let mut ret = [0; 12];
        ret[0..8].copy_from_slice(&self.0.timestamp().to_ne_bytes());
        ret[8..12].copy_from_slice(&self.0.timestamp_subsec_nanos().to_ne_bytes());

        ret
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 12>(source);
        let secs = i64::from_ne_bytes(value[0..8].try_into().unwrap());
        let nsecs = u32::from_ne_bytes(value[8..12].try_into().unwrap());
        Timestamp::with_secs_nsecs(secs, nsecs).unwrap()
    }
}

impl HashKeySerDe<'_> for Time {
    type S = [u8; 8];

    fn serialize(self) -> Self::S {
        let mut ret = [0; 8];
        ret[0..4].copy_from_slice(&self.0.num_seconds_from_midnight().to_ne_bytes());
        ret[4..8].copy_from_slice(&self.0.nanosecond().to_ne_bytes());

        ret
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 8>(source);
        let secs = u32::from_ne_bytes(value[0..4].try_into().unwrap());
        let nano = u32::from_ne_bytes(value[4..8].try_into().unwrap());
        Time::with_secs_nano(secs, nano).unwrap()
    }
}

impl<'a> HashKeySerDe<'a> for JsonbRef<'a> {
    type S = Vec<u8>;

    /// This should never be called
    fn serialize(self) -> Self::S {
        todo!()
    }

    /// This should never be called
    fn deserialize<R: Read>(_source: &mut R) -> Self {
        todo!()
    }
}

impl<'a> HashKeySerDe<'a> for Serial {
    type S = <i64 as HashKeySerDe<'a>>::S;

    fn serialize(self) -> Self::S {
        self.into_inner().serialize()
    }

    fn deserialize<R: Read>(source: &mut R) -> Self {
        i64::deserialize(source).into()
    }
}

impl<'a> HashKeySerDe<'a> for StructRef<'a> {
    type S = Vec<u8>;

    /// This should never be called
    fn serialize(self) -> Self::S {
        todo!()
    }

    /// This should never be called
    fn deserialize<R: Read>(_source: &mut R) -> Self {
        todo!()
    }
}

impl<'a> HashKeySerDe<'a> for ListRef<'a> {
    type S = Vec<u8>;

    /// This should never be called
    fn serialize(self) -> Self::S {
        todo!()
    }

    /// This should never be called
    fn deserialize<R: Read>(_source: &mut R) -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::str::FromStr;

    use itertools::Itertools;

    use super::*;
    use crate::array;
    use crate::array::column::Column;
    use crate::array::{
        ArrayBuilder, ArrayBuilderImpl, ArrayImpl, BoolArray, DataChunk, DataChunkTestExt,
        DateArray, DecimalArray, F32Array, F64Array, I16Array, I32Array, I32ArrayBuilder, I64Array,
        TimeArray, TimestampArray, Utf8Array,
    };
    use crate::hash::{
        HashKey, Key128, Key16, Key256, Key32, Key64, KeySerialized, PrecomputedBuildHasher,
    };
    use crate::test_utils::rand_array::seed_rand_array_ref;
    use crate::types::{DataType, Datum};

    #[derive(Hash, PartialEq, Eq)]
    struct Row(Vec<Datum>);

    fn generate_random_data_chunk() -> (DataChunk, Vec<DataType>) {
        let capacity = 128;
        let seed = 10244021u64;
        let columns = vec![
            Column::new(seed_rand_array_ref::<BoolArray>(capacity, seed, 0.5)),
            Column::new(seed_rand_array_ref::<I16Array>(capacity, seed + 1, 0.5)),
            Column::new(seed_rand_array_ref::<I32Array>(capacity, seed + 2, 0.5)),
            Column::new(seed_rand_array_ref::<I64Array>(capacity, seed + 3, 0.5)),
            Column::new(seed_rand_array_ref::<F32Array>(capacity, seed + 4, 0.5)),
            Column::new(seed_rand_array_ref::<F64Array>(capacity, seed + 5, 0.5)),
            Column::new(seed_rand_array_ref::<DecimalArray>(capacity, seed + 6, 0.5)),
            Column::new(seed_rand_array_ref::<Utf8Array>(capacity, seed + 7, 0.5)),
            Column::new(seed_rand_array_ref::<DateArray>(capacity, seed + 8, 0.5)),
            Column::new(seed_rand_array_ref::<TimeArray>(capacity, seed + 9, 0.5)),
            Column::new(seed_rand_array_ref::<TimestampArray>(
                capacity,
                seed + 10,
                0.5,
            )),
        ];
        let types = vec![
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Varchar,
            DataType::Date,
            DataType::Time,
            DataType::Timestamp,
        ];

        (DataChunk::new(columns, capacity), types)
    }

    fn do_test_serialize<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> DataChunk,
    {
        let data = data_gen();

        let mut actual_row_id_mapping = HashMap::<usize, Vec<usize>>::with_capacity(100);
        {
            let mut fast_hash_map =
                HashMap::<K, Vec<usize>, PrecomputedBuildHasher>::with_capacity_and_hasher(
                    100,
                    PrecomputedBuildHasher,
                );
            let keys =
                K::build(column_indexes.as_slice(), &data).expect("Failed to build hash keys");

            for (row_id, key) in keys.into_iter().enumerate() {
                let row_ids = fast_hash_map.entry(key).or_default();
                row_ids.push(row_id);
            }

            for row_ids in fast_hash_map.values() {
                for row_id in row_ids {
                    actual_row_id_mapping.insert(*row_id, row_ids.clone());
                }
            }
        }

        let mut expected_row_id_mapping = HashMap::<usize, Vec<usize>>::with_capacity(100);
        {
            let mut normal_hash_map: HashMap<Row, Vec<usize>> = HashMap::with_capacity(100);
            for row_idx in 0..data.capacity() {
                let row = column_indexes
                    .iter()
                    .map(|col_idx| data.column_at(*col_idx))
                    .map(|col| col.array_ref().datum_at(row_idx))
                    .collect::<Vec<Datum>>();

                normal_hash_map.entry(Row(row)).or_default().push(row_idx);
            }

            for row_ids in normal_hash_map.values() {
                for row_id in row_ids {
                    expected_row_id_mapping.insert(*row_id, row_ids.clone());
                }
            }
        }

        assert_eq!(expected_row_id_mapping, actual_row_id_mapping);
    }

    fn do_test_deserialize<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> (DataChunk, Vec<DataType>),
    {
        let (data, types) = data_gen();
        let keys = K::build(column_indexes.as_slice(), &data).expect("Failed to build hash keys");

        let mut array_builders = column_indexes
            .iter()
            .map(|idx| data.columns()[*idx].array_ref().create_builder(1024))
            .collect::<Vec<ArrayBuilderImpl>>();
        let key_types: Vec<_> = column_indexes
            .iter()
            .map(|idx| types[*idx].clone())
            .collect();

        keys.into_iter()
            .try_for_each(|k| k.deserialize_to_builders(&mut array_builders[..], &key_types))
            .expect("Failed to deserialize!");

        let result_arrays = array_builders
            .into_iter()
            .map(|array_builder| array_builder.finish())
            .collect::<Vec<ArrayImpl>>();

        for (ret_idx, col_idx) in column_indexes.iter().enumerate() {
            assert_eq!(
                data.columns()[*col_idx].array_ref(),
                &result_arrays[ret_idx]
            );
        }
    }

    fn do_test<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
    where
        F: FnOnce() -> (DataChunk, Vec<DataType>),
    {
        let (data, types) = data_gen();

        let data1 = data.clone();
        do_test_serialize::<K, _>(column_indexes.clone(), move || data1);
        do_test_deserialize::<K, _>(column_indexes, move || (data, types));
    }

    #[test]
    fn test_two_bytes_hash_key() {
        do_test::<Key16, _>(vec![1], generate_random_data_chunk);
    }

    #[test]
    fn test_four_bytes_hash_key() {
        do_test::<Key32, _>(vec![0, 1], generate_random_data_chunk);
        do_test::<Key32, _>(vec![2], generate_random_data_chunk);
        do_test::<Key32, _>(vec![4], generate_random_data_chunk);
    }

    #[test]
    fn test_eight_bytes_hash_key() {
        do_test::<Key64, _>(vec![1, 2], generate_random_data_chunk);
        do_test::<Key64, _>(vec![0, 1, 2], generate_random_data_chunk);
        do_test::<Key64, _>(vec![3], generate_random_data_chunk);
        do_test::<Key64, _>(vec![5], generate_random_data_chunk);
    }

    #[test]
    fn test_128_bits_hash_key() {
        do_test::<Key128, _>(vec![3, 5], generate_random_data_chunk);
        do_test::<Key128, _>(vec![6], generate_random_data_chunk);
    }

    #[test]
    fn test_256_bits_hash_key() {
        do_test::<Key256, _>(vec![3, 5, 6], generate_random_data_chunk);
        do_test::<Key256, _>(vec![3, 6], generate_random_data_chunk);
    }

    #[test]
    fn test_var_length_hash_key() {
        do_test::<KeySerialized, _>(vec![0, 7], generate_random_data_chunk);
    }

    fn generate_decimal_test_data() -> (DataChunk, Vec<DataType>) {
        let columns = vec![array! { DecimalArray, [
            Some(Decimal::from_str("1.2").unwrap()),
            None,
            Some(Decimal::from_str("1.200").unwrap()),
            Some(Decimal::from_str("0.00").unwrap()),
            Some(Decimal::from_str("0.0").unwrap())
        ]}
        .into()];
        let types = vec![DataType::Decimal];

        (DataChunk::new(columns, 5), types)
    }

    #[test]
    fn test_decimal_hash_key_serialization() {
        do_test::<Key128, _>(vec![0], generate_decimal_test_data);
    }

    // Simple test to ensure a row <None, Some(2)> will be serialized and restored
    // losslessly.
    #[test]
    fn test_simple_hash_key_nullable_serde() {
        let keys = Key64::build(
            &[0, 1],
            &DataChunk::from_pretty(
                "i i
                 1 .
                 . 2",
            ),
        )
        .unwrap();

        let mut array_builders = [0, 1]
            .iter()
            .map(|_| ArrayBuilderImpl::Int32(I32ArrayBuilder::new(2)))
            .collect::<Vec<_>>();

        keys.into_iter().for_each(|k: Key64| {
            k.deserialize_to_builders(&mut array_builders[..], &[DataType::Int32, DataType::Int32])
                .unwrap()
        });

        let array = array_builders.pop().unwrap().finish();
        let i32_vec = array
            .iter()
            .map(|opt| opt.map(|s| s.into_int32()))
            .collect_vec();
        assert_eq!(i32_vec, vec![None, Some(2)]);
    }
}
