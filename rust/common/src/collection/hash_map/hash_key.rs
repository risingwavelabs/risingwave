use crate::array::{Array, ArrayBuilderImpl, ArrayImpl, DataChunk};
use crate::error::Result;
use crate::types::{Datum, Scalar};
use crate::types::{IntervalUnit, ScalarRef};
use crate::util::hash_util::CRC32FastBuilder;
use rust_decimal::Decimal;
use std::convert::TryInto;
use std::default::Default;
use std::hash::{BuildHasher, Hash, Hasher};

/// Max element count in [`FixedSizeKeyWithHashCode`]
pub const MAX_FIXED_SIZE_KEY_ELEMENTS: usize = 8;

pub trait HashKeySerializer {
    type K: HashKey;
    fn from_hash_code(hash_code: u64) -> Self;
    fn append<'a, D: HashKeySerialize<'a>>(&mut self, data: Option<D>) -> Result<()>;
    fn into_hash_key(self) -> Self::K;
}

pub trait HashKeySerialize<'a>: ScalarRef<'a> + Default {
    const N: usize;
    // the type `S` should be [u8; N], to avoid using `feature(generic_const_exprs)` and `feature(associated_type_defaults)
    type S: AsRef<[u8]>;
    fn serialize(self) -> Self::S;
}

/// Trait for different kinds of hash keys.
///
/// Current comparison implementation treats `null == null`. This is consistent with postgresql's
/// group by implementation, but not join. In pg's join implementation, `null != null`, and the join
/// executor should take care of this.
pub trait HashKey: Hash + Eq + Sized + Send + Sync + 'static {
    type S: HashKeySerializer<K = Self>;

    fn build(column_idxes: &[usize], data_chunk: &DataChunk) -> Result<Vec<Self>> {
        let hash_codes = data_chunk.get_hash_values(column_idxes, CRC32FastBuilder)?;
        let mut serializers: Vec<Self::S> = hash_codes
            .into_iter()
            .map(Self::S::from_hash_code)
            .collect();

        for column_idx in column_idxes {
            data_chunk
                .column_at(*column_idx)?
                .array_ref()
                .serialize_to_hash_key(&mut serializers)?;
        }

        Ok(serializers
            .into_iter()
            .map(Self::S::into_hash_key)
            .collect())
    }

    fn deserialize_to_builders(self, builders: &mut Vec<ArrayBuilderImpl>) -> Result<()>;

    fn has_null(&self) -> bool;
}

/// Designed for hash keys with at most `N` serialized bytes.
///
/// See [`crate::executor::hash_map::calc_hash_key_kind`]
pub struct FixedSizeKey<const N: usize> {
    hash_code: u64,
    key: [u8; N],
    null_bitmap: u8,
}

/// Designed for hash keys which can't be represented by [`FixedSizeKey`].
///
/// See [`crate::executor::hash_map::calc_hash_key_kind`]
pub struct SerializedKey {
    key: Vec<Datum>,
    hash_code: u64,
    has_null: bool,
}

/// Fix clippy warning.
impl<const N: usize> PartialEq for FixedSizeKey<N> {
    fn eq(&self, other: &Self) -> bool {
        (self.key == other.key) && (self.null_bitmap == other.null_bitmap)
    }
}

impl<const N: usize> Eq for FixedSizeKey<N> {}

impl<const N: usize> Hash for FixedSizeKey<N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash_code)
    }
}

impl PartialEq for SerializedKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for SerializedKey {}

impl Hash for SerializedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash_code)
    }
}

/// A special hasher designed our hashmap, which just stores precomputed hash key.
///
/// We need this because we compute hash keys in vectorized fashion, and we store them in this
/// hasher.
#[derive(Default)]
pub struct PrecomputedHasher {
    hash_code: u64,
}

impl Hasher for PrecomputedHasher {
    fn finish(&self) -> u64 {
        self.hash_code
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hash_code = u64::from_ne_bytes(bytes.try_into().unwrap());
    }
}

#[derive(Default)]
pub struct PrecomputedBuildHasher;

impl BuildHasher for PrecomputedBuildHasher {
    type Hasher = PrecomputedHasher;

    fn build_hasher(&self) -> Self::Hasher {
        PrecomputedHasher::default()
    }
}

pub type Key16 = FixedSizeKey<2>;
pub type Key32 = FixedSizeKey<4>;
pub type Key64 = FixedSizeKey<8>;
pub type Key128 = FixedSizeKey<16>;
pub type Key256 = FixedSizeKey<32>;
pub type KeySerialized = SerializedKey;

impl HashKeySerialize<'_> for bool {
    const N: usize = 1;
    type S = [u8; Self::N];
    fn serialize(self) -> Self::S {
        if self {
            [1u8; 1]
        } else {
            [0u8; 1]
        }
    }
}

impl HashKeySerialize<'_> for i16 {
    const N: usize = 2;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }
}

impl HashKeySerialize<'_> for i32 {
    const N: usize = 4;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }
}

impl HashKeySerialize<'_> for i64 {
    const N: usize = 8;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }
}

impl HashKeySerialize<'_> for f32 {
    const N: usize = 4;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }
}

impl HashKeySerialize<'_> for f64 {
    const N: usize = 8;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        self.to_ne_bytes()
    }
}

impl HashKeySerialize<'_> for Decimal {
    const N: usize = 16;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        Decimal::serialize(&self.normalize())
    }
}

impl HashKeySerialize<'_> for IntervalUnit {
    const N: usize = 16;
    type S = [u8; Self::N];

    fn serialize(self) -> Self::S {
        let mut ret = [0; 16];
        ret[0..4].copy_from_slice(&self.get_months().to_ne_bytes());
        ret[4..8].copy_from_slice(&self.get_days().to_ne_bytes());
        ret[8..16].copy_from_slice(&self.get_ms().to_ne_bytes());

        ret
    }
}

impl<'a> HashKeySerialize<'a> for &'a str {
    type S = Vec<u8>;
    const N: usize = 0;

    /// This should never be called
    fn serialize(self) -> Self::S {
        panic!("Should not serialize str for hash!")
    }
}

pub struct FixedSizeKeySerializer<const N: usize> {
    buffer: [u8; N],
    null_bitmap: u8,
    null_bitmap_idx: usize,
    data_len: usize,
    hash_code: u64,
}

impl<const N: usize> FixedSizeKeySerializer<N> {
    fn left_size(&self) -> usize {
        N - self.data_len
    }
}

impl<const N: usize> HashKeySerializer for FixedSizeKeySerializer<N> {
    type K = FixedSizeKey<N>;

    fn from_hash_code(hash_code: u64) -> Self {
        Self {
            buffer: [0u8; N],
            null_bitmap: 0xFFu8,
            null_bitmap_idx: 0,
            data_len: 0,
            hash_code,
        }
    }

    fn append<'a, D: HashKeySerialize<'a>>(&mut self, data: Option<D>) -> Result<()> {
        ensure!(self.null_bitmap_idx < 8);
        let data = match data {
            Some(v) => {
                let mask = 1u8 << self.null_bitmap_idx;
                self.null_bitmap |= mask;
                v.serialize()
            }
            None => {
                let mask = !(1u8 << self.null_bitmap_idx);
                self.null_bitmap &= mask;
                D::default().serialize()
            }
        };

        self.null_bitmap_idx += 1;

        let ret = data.as_ref();
        ensure!(self.left_size() >= ret.len());
        self.buffer[self.data_len..(self.data_len + ret.len())].copy_from_slice(ret);
        self.data_len += ret.len();
        Ok(())
    }

    fn into_hash_key(self) -> Self::K {
        FixedSizeKey::<N> {
            hash_code: self.hash_code,
            key: self.buffer,
            null_bitmap: self.null_bitmap,
        }
    }
}

pub struct FixedSizeKeyDeserializer<const N: usize> {
    buffer: [u8; N],
    null_bitmap: u8,
    null_bitmap_idx: usize,
    cur_addr: usize,
}

impl<const N: usize> FixedSizeKeyDeserializer<N> {
    fn with_hash_key(key: FixedSizeKey<N>) -> Self {
        key.into_deserializer()
    }
}

pub struct SerializedKeySerializer {
    buffer: Vec<Datum>,
    hash_code: u64,
    has_null: bool,
}

impl HashKeySerializer for SerializedKeySerializer {
    type K = SerializedKey;

    fn from_hash_code(hash_code: u64) -> Self {
        Self {
            buffer: Vec::new(),
            hash_code,
            has_null: false,
        }
    }

    fn append<'a, D: HashKeySerialize<'a>>(&mut self, data: Option<D>) -> Result<()> {
        match data {
            Some(v) => {
                self.buffer
                    .push(Some(v.to_owned_scalar().to_scalar_value()));
            }
            None => {
                self.buffer.push(None);
                self.has_null = true;
            }
        }
        Ok(())
    }

    fn into_hash_key(self) -> SerializedKey {
        SerializedKey {
            key: self.buffer,
            hash_code: self.hash_code,
            has_null: self.has_null,
        }
    }
}

fn serialize_array_to_hash_key<'a, A, S>(array: &'a A, serializers: &mut Vec<S>) -> Result<()>
where
    A: Array,
    A::RefItem<'a>: HashKeySerialize<'a>,
    S: HashKeySerializer,
{
    for (item, serializer) in array.iter().zip(serializers.iter_mut()) {
        serializer.append(item)?;
    }
    Ok(())
}

impl ArrayImpl {
    fn serialize_to_hash_key<S: HashKeySerializer>(&self, serializers: &mut Vec<S>) -> Result<()> {
        macro_rules! impl_all_serialize_to_hash_key {
      ([$self:ident, $serializers: ident], $({ $variant_name:ident, $suffix_name:ident, $array:ty, $builder:ty } ),*) => {
        match $self {
          $( Self::$variant_name(inner) => serialize_array_to_hash_key(inner, $serializers), )*
        }
      };
    }
        for_all_variants! { impl_all_serialize_to_hash_key, self, serializers }
    }
}

impl<const N: usize> FixedSizeKey<N> {
    fn into_deserializer(self) -> FixedSizeKeyDeserializer<N> {
        FixedSizeKeyDeserializer::<N> {
            cur_addr: 0,
            null_bitmap_idx: 0,
            buffer: self.key,
            null_bitmap: self.null_bitmap,
        }
    }
}
impl<const N: usize> HashKey for FixedSizeKey<N> {
    type S = FixedSizeKeySerializer<N>;

    fn has_null(&self) -> bool {
        self.null_bitmap != 0xFF
    }

    fn deserialize_to_builders(self, _builders: &mut Vec<ArrayBuilderImpl>) -> Result<()> {
        {
            todo!()
        }
    }
}

impl HashKey for SerializedKey {
    type S = SerializedKeySerializer;

    fn has_null(&self) -> bool {
        self.has_null
    }

    fn deserialize_to_builders(self, builders: &mut Vec<ArrayBuilderImpl>) -> Result<()> {
        self.key
            .into_iter()
            .zip(builders)
            .try_for_each(|(datum, builder)| builder.append_datum(&datum))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array;
    use crate::array::column::Column;
    use crate::array::{
        ArrayRef, BoolArray, DataChunk, DecimalArray, F32Array, F64Array, I16Array, I32Array,
        I64Array, UTF8Array,
    };
    use crate::collection::hash_map::{
        HashKey, Key128, Key16, Key256, Key32, Key64, KeySerialized, PrecomputedBuildHasher,
    };
    use crate::test_utils::rand_array::seed_rand_array_ref;
    use crate::types::{
        BoolType, DataTypeKind, Datum, DecimalType, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, StringType,
    };
    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::str::FromStr;
    use std::sync::Arc;

    #[derive(Hash, PartialEq, Eq)]
    struct Row(Vec<Datum>);

    fn generate_random_data_chunk() -> DataChunk {
        let capacity = 128;
        let seed = 10244021u64;
        let columns = vec![
            Column::new(
                seed_rand_array_ref::<BoolArray>(capacity, seed),
                BoolType::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<I16Array>(capacity, seed),
                Int16Type::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<I32Array>(capacity, seed),
                Int32Type::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<I64Array>(capacity, seed),
                Int64Type::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<F32Array>(capacity, seed),
                Float32Type::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<F64Array>(capacity, seed),
                Float64Type::create(true),
            ),
            Column::new(
                seed_rand_array_ref::<DecimalArray>(capacity, seed),
                Arc::new(DecimalType::new(true, 20, 10).expect("Failed to create decimal type")),
            ),
            Column::new(
                seed_rand_array_ref::<UTF8Array>(capacity, seed),
                StringType::create(true, 20, DataTypeKind::Varchar),
            ),
        ];

        DataChunk::try_from(columns).expect("Failed to create data chunk")
    }

    fn do_test<K: HashKey, F>(column_indexes: Vec<usize>, data_gen: F)
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
                    .map(|col_idx| {
                        data.column_at(*col_idx)
                            .unwrap_or_else(|_| panic!("Column not found: {}", *col_idx))
                    })
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

    fn generate_decimal_test_data() -> DataChunk {
        let columns = vec![Column::new(
            Arc::new(
                array! { DecimalArray,
                  [Some(Decimal::from_str("1.2").unwrap()),
                   None,
                   Some(Decimal::from_str("1.200").unwrap()),
                   Some(Decimal::from_str("0.00").unwrap()),
                   Some(Decimal::from_str("0.0").unwrap())]
                }
                .into(),
            ) as ArrayRef,
            Arc::new(DecimalType::new(true, 20, 10).expect("Failed to create decimal type")),
        )];

        DataChunk::try_from(columns).expect("Failed to create data chunk")
    }

    #[test]
    fn test_decimal_hash_key_serialization() {
        do_test::<Key128, _>(vec![0], generate_decimal_test_data);
    }
}
