// Copyright 2025 RisingWave Labs
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

use super::HashKey;
use crate::dispatch_data_types;
use crate::hash::{
    self, HashKeySer, HeapNullBitmap, MAX_GROUP_KEYS_ON_STACK, NullBitmap, StackNullBitmap,
};
use crate::types::DataType;

/// An enum to help to dynamically dispatch [`HashKey`] template.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HashKeyKind {
    Key8,
    Key16,
    Key32,
    Key64,
    Key128,
    Key256,
    KeySerialized,
}

impl HashKeyKind {
    fn order_by_key_size() -> impl IntoIterator<Item = (HashKeyKind, usize)> {
        use HashKeyKind::*;
        [
            (Key8, 1),
            (Key16, 2),
            (Key32, 4),
            (Key64, 8),
            (Key128, 16),
            (Key256, 32),
        ]
    }
}

/// Number of bytes of one element in `HashKey` serialization of [`DataType`].
enum HashKeySize {
    /// For types with fixed size, e.g. int, float.
    Fixed(usize),
    /// For types with variable size, e.g. string.
    Variable,
}

/// A trait to help to dynamically dispatch [`HashKey`] template.
///
/// Suppose you want to build a trait object of type `T`, whose underlying implementation is `S<K:
/// HashKey>`, you can implement a `HashKeyDispatcher` with `Output=T`. Then you can use
/// `dispatch_by_kind` to build `T` directly without working with generic argument `K`.
pub trait HashKeyDispatcher: Sized {
    type Output;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output;

    /// The data types used to build the hash key.
    fn data_types(&self) -> &[DataType];

    /// Based on the number of group keys and total size of group keys,
    /// we decide:
    /// 1. What bitmap to use for representing null values in group keys.
    /// 2. What key type to store group keys in.
    fn dispatch(self) -> Self::Output {
        if self.data_types().len() <= MAX_GROUP_KEYS_ON_STACK {
            self.dispatch_by_key_size::<StackNullBitmap>()
        } else {
            self.dispatch_by_key_size::<HeapNullBitmap>()
        }
    }

    /// All data types will be stored in a single `HashKey`.
    /// We use `Key<N>` for fixed size keys,
    /// `KeySerialized` for variable length keys.
    fn dispatch_by_key_size<T: NullBitmap>(self) -> Self::Output {
        match calc_hash_key_kind(self.data_types()) {
            HashKeyKind::Key8 => self.dispatch_impl::<hash::Key8<T>>(),
            HashKeyKind::Key16 => self.dispatch_impl::<hash::Key16<T>>(),
            HashKeyKind::Key32 => self.dispatch_impl::<hash::Key32<T>>(),
            HashKeyKind::Key64 => self.dispatch_impl::<hash::Key64<T>>(),
            HashKeyKind::Key128 => self.dispatch_impl::<hash::Key128<T>>(),
            HashKeyKind::Key256 => self.dispatch_impl::<hash::Key256<T>>(),
            HashKeyKind::KeySerialized => self.dispatch_impl::<hash::KeySerialized<T>>(),
        }
    }
}

fn hash_key_size(data_type: &DataType) -> HashKeySize {
    let exact_size = dispatch_data_types!(data_type, [S = ScalarRef], { S::exact_size() });

    match exact_size {
        Some(size) => HashKeySize::Fixed(size),
        None => HashKeySize::Variable,
    }
}

const MAX_FIXED_SIZE_KEY_ELEMENTS: usize = 8;

/// Calculate what kind of hash key should be used given the key data types.
///
/// When any of following conditions is met, we choose [`crate::hash::SerializedKey`]:
/// 1. Has variable size column.
/// 2. Number of columns exceeds [`MAX_FIXED_SIZE_KEY_ELEMENTS`]
/// 3. Sizes of data types exceed `256` bytes.
/// 4. Any column's serialized format can't be used for equality check.
///
/// Otherwise we choose smallest [`crate::hash::FixedSizeKey`] whose size can hold all data types.
pub fn calc_hash_key_kind(data_types: &[DataType]) -> HashKeyKind {
    if data_types.len() > MAX_FIXED_SIZE_KEY_ELEMENTS {
        return HashKeyKind::KeySerialized;
    }

    let mut total_data_size: usize = 0;
    for data_type in data_types {
        match hash_key_size(data_type) {
            HashKeySize::Fixed(size) => {
                total_data_size += size;
            }
            HashKeySize::Variable => {
                return HashKeyKind::KeySerialized;
            }
        }
    }

    for (kind, max_size) in HashKeyKind::order_by_key_size() {
        if total_data_size <= max_size {
            return kind;
        }
    }

    HashKeyKind::KeySerialized
}

#[cfg(test)]
mod tests {

    use super::{HashKeyKind, calc_hash_key_kind};
    use crate::types::DataType;

    fn all_data_types() -> Vec<DataType> {
        vec![
            DataType::Boolean,   // 0
            DataType::Int16,     // 1
            DataType::Int32,     // 2
            DataType::Int64,     // 3
            DataType::Float32,   // 4
            DataType::Float64,   // 5
            DataType::Decimal,   // 6
            DataType::Varchar,   // 7
            DataType::Timestamp, // 8
        ]
    }

    fn compare_key_kinds(input_indices: &[usize], expected: HashKeyKind) {
        let all_types = all_data_types();

        let input_types = input_indices
            .iter()
            .map(|idx| all_types[*idx].clone())
            .collect::<Vec<DataType>>();

        let calculated_kind = calc_hash_key_kind(&input_types);
        assert_eq!(expected, calculated_kind);
    }

    #[test]
    fn test_calc_hash_key_kind() {
        compare_key_kinds(&[0], HashKeyKind::Key8);
        compare_key_kinds(&[1], HashKeyKind::Key16);
        compare_key_kinds(&[2], HashKeyKind::Key32);
        compare_key_kinds(&[3], HashKeyKind::Key64);
        compare_key_kinds(&[8], HashKeyKind::Key128);
        compare_key_kinds(&[3, 4], HashKeyKind::Key128);
        compare_key_kinds(&[3, 4, 6], HashKeyKind::Key256);
        compare_key_kinds(&[7], HashKeyKind::KeySerialized);
        compare_key_kinds(&[1, 7], HashKeyKind::KeySerialized);
    }
}
