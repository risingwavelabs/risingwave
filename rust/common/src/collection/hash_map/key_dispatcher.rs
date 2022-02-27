use super::HashKey;
use crate::collection::hash_map::{self, MAX_FIXED_SIZE_KEY_ELEMENTS};
use crate::types::{DataSize, DataType};

/// An enum to help to dynamically dispatch [`HashKey`] template.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum HashKeyKind {
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
            (Key16, 2),
            (Key32, 4),
            (Key64, 8),
            (Key128, 16),
            (Key256, 32),
        ]
    }
}

pub trait HashKeyDispatcher {
    type Input;
    type Output;

    fn dispatch<K: HashKey>(input: Self::Input) -> Self::Output;

    fn dispatch_by_kind(kind: HashKeyKind, input: Self::Input) -> Self::Output {
        match kind {
            HashKeyKind::Key16 => Self::dispatch::<hash_map::Key16>(input),
            HashKeyKind::Key32 => Self::dispatch::<hash_map::Key32>(input),
            HashKeyKind::Key64 => Self::dispatch::<hash_map::Key64>(input),
            HashKeyKind::Key128 => Self::dispatch::<hash_map::Key128>(input),
            HashKeyKind::Key256 => Self::dispatch::<hash_map::Key256>(input),
            HashKeyKind::KeySerialized => Self::dispatch::<hash_map::KeySerialized>(input),
        }
    }
}

/// Calculate what kind of hash key should be used given the key data types.
///
/// When any of following conditions is met, we choose [`SerializedKey`]:
/// 1. Has variable size column.
/// 2. Number of columns exceeds [`MAX_FIXED_SIZE_KEY_ELEMENTS`]
/// 3. Sizes of data types exceed `256` bytes.
/// 4. Any column's serialized format can't be used for equality check.
///
/// Otherwise we choose smallest [`FixedSizeKey`] whose size can hold all data types.
pub fn calc_hash_key_kind(data_types: &[DataType]) -> HashKeyKind {
    if data_types.len() > MAX_FIXED_SIZE_KEY_ELEMENTS {
        return HashKeyKind::KeySerialized;
    }

    let mut total_data_size: usize = 0;
    for data_type in data_types {
        match data_type.data_size() {
            DataSize::Fixed(size) => {
                total_data_size += size;
            }
            DataSize::Variable => {
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

    use crate::collection::hash_map::{calc_hash_key_kind, HashKeyKind};
    use crate::types::DataType;

    fn all_data_types() -> Vec<DataType> {
        vec![
            DataType::Boolean, // 0
            DataType::Int16,   // 1
            DataType::Int32,   // 2
            DataType::Int64,   // 3
            DataType::Float32, // 4
            DataType::Float64, // 5
            DataType::Decimal, // 6
            DataType::Varchar, // 7
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
        compare_key_kinds(&[0], HashKeyKind::KeySerialized);
        compare_key_kinds(&[1], HashKeyKind::Key16);
        compare_key_kinds(&[2], HashKeyKind::Key32);
        compare_key_kinds(&[3], HashKeyKind::Key64);
        compare_key_kinds(&[3, 4], HashKeyKind::Key128);
        compare_key_kinds(&[3, 4, 6], HashKeyKind::Key256);
        compare_key_kinds(&[7], HashKeyKind::KeySerialized);
        compare_key_kinds(&[1, 7], HashKeyKind::KeySerialized);
    }
}
