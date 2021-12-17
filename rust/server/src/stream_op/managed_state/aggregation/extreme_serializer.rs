use std::marker::PhantomData;

use risingwave_common::error::Result;
use risingwave_common::types::{deserialize_datum_not_null_from, DataTypeKind, Scalar, ScalarImpl};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

// FIXME: support pk type besides i64
type ExtremePkItem = i64;

pub type ExtremePk = SmallVec<[ExtremePkItem; 1]>;

/// All possible extreme types.
pub mod variants {
    pub const EXTREME_MIN: usize = 0;
    pub const EXTREME_MAX: usize = 1;
}

/// A serializer built specifically for `ManagedExtremeState`
///
/// The serializer will encode original key and pks one by one. If `EXTREME_TYPE == EXTREME_MAX`,
/// we will flip the bits of the whole encoded data (including pks).
pub struct ExtremeSerializer<K: Scalar, const EXTREME_TYPE: usize> {
    data_type_kind: DataTypeKind,
    pk_length: usize,
    _phantom: PhantomData<K>,
}

impl<K: Scalar, const EXTREME_TYPE: usize> ExtremeSerializer<K, EXTREME_TYPE> {
    pub fn new(data_type_kind: DataTypeKind, pk_length: usize) -> Self {
        Self {
            data_type_kind,
            pk_length,
            _phantom: PhantomData,
        }
    }

    fn is_reversed_order(&self) -> bool {
        match EXTREME_TYPE {
            variants::EXTREME_MAX => true,
            variants::EXTREME_MIN => false,
            _ => unimplemented!(),
        }
    }

    /// Serialize key and `pk` (or, `row_id`s) into a sort key
    ///
    /// TODO: support `&K` instead of `K` as parameter.
    pub fn serialize(&self, key: K, pk: &ExtremePk) -> Result<Vec<u8>> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(self.is_reversed_order());

        // 1. key
        let key: ScalarImpl = key.into();
        key.serialize(&mut serializer)?;

        // 2. pk
        assert_eq!(pk.len(), self.pk_length, "mismatch pk length");
        for i in pk {
            (*i).serialize(&mut serializer)?;
        }

        // 3. take
        let encoded_key = serializer.into_inner();
        Ok(encoded_key)
    }

    /// Extract the pks from the sort key
    pub fn get_pk(&self, data: &[u8]) -> Result<ExtremePk> {
        if self.pk_length == 0 {
            return Ok(ExtremePk::default());
        }

        let mut deserializer = memcomparable::Deserializer::new(data);
        deserializer.set_reverse(self.is_reversed_order());

        // 1. key
        let _key = deserialize_datum_not_null_from(&self.data_type_kind, &mut deserializer)?;

        // 2. pk
        let mut pk = ExtremePk::with_capacity(self.pk_length);
        for _ in 0..self.pk_length {
            let i = ExtremePkItem::deserialize(&mut deserializer)?;
            pk.push(i);
        }
        assert_eq!(pk.len(), self.pk_length, "mismatch pk length");

        Ok(pk)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::OrderedF64;

    use super::*;

    #[test]
    fn test_extreme_serde_min() {
        test_extreme_serde::<{ variants::EXTREME_MIN }>().unwrap()
    }

    #[test]
    fn test_extreme_serde_max() {
        test_extreme_serde::<{ variants::EXTREME_MAX }>().unwrap()
    }

    fn test_extreme_serde<const EXTREME_TYPE: usize>() -> Result<()> {
        let pk_length_cases = [0, 1, 10];
        let key_cases = [1.14, 5.14, 19.19, 8.10].map(OrderedF64::from);

        for pk_length in pk_length_cases {
            let s = ExtremeSerializer::<OrderedF64, EXTREME_TYPE>::new(
                DataTypeKind::Float64,
                pk_length,
            );
            let pk = (0..pk_length).map(|x| x as i64).collect();
            for key in key_cases {
                let encoded_key = s.serialize(key, &pk)?;
                let decoded_pk = s.get_pk(&encoded_key)?;
                assert_eq!(pk, decoded_pk);
            }
        }

        Ok(())
    }
}
