use std::marker::PhantomData;

use risingwave_common::error::Result;
use risingwave_common::types::{
    deserialize_datum_from, deserialize_datum_not_null_from, serialize_datum_into, DataTypeKind,
    Datum, Scalar, ScalarImpl,
};
use smallvec::SmallVec;

use crate::executor::PkDataTypeKinds;

type ExtremePkItem = Datum;

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
    pub data_type_kind: DataTypeKind,
    pub pk_data_type_kinds: PkDataTypeKinds,
    _phantom: PhantomData<K>,
}

impl<K: Scalar, const EXTREME_TYPE: usize> ExtremeSerializer<K, EXTREME_TYPE> {
    pub fn new(data_type_kind: DataTypeKind, pk_data_type_kinds: PkDataTypeKinds) -> Self {
        Self {
            data_type_kind,
            pk_data_type_kinds,
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
        assert_eq!(
            pk.len(),
            self.pk_data_type_kinds.len(),
            "mismatch pk length"
        );
        for i in pk {
            serialize_datum_into(i, &mut serializer)?;
        }

        // 3. take
        let encoded_key = serializer.into_inner();
        Ok(encoded_key)
    }

    /// Extract the pks from the sort key
    pub fn get_pk(&self, data: &[u8]) -> Result<ExtremePk> {
        if self.pk_data_type_kinds.is_empty() {
            return Ok(ExtremePk::default());
        }

        let mut deserializer = memcomparable::Deserializer::new(data);
        deserializer.set_reverse(self.is_reversed_order());

        // 1. key
        let _key = deserialize_datum_not_null_from(&self.data_type_kind, &mut deserializer)?;

        // 2. pk
        let mut pk = ExtremePk::with_capacity(self.pk_data_type_kinds.len());
        for kind in self.pk_data_type_kinds.iter() {
            let i = deserialize_datum_from(kind, &mut deserializer)?;
            pk.push(i);
        }

        Ok(pk)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::OrderedF64;
    use smallvec::smallvec;

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
                smallvec![DataTypeKind::Int64; pk_length],
            );
            let pk = (0..pk_length)
                .map(|x| (x as i64).to_scalar_value().into())
                .collect();
            for key in key_cases {
                let encoded_key = s.serialize(key, &pk)?;
                let decoded_pk = s.get_pk(&encoded_key)?;
                assert_eq!(pk, decoded_pk);
            }
        }

        Ok(())
    }
}
