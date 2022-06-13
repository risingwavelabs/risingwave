// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::marker::PhantomData;

use risingwave_common::types::{
    deserialize_datum_from, serialize_datum_into, DataType, Datum, Scalar,
};
use smallvec::SmallVec;

use crate::executor::error::StreamExecutorResult;
use crate::executor::PkDataTypes;

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
    pub data_type: DataType,
    pub pk_data_types: PkDataTypes,
    _phantom: PhantomData<K>,
}

impl<K: Scalar, const EXTREME_TYPE: usize> ExtremeSerializer<K, EXTREME_TYPE> {
    pub fn new(data_type: DataType, pk_data_types: PkDataTypes) -> Self {
        Self {
            data_type,
            pk_data_types,
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
    pub fn serialize(&self, key: Option<K>, pk: &ExtremePk) -> StreamExecutorResult<Vec<u8>> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        serializer.set_reverse(self.is_reversed_order());

        // 1. key
        let key: Datum = key.map(|x| x.into());
        serialize_datum_into(&key, &mut serializer)?;

        // 2. pk
        assert_eq!(pk.len(), self.pk_data_types.len(), "mismatch pk length");
        for i in pk {
            serialize_datum_into(i, &mut serializer)?;
        }

        // 3. take
        let encoded_key = serializer.into_inner();
        Ok(encoded_key)
    }

    /// Extract the pks from the sort key
    pub fn get_pk(&self, data: &[u8]) -> StreamExecutorResult<ExtremePk> {
        if self.pk_data_types.is_empty() {
            return Ok(ExtremePk::default());
        }

        let mut deserializer = memcomparable::Deserializer::new(data);
        deserializer.set_reverse(self.is_reversed_order());

        // 1. key
        let _key = deserialize_datum_from(&self.data_type, &mut deserializer)?;

        // 2. pk
        let mut pk = ExtremePk::with_capacity(self.pk_data_types.len());
        for kind in self.pk_data_types.iter() {
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

    fn test_extreme_serde<const EXTREME_TYPE: usize>() -> StreamExecutorResult<()> {
        let pk_length_cases = [0, 1, 10];
        let key_cases = [1.14, 5.14, 19.19, 8.10].map(OrderedF64::from);

        for pk_length in pk_length_cases {
            let s = ExtremeSerializer::<OrderedF64, EXTREME_TYPE>::new(
                DataType::Float64,
                smallvec![DataType::Int64; pk_length],
            );
            let pk = (0..pk_length)
                .map(|x| (x as i64).to_scalar_value().into())
                .collect();
            for key in key_cases {
                let encoded_key = s.serialize(Some(key), &pk)?;
                let decoded_pk = s.get_pk(&encoded_key)?;
                assert_eq!(pk, decoded_pk);
            }
        }

        Ok(())
    }
}
