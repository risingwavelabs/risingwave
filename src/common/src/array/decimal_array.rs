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

use super::{PrimitiveArray, PrimitiveArrayBuilder};
use crate::types::Decimal;

pub type DecimalArray = PrimitiveArray<Decimal>;
pub type DecimalArrayBuilder = PrimitiveArrayBuilder<Decimal>;

#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use std::str::FromStr;

    use itertools::Itertools;

    use super::*;
    use crate::array::{Array, ArrayBuilder, ArrayImpl, NULL_VAL_FOR_HASH};
    use crate::util::iter_util::ZipEqFast;

    #[test]
    fn test_decimal_builder() {
        let v = (0..1000).map(Decimal::from).collect_vec();
        let mut builder = DecimalArrayBuilder::new(0);
        for i in &v {
            builder.append(Some(*i));
        }
        let a = builder.finish();
        let res = v.iter().zip_eq_fast(a.iter()).all(|(a, b)| Some(*a) == b);
        assert!(res);
    }

    #[test]
    fn test_decimal_array_to_protobuf() {
        let input = vec![
            Some(Decimal::from_str("1.01").unwrap()),
            Some(Decimal::from_str("2.02").unwrap()),
            None,
            Some(Decimal::from_str("4.04").unwrap()),
            None,
            Some(Decimal::NegativeInf),
            Some(Decimal::PositiveInf),
            Some(Decimal::NaN),
        ];

        let array = DecimalArray::from_iter(&input);
        let prost_array = array.to_protobuf();

        assert_eq!(prost_array.values.len(), 1);

        let decoded_array = ArrayImpl::from_protobuf(&prost_array, 8)
            .unwrap()
            .into_decimal();

        assert!(array.iter().eq(decoded_array.iter()));
    }

    #[test]
    fn test_decimal_array_hash() {
        use std::hash::BuildHasher;

        use twox_hash::RandomXxHashBuilder64;

        use super::super::test_util::{hash_finish, test_hash};

        const ARR_NUM: usize = 3;
        const ARR_LEN: usize = 270;
        let vecs: [Vec<Option<Decimal>>; ARR_NUM] = [
            (0..ARR_LEN)
                .map(|x| match x % 2 {
                    0 => Some(Decimal::from(0)),
                    1 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 3 {
                    0 => Some(Decimal::from(0)),
                    #[expect(clippy::approx_constant)]
                    1 => Decimal::try_from(3.14).ok(),
                    2 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
            (0..ARR_LEN)
                .map(|x| match x % 5 {
                    0 => Some(Decimal::from(0)),
                    1 => Some(Decimal::from(123)),
                    #[expect(clippy::approx_constant)]
                    2 => Decimal::try_from(3.1415926).ok(),
                    #[expect(clippy::approx_constant)]
                    3 => Decimal::try_from(3.14).ok(),
                    4 => None,
                    _ => unreachable!(),
                })
                .collect_vec(),
        ];

        let arrs = vecs
            .iter()
            .map(|v| {
                let mut builder = DecimalArrayBuilder::new(0);
                for i in v {
                    builder.append(*i);
                }
                builder.finish()
            })
            .collect_vec();

        let hasher_builder = RandomXxHashBuilder64::default();
        let mut states = vec![hasher_builder.build_hasher(); ARR_LEN];
        vecs.iter().for_each(|v| {
            v.iter()
                .zip_eq_fast(&mut states)
                .for_each(|(x, state)| match x {
                    Some(inner) => inner.hash(state),
                    None => NULL_VAL_FOR_HASH.hash(state),
                })
        });
        let hashes = hash_finish(&mut states[..]);

        let count = hashes.iter().counts().len();
        assert_eq!(count, 30);

        test_hash(arrs, hashes, hasher_builder);
    }
}
