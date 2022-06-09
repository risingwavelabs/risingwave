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

//! This module implements `StreamingApproxCountDistinct`.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::{Datum, DatumRef, Scalar, ScalarImpl};

use super::StreamingAggStateImpl;

const INDEX_BITS: u8 = 10; // number of bits used for finding the index of each 64-bit hash
const INDICES: usize = 1 << INDEX_BITS; // number of indices available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash

/// `StreamingApproxCountDistinct` approximates the count of non-null rows using `HyperLogLog`.
#[derive(Clone, Debug)]
pub struct StreamingApproxCountDistinct {
    registers: [u8; INDICES],
    initial_count: i64,
}

impl StreamingApproxCountDistinct {
    pub fn new() -> Self {
        StreamingApproxCountDistinct::new_with_datum(None)
    }

    pub fn new_with_datum(datum: Datum) -> Self {
        let count = if let Some(c) = datum {
            match c {
                ScalarImpl::Int64(num) => num,
                other => panic!(
                    "type mismatch in streaming aggregator StreamingApproxCountDistinct init: expected i64, get {}",
                    other.get_ident()
                ),
            }
        } else {
            0
        };

        Self {
            registers: [0; INDICES],
            initial_count: count,
        }
    }

    /// Adds the count of the datum's hash into the register, if it is greater than the existing
    /// count at the register
    fn add_datum(&mut self, datum_ref: DatumRef) {
        if datum_ref.is_none() {
            return;
        }

        let scalar_impl = datum_ref.unwrap().into_scalar_impl();
        let hash = self.get_hash(scalar_impl);

        let index = (hash as usize) & (INDICES - 1); // Index is based on last few bits
        let count = self.count_hash(hash);

        if count > self.registers[index] {
            self.registers[index] = count;
        }
    }

    /// Calculate the hash of the `scalar_impl` using Rust's default hasher
    /// Perhaps a different hash like Murmur2 could be used instead for optimization?
    fn get_hash(&self, scalar_impl: ScalarImpl) -> u64 {
        let mut hasher = DefaultHasher::new();
        scalar_impl.hash(&mut hasher);
        hasher.finish()
    }

    /// Counts the number of trailing zeroes plus 1 in the non-index bits of the hash
    fn count_hash(&self, mut hash: u64) -> u8 {
        let mut count = 1;

        hash >>= INDEX_BITS; // Ignore bits used as index for the hash
        hash |= 1 << COUNT_BITS; // To allow hash to terminate if it is all 0s

        while hash & 1 == 0 {
            count += 1;
            hash >>= 1;
        }

        count
    }
}

impl StreamingAggStateImpl for StreamingApproxCountDistinct {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        match visibility {
            None => {
                for (op, datum) in ops.iter().zip_eq(data[0].iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.add_datum(datum),
                        Op::Delete | Op::UpdateDelete => {
                            return Err(RwError::from(ErrorCode::InternalError(
                                "insert only for approx_count_distinct".to_string(),
                            )))
                        }
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), datum) in
                    visibility.iter().zip_eq(ops.iter()).zip_eq(data[0].iter())
                {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.add_datum(datum),
                            Op::Delete | Op::UpdateDelete => {
                                return Err(RwError::from(ErrorCode::InternalError(
                                    "insert only for approx_count_distinct".to_string(),
                                )))
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_output(&self) -> Result<Datum> {
        // Approximation for bias correction. See "HyperLogLog: the analysis of a near-optimal
        // cardinality estimation algorithm" by Philippe Flajolet et al.
        let bias_correction = 0.72134;
        let m = INDICES as f64;
        let mut mean = 0.0;

        // Get harmonic mean of all the counts in results
        for count in self.registers.iter() {
            mean += 1.0 / ((1 << *count) as f64);
        }

        let raw_estimate = bias_correction * m * m / mean;

        // If raw_estimate is not much bigger than m and some registers have value 0, set answer to
        // m * log(m/V) where V is the number of registers with value 0
        let answer = if raw_estimate <= 2.5 * m {
            let mut zero_registers: f64 = 0.0;
            for i in self.registers.iter() {
                if *i == 0 {
                    zero_registers += 1.0;
                }
            }

            if zero_registers == 0.0 {
                raw_estimate
            } else {
                m * (m.log2() - (zero_registers.log2()))
            }
        } else {
            raw_estimate
        };

        Ok(Some((answer as i64 + self.initial_count).to_scalar_value()))
    }

    fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0).unwrap())
    }

    fn reset(&mut self) {
        self.registers = [0; INDICES];
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array_nonnull;

    use super::*;

    #[test]
    fn test_insert() {
        let mut agg = StreamingApproxCountDistinct::new();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Insert],
            Some(&(vec![true, false, false]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));
    }

    #[test]
    fn test_error_on_delete() {
        let mut agg = StreamingApproxCountDistinct::new();
        let result = agg.apply_batch(
            &[Op::Delete, Op::Insert],
            None,
            &[&array_nonnull!(I64Array, [3, 1]).into()],
        );

        assert_matches!(result, Err(_));
    }
}
