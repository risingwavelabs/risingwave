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
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{Datum, DatumRef, Scalar, ScalarImpl};

use super::StreamingAggStateImpl;

const INDEX_BITS: u8 = 14; // number of bits used for finding the index of each 64-bit hash
const NUM_OF_REGISTERS: usize = 1 << INDEX_BITS; // number of registers available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash

// Approximation for bias correction for 16384 registers. See "HyperLogLog: the analysis of a
// near-optimal cardinality estimation algorithm" by Philippe Flajolet et al.
const BIAS_CORRECTION: f64 = 0.72125;

#[derive(Clone, Debug)]
struct RegisterBucket {
    count_1_to_16: Vec<u32>,
    count_17_to_24: Vec<u16>,
    count_25_to_32: Vec<u8>,
    count_33_to_64: u32,
}

impl RegisterBucket {
    pub fn new() -> Self {
        Self {
            count_1_to_16: vec![0; 16],
            count_17_to_24: vec![0; 8],
            count_25_to_32: vec![0; 8],
            count_33_to_64: 0,
        }
    }

    fn get_bucket(&self, index: usize) -> Result<u32> {
        if !(1..=64).contains(&index) {
            return Err(
                ErrorCode::InternalError("HyperLogLog: Invalid bucket index".into()).into(),
            );
        }

        if index >= 33 {
            return Ok(self.count_33_to_64 & (1 << (index - 33)));
        }

        if index >= 25 {
            return Ok(self.count_25_to_32[index - 25] as u32);
        }

        if index >= 17 {
            return Ok(self.count_17_to_24[index - 17] as u32);
        }

        Ok(self.count_1_to_16[index - 1])
    }

    /// Increments or decrements the bucket at `index` depending on the state of `is_insert`.
    /// Returns an Error if `index` is invalid or if inserting will cause an overflow in the bucket.
    fn update_bucket(&mut self, index: usize, is_insert: bool) -> Result<()> {
        if !(1..=64).contains(&index) {
            return Err(
                ErrorCode::InternalError("HyperLogLog: Invalid bucket index".into()).into(),
            );
        }

        let count = self.get_bucket(index).unwrap();
        if is_insert {
            if index >= 33 {
                if count == 1 {
                    return Err(ErrorCode::InternalError("HyperLogLog: Count exceeds maximum bucket value. Your data stream may have too many repeated values or too large a cardinality for approx_count_distinct to handle.".into()).into());
                }
                self.count_33_to_64 |= 1 << (index - 33);
            } else if index >= 25 {
                if count as u8 == u8::MAX {
                    return Err(ErrorCode::InternalError("HyperLogLog: Count exceeds maximum bucket value. Your data stream may have too many repeated values or too large a cardinality for approx_count_distinct to handle.".into()).into());
                }
                self.count_25_to_32[index - 25] = count as u8 + 1;
            } else if index >= 17 {
                if count as u16 == u16::MAX {
                    return Err(ErrorCode::InternalError("HyperLogLog: Count exceeds maximum bucket value. Your data stream may have too many repeated values or too large a cardinality for approx_count_distinct to handle.".into()).into());
                }
                self.count_17_to_24[index - 17] = count as u16 + 1;
            } else if index >= 1 {
                if count == u32::MAX {
                    return Err(ErrorCode::InternalError("HyperLogLog: Count exceeds maximum bucket value. Your data stream may have too many repeated values or too large a cardinality for approx_count_distinct to handle.".into()).into());
                }
                self.count_1_to_16[index - 1] = count + 1;
            }
        } else {
            // We don't have to worry about the user deleting nonexistent elements, so the counts
            // can never go below 0.
            if index >= 33 {
                self.count_33_to_64 ^= 1 << (index - 33);
            } else if index >= 25 {
                self.count_25_to_32[index - 25] = count as u8 - 1;
            } else if index >= 17 {
                self.count_17_to_24[index - 17] = count as u16 - 1;
            } else if index >= 1 {
                self.count_1_to_16[index - 1] = count - 1;
            }
        }

        Ok(())
    }

    /// Gets the number of the maximum bucket which has a count greater than zero.
    fn get_max(&self) -> u8 {
        for i in (1..64).rev() {
            if self.get_bucket(i).unwrap() > 0 {
                return i as u8;
            }
        }

        0
    }
}

/// `StreamingApproxCountDistinct` approximates the count of non-null rows using a modified version
/// of the `HyperLogLog` algorithm. Each `RegisterBucket` stores a count of how many hash values
/// have x trailing zeroes for all x from 1-64. This allows the algorithm to support insertion and
/// deletion, but uses up more memory and limits the number of rows that can be counted.
///
/// Currently, each `RegisterBucket` can count 2^33 rows on average. `StreamingApproxCountDistinct`
/// stores 2^14 `RegisterBuckets`, so about 2^47 or 141 trillion rows can be counted on average.
/// Each `RegisterBucket` takes up 736 bits, so the memory usage of `StreamingApproxCountDistinct`
/// is about 1.5 MB.
///
/// The estimation error for `HyperLogLog` is 1.04/sqrt(num of registers). With 2^14 registers this
/// is ~1/128.
#[derive(Clone, Debug)]
pub struct StreamingApproxCountDistinct {
    registers: Vec<RegisterBucket>,
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
            registers: vec![RegisterBucket::new(); NUM_OF_REGISTERS],
            initial_count: count,
        }
    }

    /// Adds the count of the datum's hash into the register, if it is greater than the existing
    /// count at the register.
    fn update_registers(&mut self, datum_ref: DatumRef, is_insert: bool) {
        if datum_ref.is_none() {
            return;
        }

        let scalar_impl = datum_ref.unwrap().into_scalar_impl();
        let hash = self.get_hash(scalar_impl);

        let index = (hash as usize) & (NUM_OF_REGISTERS - 1); // Index is based on last few bits
        let count = self.count_hash(hash) as usize;

        self.registers[index]
            .update_bucket(count, is_insert)
            .unwrap();
    }

    /// Calculate the hash of the `scalar_impl`.
    fn get_hash(&self, scalar_impl: ScalarImpl) -> u64 {
        let mut hasher = DefaultHasher::new();
        scalar_impl.hash(&mut hasher);
        hasher.finish()
    }

    /// Counts the number of trailing zeroes plus 1 in the non-index bits of the hash.
    fn count_hash(&self, mut hash: u64) -> u8 {
        hash >>= INDEX_BITS; // Ignore bits used as index for the hash
        hash |= 1 << COUNT_BITS; // To allow hash to terminate if it is all 0s

        (hash.trailing_zeros() + 1) as u8
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
                        Op::Insert | Op::UpdateInsert => self.update_registers(datum, true),
                        Op::Delete | Op::UpdateDelete => self.update_registers(datum, false),
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), datum) in
                    visibility.iter().zip_eq(ops.iter()).zip_eq(data[0].iter())
                {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.update_registers(datum, true),
                            Op::Delete | Op::UpdateDelete => self.update_registers(datum, false),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_output(&self) -> Result<Datum> {
        let m = NUM_OF_REGISTERS as f64;
        let mut mean = 0.0;

        // Get harmonic mean of all the counts in results
        for register_bucket in &self.registers {
            let count = register_bucket.get_max();
            mean += 1.0 / ((1 << count) as f64);
        }

        let raw_estimate = BIAS_CORRECTION * m * m / mean;

        // If raw_estimate is not much bigger than m and some registers have value 0, set answer to
        // m * log(m/V) where V is the number of registers with value 0
        let answer = if raw_estimate <= 2.5 * m {
            let mut zero_registers: f64 = 0.0;
            for i in &self.registers {
                if i.get_max() == 0 {
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
        self.registers = vec![RegisterBucket::new(); NUM_OF_REGISTERS];
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array_nonnull;

    use super::*;

    #[test]
    fn test_insert_and_delete() {
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

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![true, true, true, true]).try_into().unwrap()),
            &[&array_nonnull!(I64Array, [3, 3, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().into_int64(), 0);
    }
}
