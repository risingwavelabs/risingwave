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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use dyn_clone::DynClone;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{Datum, DatumRef, Scalar, ScalarImpl};

use crate::executor::aggregation::agg_impl::StreamingAggImpl;
use crate::executor::StreamExecutorResult;

const INDEX_BITS: u8 = 16; // number of bits used for finding the index of each 64-bit hash
const NUM_OF_REGISTERS: u32 = 1 << INDEX_BITS; // number of registers available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash

// Approximation for bias correction for 16384 registers. See "HyperLogLog: the analysis of a
// near-optimal cardinality estimation algorithm" by Philippe Flajolet et al.
const BIAS_CORRECTION: f64 = 0.7213 / (1. + (1.079 / NUM_OF_REGISTERS as f64));

pub(super) trait RegisterBucket {
    fn new() -> Self;

    /// Increments or decrements the bucket at `index` depending on the state of `is_insert`.
    /// Returns an Error if `index` is invalid or if inserting will cause an overflow in the bucket.
    fn update_bucket(&mut self, index: usize, is_insert: bool) -> StreamExecutorResult<()>;

    /// Gets the number of the maximum bucket which has a count greater than zero.
    fn get_max(&self) -> u8;
}

/// `StreamingApproxCountDistinct` approximates the count of non-null rows using a modified version
/// of the `HyperLogLog` algorithm. Each `RegisterBucket` stores a count of how many hash values
/// have x trailing zeroes for all x from 1-64. This allows the algorithm to support insertion and
/// deletion, but uses up more memory and limits the number of rows that can be counted.
///
/// `StreamingApproxCountDistinct` can count up to a total of 2^64 unduplicated rows.
///
/// The estimation error for `HyperLogLog` is 1.04/sqrt(num of registers). With 2^16 registers this
/// is ~1/256, or about 0.4%. The memory usage for the default choice of parameters is about
/// (1024 + 24) bits * 2^16 buckets, which is about 8.58 MB.
pub(super) trait StreamingApproxCountDistinct: Sized {
    type Bucket: RegisterBucket;

    fn with_no_initial() -> Self {
        Self::with_datum(None)
    }

    fn with_datum(datum: Datum) -> Self {
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

        Self::with_i64(NUM_OF_REGISTERS, count)
    }

    fn with_i64(registers_num: u32, initial_count: i64) -> Self;
    fn get_initial_count(&self) -> i64;
    fn reset_buckets(&mut self, registers_num: u32);
    fn registers(&self) -> &[Self::Bucket];
    fn registers_mut(&mut self) -> &mut [Self::Bucket];

    /// Adds the count of the datum's hash into the register, if it is greater than the existing
    /// count at the register.
    fn update_registers(
        &mut self,
        datum_ref: DatumRef<'_>,
        is_insert: bool,
    ) -> StreamExecutorResult<()> {
        if datum_ref.is_none() {
            return Ok(());
        }

        let scalar_impl = datum_ref.unwrap().into_scalar_impl();
        let hash = self.get_hash(scalar_impl);

        let index = (hash as u32) & (NUM_OF_REGISTERS - 1); // Index is based on last few bits
        let count = self.count_hash(hash) as usize;

        self.registers_mut()[index as usize].update_bucket(count, is_insert)?;

        Ok(())
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

    fn apply_batch_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        match visibility {
            None => {
                for (op, datum) in ops.iter().zip_eq(data[0].iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.update_registers(datum, true)?,
                        Op::Delete | Op::UpdateDelete => self.update_registers(datum, false)?,
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), datum) in
                    visibility.iter().zip_eq(ops.iter()).zip_eq(data[0].iter())
                {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.update_registers(datum, true)?,
                            Op::Delete | Op::UpdateDelete => self.update_registers(datum, false)?,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_output_inner(&self) -> StreamExecutorResult<Datum> {
        let m = NUM_OF_REGISTERS as f64;
        let mut mean = 0.0;

        // Get harmonic mean of all the counts in results
        for register_bucket in self.registers() {
            let count = register_bucket.get_max();
            mean += 1.0 / ((1 << count) as f64);
        }

        let raw_estimate = BIAS_CORRECTION * m * m / mean;

        // If raw_estimate is not much bigger than m and some registers have value 0, set answer to
        // m * log(m/V) where V is the number of registers with value 0
        let answer = if raw_estimate <= 2.5 * m {
            let mut zero_registers: f64 = 0.0;
            for i in self.registers() {
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

        Ok(Some(
            (answer as i64 + self.get_initial_count()).to_scalar_value(),
        ))
    }
}

impl<B, T> StreamingAggImpl for T
where
    B: RegisterBucket,
    T: std::fmt::Debug
        + DynClone
        + Send
        + Sync
        + 'static
        + StreamingApproxCountDistinct<Bucket = B>,
{
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()> {
        self.apply_batch_inner(ops, visibility, data)
    }

    fn get_output(&self) -> StreamExecutorResult<Datum> {
        self.get_output_inner()
    }

    fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0))
    }

    fn reset(&mut self) {
        self.reset_buckets(NUM_OF_REGISTERS);
    }
}
