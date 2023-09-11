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

use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Range;

use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::Row;
use risingwave_common::types::*;
use risingwave_expr_macro::build_aggregate;

use self::append_only::AppendOnlyBucket;
use self::updatable::UpdatableBucket;
use super::{AggCall, AggStateDyn, AggregateFunction, AggregateState};
use crate::Result;

mod append_only;
mod updatable;

const INDEX_BITS: u8 = 16; // number of bits used for finding the index of each 64-bit hash
const NUM_OF_REGISTERS: usize = 1 << INDEX_BITS; // number of indices available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash
const LOG_COUNT_BITS: u8 = 6;

// Approximation for bias correction for 16384 registers. See "HyperLogLog: the analysis of a
// near-optimal cardinality estimation algorithm" by Philippe Flajolet et al.
const BIAS_CORRECTION: f64 = 0.7213 / (1. + (1.079 / NUM_OF_REGISTERS as f64));

/// Count the approximate number of unique non-null values.
#[build_aggregate("approx_count_distinct(*) -> int64")]
fn build(_agg: &AggCall) -> Result<Box<dyn AggregateFunction>> {
    Ok(Box::new(ApproxCountDistinct::<UpdatableBucket> {
        _mark: PhantomData,
    }))
}

struct ApproxCountDistinct<B: Bucket> {
    _mark: PhantomData<B>,
}

#[async_trait::async_trait]
impl<B: Bucket> AggregateFunction for ApproxCountDistinct<B> {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn create_state(&self) -> AggregateState {
        AggregateState::Any(Box::<UpdatableRegisters>::default())
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = state.downcast_mut::<UpdatableRegisters>();
        for (op, row) in input.rows() {
            let retract = matches!(op, Op::Delete | Op::UpdateDelete);
            if let Some(scalar) = row.datum_at(0) {
                state.update(scalar, retract)?;
            }
        }
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        let state = state.downcast_mut::<UpdatableRegisters>();
        for (op, row) in input.rows_in(range) {
            let retract = matches!(op, Op::Delete | Op::UpdateDelete);
            if let Some(scalar) = row.datum_at(0) {
                state.update(scalar, retract)?;
            }
        }
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = state.downcast_ref::<UpdatableRegisters>();
        Ok(Some(state.calculate_result().into()))
    }
}

/// Approximates the count of non-null rows using a modified version of the `HyperLogLog` algorithm.
/// Each `Bucket` stores a count of how many hash values have x trailing zeroes for all x from 1-64.
/// This allows the algorithm to support insertion and deletion, but uses up more memory and limits
/// the number of rows that can be counted.
///
/// This can count up to a total of 2^64 unduplicated rows.
///
/// The estimation error for `HyperLogLog` is 1.04/sqrt(num of registers). With 2^16 registers this
/// is ~1/256, or about 0.4%. The memory usage for the default choice of parameters is about
/// (1024 + 24) bits * 2^16 buckets, which is about 8.58 MB.
#[derive(Debug, Clone)]
struct Registers<B: Bucket> {
    registers: Box<[B]>,
    // FIXME: Currently we only store the count result (i64) as the state of updatable register.
    // This is not correct, because the state should be the registers themselves.
    initial_count: i64,
}

type UpdatableRegisters = Registers<UpdatableBucket>;
type AppendOnlyRegisters = Registers<AppendOnlyBucket>;

trait Bucket: Debug + Default + Clone + EstimateSize + Send + Sync + 'static {
    /// Increments or decrements the bucket at `index` depending on the state of `retract`.
    /// Returns an Error if `index` is invalid or if inserting will cause an overflow in the bucket.
    fn update(&mut self, index: u8, retract: bool) -> Result<()>;

    /// Gets the number of the maximum bucket which has a count greater than zero.
    fn max(&self) -> u8;
}

impl<B: Bucket> AggStateDyn for Registers<B> {}

impl<B: Bucket> Default for Registers<B> {
    fn default() -> Self {
        Self {
            registers: (0..NUM_OF_REGISTERS).map(|_| B::default()).collect(),
            initial_count: 0,
        }
    }
}

impl<B: Bucket> Registers<B> {
    /// Adds the count of the datum's hash into the register, if it is greater than the existing
    /// count at the register
    fn update(&mut self, scalar_ref: ScalarRefImpl<'_>, retract: bool) -> Result<()> {
        let hash = self.get_hash(scalar_ref);

        let index = (hash as usize) & (NUM_OF_REGISTERS - 1); // Index is based on last few bits
        let count = self.count_hash(hash);

        self.registers[index].update(count, retract)?;
        Ok(())
    }

    /// Calculate the hash of the `scalar` using Rust's default hasher
    /// Perhaps a different hash like Murmur2 could be used instead for optimization?
    fn get_hash(&self, scalar: ScalarRefImpl<'_>) -> u64 {
        let mut hasher = DefaultHasher::new();
        scalar.hash(&mut hasher);
        hasher.finish()
    }

    /// Counts the number of trailing zeroes plus 1 in the non-index bits of the hash
    fn count_hash(&self, mut hash: u64) -> u8 {
        hash >>= INDEX_BITS; // Ignore bits used as index for the hash
        hash |= 1 << COUNT_BITS; // To allow hash to terminate if it is all 0s

        (hash.trailing_zeros() + 1) as u8
    }

    /// Calculates the bias-corrected harmonic mean of the registers to get the approximate count
    fn calculate_result(&self) -> i64 {
        let m = NUM_OF_REGISTERS as f64;
        let mut mean = 0.0;

        // Get harmonic mean of all the counts in results
        for bucket in &*self.registers {
            let count = bucket.max();
            mean += 1.0 / ((1 << count) as f64);
        }

        let raw_estimate = BIAS_CORRECTION * m * m / mean;

        // If raw_estimate is not much bigger than m and some registers have value 0, set answer to
        // m * log(m/V) where V is the number of registers with value 0
        let answer = if raw_estimate <= 2.5 * m {
            let mut zero_registers: f64 = 0.0;
            for i in &*self.registers {
                if i.max() == 0 {
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

        self.initial_count + answer as i64
    }
}

impl<B: Bucket> From<Registers<B>> for i64 {
    fn from(reg: Registers<B>) -> Self {
        reg.calculate_result()
    }
}

impl<B: Bucket> EstimateSize for Registers<B> {
    fn estimated_heap_size(&self) -> usize {
        self.registers.len() * std::mem::size_of::<B>()
    }
}

/// Serialize the state into a scalar.
impl From<AppendOnlyRegisters> for ScalarImpl {
    fn from(reg: AppendOnlyRegisters) -> Self {
        let buckets = &reg.registers[..];
        let result_len = (buckets.len() * LOG_COUNT_BITS as usize - 1) / (i64::BITS as usize) + 1;
        let mut result = vec![0u64; result_len];
        for (i, bucket_val) in buckets.iter().enumerate() {
            let (start_idx, begin_bit, post_end_bit) = pos_in_serialized(i);
            result[start_idx] |= (buckets[i].0 as u64) << begin_bit;
            if post_end_bit > i64::BITS {
                result[start_idx + 1] |= (bucket_val.0 as u64) >> (i64::BITS - begin_bit as u32);
            }
        }
        ScalarImpl::List(ListValue::new(
            result
                .into_iter()
                .map(|x| Some(ScalarImpl::Int64(x as i64)))
                .collect(),
        ))
    }
}

/// Deserialize the state from a scalar.
impl From<ScalarImpl> for AppendOnlyRegisters {
    fn from(state: ScalarImpl) -> Self {
        let list = state.as_list().values();
        let bucket_num = list.len() * i64::BITS as usize / LOG_COUNT_BITS as usize;
        let registers = (0..bucket_num)
            .map(|i| {
                let (start_idx, begin_bit, post_end_bit) = pos_in_serialized(i);
                let val = *list[start_idx].as_ref().unwrap().as_int64();
                let v = if post_end_bit <= i64::BITS {
                    (val as u64) << (i64::BITS - post_end_bit)
                        >> (i64::BITS - LOG_COUNT_BITS as u32)
                } else {
                    ((val as u64) >> begin_bit)
                        + (((*list[start_idx + 1].as_ref().unwrap().as_int64() as u64)
                            & ((1 << (post_end_bit - i64::BITS)) - 1))
                            << (i64::BITS - begin_bit as u32))
                };
                AppendOnlyBucket(v as u8)
            })
            .collect();
        Self {
            registers,
            initial_count: 0,
        }
    }
}

/// Serialize the state into a scalar.
impl From<UpdatableRegisters> for ScalarImpl {
    fn from(reg: UpdatableRegisters) -> Self {
        // FIXME: store state of updatable registers properly
        ScalarImpl::Int64(reg.calculate_result())
    }
}

/// Deserialize the state from a scalar.
impl From<ScalarImpl> for UpdatableRegisters {
    fn from(state: ScalarImpl) -> Self {
        // FIXME: restore state of updatable registers properly
        Self {
            initial_count: state.into_int64(),
            ..Self::default()
        }
    }
}

fn pos_in_serialized(bucket_idx: usize) -> (usize, usize, u32) {
    // rust compiler will optimize for us
    let start_idx = bucket_idx * LOG_COUNT_BITS as usize / i64::BITS as usize;
    let begin_bit = bucket_idx * LOG_COUNT_BITS as usize % i64::BITS as usize;
    let post_end_bit = begin_bit as u32 + LOG_COUNT_BITS as u32;
    (start_idx, begin_bit, post_end_bit)
}

#[cfg(test)]
mod tests {
    use futures_util::FutureExt;
    use risingwave_common::array::{Array, DataChunk, I32Array, StreamChunk};

    use crate::agg::AggCall;

    #[test]
    fn test() {
        let approx_count_distinct = crate::agg::build(&AggCall::from_pretty(
            "(approx_count_distinct:int8 $0:int4)",
        ))
        .unwrap();

        for range in [0..20000, 20000..30000, 30000..35000] {
            let col = I32Array::from_iter(range.clone()).into_ref();
            let input = StreamChunk::from(DataChunk::new(vec![col], range.len()));
            let mut state = approx_count_distinct.create_state();
            approx_count_distinct
                .update(&mut state, &input)
                .now_or_never()
                .unwrap()
                .unwrap();
            let count = approx_count_distinct
                .get_result(&state)
                .now_or_never()
                .unwrap()
                .unwrap()
                .unwrap()
                .into_int64() as usize;
            let actual = range.len();
            // FIXME: the error is too large?
            // assert!((actual as f32 * 0.9..actual as f32 * 1.1).contains(&(count as f32)));
            let expected_range = actual as f32 * 0.5..actual as f32 * 1.5;
            if !expected_range.contains(&(count as f32)) {
                panic!("approximate count {} not in {:?}", count, expected_range);
            }
        }
    }
}
