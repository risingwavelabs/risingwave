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

//! This module implements `DeletableStreamingApproxDistinct`.

use risingwave_common::bail;

use super::approx_distinct_utils::{RegisterBucket, StreamingApproxDistinct};
use crate::executor::error::StreamExecutorResult;

pub(crate) const DENSE_BITS_DEFAULT: usize = 16; // number of bits in the dense repr of the `DeletableRegisterBucket`

#[derive(Clone, Debug)]
struct SparseCount {
    inner: Vec<(u8, u64)>,
}

impl SparseCount {
    fn new() -> Self {
        Self {
            inner: Vec::default(),
        }
    }

    fn get(&self, k: u8) -> u64 {
        for (key, count) in &self.inner {
            if *key == k {
                return *count;
            }
            if *key > k {
                break;
            }
        }
        0
    }

    fn add(&mut self, k: u8) -> bool {
        let mut last = 0;
        for (key, count) in &mut self.inner {
            if *key == k {
                *count += 1;
                return true;
            }
            if *key > k {
                break;
            }
            last += 1;
        }
        self.inner.insert(last, (k, 1));
        false
    }

    fn subtract(&mut self, k: u8) -> bool {
        for (i, (key, count)) in self.inner.iter_mut().enumerate() {
            if *key == k {
                *count -= 1;
                if *count == 0 {
                    // delete the count
                    self.inner.remove(i);
                }
                return true;
            }
            if *key > k {
                break;
            }
        }
        false
    }

    fn is_empty(&self) -> bool {
        self.inner.len() == 0
    }

    fn last_key(&self) -> u8 {
        assert!(!self.is_empty());
        self.inner.last().unwrap().0
    }
}

#[derive(Clone, Debug)]
pub(super) struct DeletableRegisterBucket<const DENSE_BITS: usize> {
    dense_counts: [u64; DENSE_BITS],
    sparse_counts: SparseCount,
}

impl<const DENSE_BITS: usize> DeletableRegisterBucket<DENSE_BITS> {
    fn get_bucket(&self, index: usize) -> StreamExecutorResult<u64> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        if index > DENSE_BITS {
            Ok(self.sparse_counts.get(index as u8))
        } else {
            Ok(self.dense_counts[index - 1])
        }
    }
}

impl<const DENSE_BITS: usize> RegisterBucket for DeletableRegisterBucket<DENSE_BITS> {
    fn new() -> Self {
        Self {
            dense_counts: [0u64; DENSE_BITS],
            sparse_counts: SparseCount::new(),
        }
    }

    fn update_bucket(&mut self, index: usize, is_insert: bool) -> StreamExecutorResult<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        let count = self.get_bucket(index)?;

        if is_insert {
            if index > DENSE_BITS {
                self.sparse_counts.add(index as u8);
            } else if index >= 1 {
                if count == u64::MAX {
                    bail!(
                        "HyperLogLog: Count exceeds maximum bucket value.\
                        Your data stream may have too many repeated values or too large a\
                        cardinality for approx_count_distinct to handle (max: 2^64 - 1)"
                    );
                }
                self.dense_counts[index - 1] = count + 1;
            }
        } else {
            // We don't have to worry about the user deleting nonexistent elements, so the counts
            // can never go below 0.
            if index > DENSE_BITS {
                self.sparse_counts.subtract(index as u8);
            } else if index >= 1 {
                self.dense_counts[index - 1] = count - 1;
            }
        }

        Ok(())
    }

    fn get_max(&self) -> StreamExecutorResult<u8> {
        if !self.sparse_counts.is_empty() {
            return Ok(self.sparse_counts.last_key());
        }
        for i in (0..DENSE_BITS).rev() {
            if self.dense_counts[i] > 0 {
                return Ok(i as u8 + 1);
            }
        }
        Ok(0)
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeletableStreamingApproxDistinct<const DENSE_BITS: usize> {
    // TODO(yuchao): The state may need to be stored in state table to allow correct recovery.
    registers: Vec<DeletableRegisterBucket<DENSE_BITS>>,
    initial_count: i64,
}

impl<const DENSE_BITS: usize> StreamingApproxDistinct
    for DeletableStreamingApproxDistinct<DENSE_BITS>
{
    type Bucket = DeletableRegisterBucket<DENSE_BITS>;

    fn with_i64(registers_num: u32, initial_count: i64) -> Self {
        Self {
            registers: vec![DeletableRegisterBucket::new(); registers_num as usize],
            initial_count,
        }
    }

    fn get_initial_count(&self) -> i64 {
        self.initial_count
    }

    fn reset_buckets(&mut self, registers_num: u32) {
        self.registers = vec![DeletableRegisterBucket::new(); registers_num as usize];
    }

    fn registers(&self) -> &[DeletableRegisterBucket<DENSE_BITS>] {
        &self.registers
    }

    fn registers_mut(&mut self) -> &mut [DeletableRegisterBucket<DENSE_BITS>] {
        &mut self.registers
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::*;
    use risingwave_common::array_nonnull;

    use super::*;
    use crate::executor::aggregation::agg_impl::StreamingAggImpl;

    #[test]
    fn test_streaming_approx_count_distinct_insert_and_delete() {
        // sparse case
        test_streaming_approx_count_distinct_insert_and_delete_inner::<0>();
        // dense case
        test_streaming_approx_count_distinct_insert_and_delete_inner::<4>();
    }

    fn test_streaming_approx_count_distinct_insert_and_delete_inner<const DENSE_BITS: usize>() {
        let mut agg = DeletableStreamingApproxDistinct::<DENSE_BITS>::new();
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
            Some(&(vec![true, false, false]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 3, 1]).into()],
        )
        .unwrap();
        assert_matches!(agg.get_output().unwrap(), Some(_));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![true, true, true, true]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 3, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().into_int64(), 0);
    }

    #[test]
    fn test_register_bucket_get_and_update() {
        // sparse case
        test_register_bucket_get_and_update_inner::<0>();
        // dense case
        test_register_bucket_get_and_update_inner::<4>();
    }

    /// In this test case, we use `1_000_000` distinct values to ensure there is enough samples.
    /// Theoretically, we need at least 2.5 * m samples. (m is the registers size which is equal to
    /// 2^`INDEX_BITS`, by default `INDEX_BITS` is 16) The error can be estimated as 1.04 /
    /// sqrt(m) which is approximately equal to 0.004, So we use 0.01 to make sure we can bound the
    /// error.
    #[test]
    fn test_error_ratio() {
        let mut agg = DeletableStreamingApproxDistinct::<16>::new();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);
        let actual_ndv = 1000000;
        for i in 0..1000000 {
            agg.apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&array_nonnull!(I64Array, [i, i, i]).into()],
            )
            .unwrap();
        }

        let estimation = agg.get_output().unwrap().unwrap().into_int64();
        let error_ratio = ((estimation - actual_ndv) as f64 / actual_ndv as f64).abs();
        assert!(error_ratio < 0.01);
    }

    fn test_register_bucket_get_and_update_inner<const DENSE_BITS: usize>() {
        let mut rb = DeletableRegisterBucket::<DENSE_BITS>::new();

        for i in 0..20 {
            rb.update_bucket(i % 2 + 1, true).unwrap();
        }
        assert_eq!(rb.get_bucket(1).unwrap(), 10);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(1, false).unwrap();
        assert_eq!(rb.get_bucket(1).unwrap(), 9);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update_bucket(64, true).unwrap();
        assert_eq!(rb.get_bucket(64).unwrap(), 1);
    }

    #[test]
    fn test_register_bucket_invalid_register() {
        let mut rb = DeletableRegisterBucket::<0>::new();

        assert_matches!(rb.get_bucket(0), Err(_));
        assert_matches!(rb.get_bucket(65), Err(_));
        assert_matches!(rb.update_bucket(0, true), Err(_));
        assert_matches!(rb.update_bucket(65, true), Err(_));
    }
}
