// Copyright 2025 RisingWave Labs
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

use super::*;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
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

impl EstimateSize for SparseCount {
    fn estimated_heap_size(&self) -> usize {
        self.inner.capacity() * std::mem::size_of::<(u8, u64)>()
    }
}

#[derive(Clone, Debug, EstimateSize, PartialEq, Eq)]
pub(super) struct UpdatableBucket<const DENSE_BITS: usize = 16> {
    dense_counts: [u64; DENSE_BITS],
    sparse_counts: SparseCount,
}

impl<const DENSE_BITS: usize> UpdatableBucket<DENSE_BITS> {
    fn get_bucket(&self, index: u8) -> Result<u64> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        if index > DENSE_BITS as u8 {
            Ok(self.sparse_counts.get(index))
        } else {
            Ok(self.dense_counts[index as usize - 1])
        }
    }
}

impl<const DENSE_BITS: usize> Default for UpdatableBucket<DENSE_BITS> {
    fn default() -> Self {
        Self {
            dense_counts: [0u64; DENSE_BITS],
            sparse_counts: SparseCount::new(),
        }
    }
}

impl<const DENSE_BITS: usize> Bucket for UpdatableBucket<DENSE_BITS> {
    fn update(&mut self, index: u8, retract: bool) -> Result<()> {
        if index > 64 || index == 0 {
            bail!("HyperLogLog: Invalid bucket index");
        }

        let count = self.get_bucket(index)?;

        if !retract {
            if index > DENSE_BITS as u8 {
                self.sparse_counts.add(index);
            } else if index >= 1 {
                if count == u64::MAX {
                    bail!(
                        "HyperLogLog: Count exceeds maximum bucket value.\
                        Your data stream may have too many repeated values or too large a\
                        cardinality for approx_count_distinct to handle (max: 2^64 - 1)"
                    );
                }
                self.dense_counts[index as usize - 1] = count + 1;
            }
        } else {
            // We don't have to worry about the user deleting nonexistent elements, so the counts
            // can never go below 0.
            if index > DENSE_BITS as u8 {
                self.sparse_counts.subtract(index);
            } else if index >= 1 {
                self.dense_counts[index as usize - 1] = count - 1;
            }
        }

        Ok(())
    }

    fn max(&self) -> u8 {
        if !self.sparse_counts.is_empty() {
            return self.sparse_counts.last_key();
        }
        for i in (0..DENSE_BITS).rev() {
            if self.dense_counts[i] > 0 {
                return i as u8 + 1;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_approx_count_distinct_insert_and_delete() {
        // sparse case
        test_streaming_approx_count_distinct_insert_and_delete_inner::<0>();
        // dense case
        test_streaming_approx_count_distinct_insert_and_delete_inner::<4>();
    }

    fn test_streaming_approx_count_distinct_insert_and_delete_inner<const DENSE_BITS: usize>() {
        let mut agg = Registers::<UpdatableBucket<DENSE_BITS>>::default();
        assert_eq!(agg.calculate_result(), 0);

        agg.update(1.into(), false).unwrap();
        agg.update(2.into(), false).unwrap();
        agg.update(3.into(), false).unwrap();
        assert_eq!(agg.calculate_result(), 3);

        agg.update(3.into(), false).unwrap();
        assert_eq!(agg.calculate_result(), 3);

        agg.update(3.into(), true).unwrap();
        agg.update(3.into(), true).unwrap();
        agg.update(1.into(), true).unwrap();
        agg.update(2.into(), true).unwrap();
        assert_eq!(agg.calculate_result(), 0);
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
        let mut agg = Registers::<UpdatableBucket<16>>::default();
        assert_eq!(agg.calculate_result(), 0);
        let actual_ndv = 1000000;
        for i in 0..1000000 {
            for _ in 0..3 {
                agg.update(i.into(), false).unwrap();
            }
        }

        let estimation = agg.calculate_result();
        let error_ratio = ((estimation - actual_ndv) as f64 / actual_ndv as f64).abs();
        assert!(error_ratio < 0.01);
    }

    fn test_register_bucket_get_and_update_inner<const DENSE_BITS: usize>() {
        let mut rb = UpdatableBucket::<DENSE_BITS>::default();

        for i in 0..20 {
            rb.update(i % 2 + 1, false).unwrap();
        }
        assert_eq!(rb.get_bucket(1).unwrap(), 10);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update(1, true).unwrap();
        assert_eq!(rb.get_bucket(1).unwrap(), 9);
        assert_eq!(rb.get_bucket(2).unwrap(), 10);

        rb.update(64, false).unwrap();
        assert_eq!(rb.get_bucket(64).unwrap(), 1);
    }

    #[test]
    fn test_register_bucket_invalid_register() {
        let mut rb = UpdatableBucket::<0>::default();

        assert!(rb.get_bucket(0).is_err());
        assert!(rb.get_bucket(65).is_err());
        assert!(rb.update(0, false).is_err());
        assert!(rb.update(65, false).is_err());
    }
}
