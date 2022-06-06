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

use risingwave_common::array::*;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::*;

use crate::vector_op::agg::aggregator::Aggregator;
use crate::vector_op::agg::general_sorted_grouper::EqGroups;

const INDEX_BITS: u8 = 10; // number of bits used for finding the index of each 64-bit hash
const INDICES: usize = 1 << INDEX_BITS; // number of indices available
const COUNT_BITS: u8 = 64 - INDEX_BITS; // number of non-index bits in each 64-bit hash

pub struct ApproxCountDistinct {
    return_type: DataType,
    input_col_idx: usize,
    registers: [u8; INDICES],
}

impl ApproxCountDistinct {
    pub fn new(return_type: DataType, input_col_idx: usize) -> Self {
        Self {
            return_type,
            input_col_idx,
            registers: [0; INDICES],
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
        hash >>= INDEX_BITS; // Ignore bits used as index for the hash
        hash |= 1 << COUNT_BITS; // To allow hash to terminate if it is all 0s

        (hash.trailing_zeros() + 1) as u8
    }

    /// Calculates the bias-corrected harmonic mean of the registers to get the approximate count
    fn calculate_result(&self) -> i64 {
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

        answer as i64
    }
}

impl Aggregator for ApproxCountDistinct {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn update_with_row(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        let array = input.column_at(self.input_col_idx).array_ref();
        let datum_ref = array.value_at(row_id);
        self.add_datum(datum_ref);

        Ok(())
    }

    fn update(&mut self, input: &DataChunk) -> Result<()> {
        let array = input.column_at(self.input_col_idx).array_ref();
        for datum_ref in array.iter() {
            self.add_datum(datum_ref);
        }
        Ok(())
    }

    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        let result = self.calculate_result();
        match builder {
            ArrayBuilderImpl::Int64(b) => b.append(Some(result)),
            _ => Err(ErrorCode::InternalError("Unexpected builder for count(*).".into()).into()),
        }
    }

    fn update_and_output_with_sorted_groups(
        &mut self,
        input: &DataChunk,
        builder: &mut ArrayBuilderImpl,
        groups: &EqGroups,
    ) -> Result<()> {
        let builder = match builder {
            ArrayBuilderImpl::Int64(b) => b,
            _ => {
                return Err(ErrorCode::InternalError(
                    "Unexpected builder for approx_distinct_count().".into(),
                )
                .into())
            }
        };

        let array = input.column_at(self.input_col_idx).array_ref();
        let mut group_cnt = 0;
        let mut groups_iter = groups.starting_indices().iter().peekable();
        let chunk_offset = groups.chunk_offset();
        for (i, datum_ref) in array.iter().skip(chunk_offset).enumerate() {
            // reset state and output result when new group is found
            if groups_iter.peek() == Some(&&i) {
                groups_iter.next();
                group_cnt += 1;
                builder.append(Some(self.calculate_result()))?;
                self.registers = [0; INDICES];
            }

            self.add_datum(datum_ref);

            // reset state and exit when reach limit
            if groups.is_reach_limit(group_cnt) {
                self.registers = [0; INDICES];
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{
        ArrayBuilder, ArrayBuilderImpl, DataChunk, I32Array, I64ArrayBuilder,
    };
    use risingwave_common::types::DataType;

    use crate::vector_op::agg::aggregator::Aggregator;
    use crate::vector_op::agg::approx_count_distinct::ApproxCountDistinct;
    use crate::vector_op::agg::EqGroups;

    fn generate_data_chunk(size: usize, start: i32) -> DataChunk {
        let mut lhs = vec![];
        for i in start..((size as i32) + start) {
            lhs.push(Some(i));
        }

        let col1 = Column::new(
            I32Array::from_slice(&lhs)
                .map(|x| Arc::new(x.into()))
                .unwrap(),
        );

        DataChunk::new(vec![col1], size)
    }

    #[test]
    fn test_update_and_output() {
        let inputs_size: [usize; 3] = [20000, 10000, 5000];
        let inputs_start: [i32; 3] = [0, 20000, 30000];

        let mut agg = ApproxCountDistinct::new(DataType::Int64, 0);
        let mut builder = ArrayBuilderImpl::Int64(I64ArrayBuilder::new(3).unwrap());

        for i in 0..3 {
            let data_chunk = generate_data_chunk(inputs_size[i], inputs_start[i]);
            agg.update(&data_chunk).unwrap();
            agg.output(&mut builder).unwrap();
        }

        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn test_update_and_output_with_sorted_groups() {
        let mut a = ApproxCountDistinct::new(DataType::Int64, 0);

        let data_chunk = generate_data_chunk(30001, 0);
        let mut builder = ArrayBuilderImpl::Int64(I64ArrayBuilder::new(5).unwrap());
        let mut group = EqGroups::new(vec![5000, 10000, 14000, 20000, 30000]);
        group.set_limit(5);

        let _ = a.update_and_output_with_sorted_groups(&data_chunk, &mut builder, &group);
        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 5);
    }
}
