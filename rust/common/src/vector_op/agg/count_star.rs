use crate::array::*;
use crate::error::{ErrorCode, Result};
use crate::types::*;
use crate::vector_op::agg::aggregator::Aggregator;
use crate::vector_op::agg::general_sorted_grouper::EqGroups;

pub struct CountStar {
    return_type: DataType,
    result: usize,
}

impl CountStar {
    pub fn new(return_type: DataType, result: usize) -> Self {
        Self {
            return_type,
            result,
        }
    }
}

impl Aggregator for CountStar {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
    fn update(&mut self, input: &DataChunk) -> Result<()> {
        self.result += input.cardinality();
        Ok(())
    }
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
            ArrayBuilderImpl::Int64(b) => b.append(Some(self.result as i64)),
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
                return Err(
                    ErrorCode::InternalError("Unexpected builder for count(*).".into()).into(),
                )
            }
        };
        // The first element continues the same group in `self.result`. The following
        // groups' sizes are simply distance between group start indices. The distance
        // between last element and `input.cardinality()` is the ongoing group that
        // may continue in following chunks.
        let mut groups_iter = groups.get_starting_indices().iter();
        if let Some(first) = groups_iter.next() {
            builder.append(Some((self.result + first) as i64))?;
            let mut prev = first;
            for g in groups_iter {
                builder.append(Some((g - prev) as i64))?;
                prev = g;
            }
            self.result = input.cardinality() - prev;
        } else {
            self.result += input.cardinality();
        }
        Ok(())
    }

    fn update_with_row(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let Some(visibility) = input.visibility() {
            if visibility.is_set(row_id)? {
                self.result += 1;
            }
        } else {
            self.result += 1;
        }
        Ok(())
    }
}
