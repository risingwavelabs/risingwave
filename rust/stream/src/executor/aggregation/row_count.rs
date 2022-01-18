//! This module implements `StreamingRowCountAgg`.

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::types::{DataTypeKind, Datum, ScalarImpl};

use super::StreamingAggStateImpl;

/// `StreamingRowCountAgg` count rows, no matter whether the datum is null.
/// Note that if there are zero rows in aggregator, `0` will be emitted
/// instead of `None`. Note that if you want to only count non-null value,
/// use `StreamingCountAgg` instead.
#[derive(Clone, Debug)]
pub struct StreamingRowCountAgg {
    row_cnt: i64,
}

impl StreamingRowCountAgg {
    pub fn new() -> Self {
        StreamingRowCountAgg::with_row_cnt(None)
    }

    pub fn with_row_cnt(datum: Datum) -> Self {
        let mut row_cnt = 0;
        if let Some(cnt) = datum {
            match cnt {
                ScalarImpl::Int64(num) => {
                    row_cnt = num;
                }
                other => panic!(
          "type mismatch in streaming aggregator StreamingRowCountAgg init: expected i64, get {}",
          other.get_ident()
        ),
            }
        }
        Self { row_cnt }
    }

    pub fn create_array_builder(capacity: usize) -> Result<ArrayBuilderImpl> {
        I64ArrayBuilder::new(capacity).map(|builder| builder.into())
    }

    pub fn return_type() -> DataTypeKind {
        DataTypeKind::Int64
    }
}

impl Default for StreamingRowCountAgg {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingAggStateImpl for StreamingRowCountAgg {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        _data: &[&ArrayImpl],
    ) -> Result<()> {
        match visibility {
            None => {
                for op in ops {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.row_cnt += 1,
                        Op::Delete | Op::UpdateDelete => self.row_cnt -= 1,
                    }
                }
            }
            Some(visibility) => {
                for (op, visible) in ops.iter().zip(visibility.iter()) {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.row_cnt += 1,
                            Op::Delete | Op::UpdateDelete => self.row_cnt -= 1,
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn get_output(&self) -> Result<Datum> {
        Ok(Some(self.row_cnt.into()))
    }

    fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_countable_agg() {
        let mut state = StreamingRowCountAgg::new();

        // when there is no element, output should be `0`.
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &0);

        // insert one element to state
        state.apply_batch(&[Op::Insert], None, &[]).unwrap();

        // should be one row
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &1);

        // delete one element from state
        state.apply_batch(&[Op::Delete], None, &[]).unwrap();

        // should be 0 rows.
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &0);

        // one more deletion, so we are having `-1` elements inside.
        state.apply_batch(&[Op::Delete], None, &[]).unwrap();

        // should be the same as `TestState`'s output
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &-1);

        // one more insert, so we are having `0` elements inside.
        state
            .apply_batch(
                &[Op::Delete, Op::Insert],
                Some(&(vec![false, true]).try_into().unwrap()),
                &[],
            )
            .unwrap();

        // should be `0`
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &0);

        // one more deletion, so we are having `-1` elements inside.
        state
            .apply_batch(
                &[Op::Delete, Op::Insert],
                Some(&(vec![true, false]).try_into().unwrap()),
                &[],
            )
            .unwrap();

        // should be `-1`
        assert_eq!(state.get_output().unwrap().unwrap().as_int64(), &-1);
    }
}
