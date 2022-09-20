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

//! This module implements `StreamingSingleValueAgg`.

use itertools::Itertools;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{option_to_owned_scalar, Datum, Scalar, ScalarImpl};

use crate::executor::aggregation::StreamingAggStateImpl;
use crate::executor::error::StreamExecutorResult;

/// `StreamingSingleValueAgg` is a temporary workaround to deal with scalar subquery.
/// Scalar subquery can at most return one row, otherwise runtime error should be emitted.
///
/// When subquery un-nesting becomes more powerful in the future, this should be removed.
#[derive(Debug, Default)]
pub struct StreamingSingleValueAgg<T: Array> {
    /// row count to record how many rows it has accumulated.
    /// Since this aggregation function would error when it accumulates
    /// more than one row,
    row_cnt: i64,
    result: Option<T::OwnedItem>,
}

impl<T: Array> Clone for StreamingSingleValueAgg<T> {
    fn clone(&self) -> Self {
        Self {
            row_cnt: self.row_cnt,
            result: self.result.clone(),
        }
    }
}

impl<T: Array> StreamingSingleValueAgg<T> {
    pub fn new() -> Self {
        Self::with_row_cnt(None)
    }

    /// This function makes the assumption that if this function gets called, then
    /// we must have row count equal to 1. If the row count is equal to 0,
    /// then `new` will be called instead of this function.
    pub fn with_datum(x: Datum) -> StreamExecutorResult<Self> {
        let mut result = None;
        if let Some(scalar) = x {
            result = Some(T::OwnedItem::try_from(scalar)?);
        }

        Ok(Self { row_cnt: 1, result })
    }

    pub fn with_row_cnt(datum: Datum) -> Self {
        let mut row_cnt = 0;
        if let Some(cnt) = datum {
            match cnt {
                ScalarImpl::Int64(num) => {
                    row_cnt = num;
                }
                other => panic!(
                    "type mismatch in streaming aggregator StreamingSingleValueAgg init: expected i64, get {}",
                    other.get_ident()
                ),
            }
        }
        Self {
            row_cnt,
            result: None,
        }
    }

    fn accumulate(&mut self, input: Option<T::RefItem<'_>>) -> StreamExecutorResult<()> {
        self.row_cnt += 1;
        if self.row_cnt > 1 {
            bail!("SingleValue aggregation can only accept exactly one value. But there is more than one.");
        } else {
            self.result = option_to_owned_scalar(&input);
            Ok(())
        }
    }

    fn retract(&mut self, _input: Option<T::RefItem<'_>>) -> StreamExecutorResult<()> {
        if self.row_cnt != 1 {
            bail!("SingleValue aggregation can only retract exactly one value. But there are {} values.", self.row_cnt);
        } else {
            self.row_cnt -= 1;
            self.result = None;
            Ok(())
        }
    }

    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &T,
    ) -> StreamExecutorResult<()> {
        match visibility {
            None => {
                for (op, data) in ops.iter().zip_eq(data.iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => self.accumulate(data)?,
                        Op::Delete | Op::UpdateDelete => self.retract(data)?,
                    }
                }
            }
            Some(visibility) => {
                for ((visible, op), data) in
                    visibility.iter().zip_eq(ops.iter()).zip_eq(data.iter())
                {
                    if visible {
                        match op {
                            Op::Insert | Op::UpdateInsert => self.accumulate(data)?,
                            Op::Delete | Op::UpdateDelete => self.retract(data)?,
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

macro_rules! impl_single_value_agg {
    ($array_type:ty, $result_variant:ident) => {
        impl StreamingAggStateImpl for StreamingSingleValueAgg<$array_type> {
            fn apply_batch(
                &mut self,
                ops: Ops<'_>,
                visibility: Option<&Bitmap>,
                data: &[&ArrayImpl],
            ) -> StreamExecutorResult<()> {
                self.apply_batch_concrete(ops, visibility, data[0].into())
            }

            fn get_output(&self) -> StreamExecutorResult<Datum> {
                Ok(self.result.clone().map(Scalar::to_scalar_value))
            }

            fn new_builder(&self) -> ArrayBuilderImpl {
                ArrayBuilderImpl::$result_variant(<$array_type as Array>::Builder::new(0))
            }

            fn reset(&mut self) {
                self.row_cnt = 0;
            }
        }
    };
}

impl_single_value_agg! {I16Array, Int16}
impl_single_value_agg! {I32Array, Int32}
impl_single_value_agg! {I64Array, Int64}
impl_single_value_agg! {F32Array, Float32}
impl_single_value_agg! {F64Array, Float64}
impl_single_value_agg! {BoolArray, Bool}
impl_single_value_agg! {DecimalArray, Decimal}
impl_single_value_agg! {Utf8Array, Utf8}
impl_single_value_agg! {IntervalArray, Interval}
impl_single_value_agg! {NaiveDateArray, NaiveDate}
impl_single_value_agg! {NaiveDateTimeArray, NaiveDateTime}
impl_single_value_agg! {NaiveTimeArray, NaiveTime}
impl_single_value_agg! {StructArray, Struct}

#[cfg(test)]
mod tests {
    use risingwave_common::array_nonnull;

    use super::*;

    #[test]
    fn test_single_value_agg() {
        let mut state = StreamingSingleValueAgg::<Utf8Array>::new();

        assert_eq!(state.get_output().unwrap(), None);

        // insert one element to state
        state
            .apply_batch(
                &[Op::Insert],
                None,
                &[&ArrayImpl::from(array_nonnull! {Utf8Array, ["abc"]})],
            )
            .unwrap();

        assert_eq!(
            state.get_output().unwrap(),
            Some(ScalarImpl::Utf8("abc".to_string()))
        );

        // delete one element from state
        state
            .apply_batch(
                &[Op::UpdateDelete],
                None,
                &[&ArrayImpl::from(array_nonnull! {Utf8Array, ["abc"]})],
            )
            .unwrap();

        assert_eq!(state.get_output().unwrap(), None);

        // insert one element from state
        state
            .apply_batch(
                &[Op::UpdateInsert],
                None,
                &[&ArrayImpl::from(array_nonnull! {Utf8Array, ["xyz"]})],
            )
            .unwrap();

        assert_eq!(
            state.get_output().unwrap(),
            Some(ScalarImpl::Utf8("xyz".to_string()))
        );

        // insert another one, leading to error
        assert!(state
            .apply_batch(
                &[Op::UpdateInsert],
                None,
                &[&ArrayImpl::from(array_nonnull! {Utf8Array, ["opq"]})],
            )
            .is_err());
    }
}
