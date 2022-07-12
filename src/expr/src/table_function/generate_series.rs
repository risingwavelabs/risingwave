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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::multizip;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayRef, DataChunk, I32Array, IntervalArray, NaiveDateTimeArray,
};
use risingwave_common::ensure;
use risingwave_common::types::{CheckedAdd, Scalar};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

use super::*;
use crate::ExprError;

#[derive(Debug)]
pub struct GenerateSeries<T: Array, S: Array> {
    start: BoxedExpression,
    stop: BoxedExpression,
    step: BoxedExpression,
    _phantom: std::marker::PhantomData<(T, S)>,
}

// TODO: use type exercise to do a generic impl..
impl GenerateSeries<NaiveDateTimeArray, IntervalArray> {
    fn eval_row(
        &self,
        start: <NaiveDateTimeArray as Array>::RefItem<'_>,
        stop: <NaiveDateTimeArray as Array>::RefItem<'_>,
        step: <IntervalArray as Array>::RefItem<'_>,
    ) -> Result<ArrayRef> {
        let mut builder = <NaiveDateTimeArray as Array>::Builder::new(DEFAULT_CHUNK_BUFFER_SIZE);

        let mut cur = start;

        // Simulate a do-while loop.
        while cur <= stop {
            if cur > stop {
                break;
            }
            builder.append(Some(cur.as_scalar_ref())).unwrap();
            cur = cur
                .checked_add(step.as_scalar_ref())
                .ok_or(ExprError::NumericOutOfRange)?;
        }

        Ok(Arc::new(builder.finish()?.into()))
    }
}

impl TableFunction for GenerateSeries<NaiveDateTimeArray, IntervalArray> {
    fn return_type(&self) -> DataType {
        self.start.return_type()
    }

    fn eval(&self, input: &DataChunk) -> Result<Vec<ArrayRef>> {
        let ret_start = self.start.eval_checked(input)?;
        let arr_start: &NaiveDateTimeArray = ret_start.as_ref().into();
        let ret_stop = self.stop.eval_checked(input)?;
        let arr_stop: &NaiveDateTimeArray = ret_stop.as_ref().into();

        let ret_step = self.step.eval_checked(input)?;
        let arr_step: &IntervalArray = ret_step.as_ref().into();

        let bitmap = input.get_visibility_ref();
        let mut output_arrays: Vec<ArrayRef> = vec![];

        match bitmap {
            Some(bitmap) => {
                for ((start, stop, step), visible) in
                    multizip((arr_start.iter(), arr_stop.iter(), arr_step.iter()))
                        .zip_eq(bitmap.iter())
                {
                    let array = if !visible {
                        empty_array(self.return_type())
                    } else if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                        self.eval_row(start, stop, step)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
            None => {
                for (start, stop, step) in
                    multizip((arr_start.iter(), arr_stop.iter(), arr_step.iter()))
                {
                    let array = if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                        self.eval_row(start, stop, step)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
        }

        Ok(output_arrays)
    }
}

// TODO: use type exercise to do a generic impl..
impl GenerateSeries<I32Array, I32Array> {
    fn eval_row(
        &self,
        start: <I32Array as Array>::RefItem<'_>,
        stop: <I32Array as Array>::RefItem<'_>,
        step: <I32Array as Array>::RefItem<'_>,
    ) -> Result<ArrayRef> {
        let mut builder = <I32Array as Array>::Builder::new(DEFAULT_CHUNK_BUFFER_SIZE);

        let mut cur = start;

        // Simulate a do-while loop.
        while cur <= stop {
            if cur > stop {
                break;
            }
            builder.append(Some(cur.as_scalar_ref())).unwrap();
            cur = cur
                .checked_add(step.as_scalar_ref())
                .ok_or(ExprError::NumericOutOfRange)?;
        }

        Ok(Arc::new(builder.finish()?.into()))
    }
}

impl TableFunction for GenerateSeries<I32Array, I32Array> {
    fn return_type(&self) -> DataType {
        self.start.return_type()
    }

    fn eval(&self, input: &DataChunk) -> Result<Vec<ArrayRef>> {
        let ret_start = self.start.eval_checked(input)?;
        let arr_start: &I32Array = ret_start.as_ref().into();
        let ret_stop = self.stop.eval_checked(input)?;
        let arr_stop: &I32Array = ret_stop.as_ref().into();

        let ret_step = self.step.eval_checked(input)?;
        let arr_step: &I32Array = ret_step.as_ref().into();

        let bitmap = input.get_visibility_ref();
        let mut output_arrays: Vec<ArrayRef> = vec![];

        match bitmap {
            Some(bitmap) => {
                for ((start, stop, step), visible) in
                    multizip((arr_start.iter(), arr_stop.iter(), arr_step.iter()))
                        .zip_eq(bitmap.iter())
                {
                    let array = if !visible {
                        empty_array(self.return_type())
                    } else if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                        self.eval_row(start, stop, step)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
            None => {
                for (start, stop, step) in
                    multizip((arr_start.iter(), arr_stop.iter(), arr_step.iter()))
                {
                    let array = if let (Some(start), Some(stop), Some(step)) = (start, stop, step) {
                        self.eval_row(start, stop, step)?
                    } else {
                        empty_array(self.return_type())
                    };
                    output_arrays.push(array);
                }
            }
        }

        Ok(output_arrays)
    }
}

pub fn new_generate_series(
    args: Vec<BoxedExpression>,
    return_type: DataType,
) -> Result<BoxedTableFunction> {
    ensure!(args.len() == 3);
    let (start, stop, step) = args.into_iter().collect_tuple().unwrap();

    match return_type {
        DataType::Timestamp => Ok(GenerateSeries::<NaiveDateTimeArray, IntervalArray> {
            start,
            stop,
            step,
            _phantom: Default::default(),
        }
        .boxed()),
        DataType::Int32 => Ok(GenerateSeries::<I32Array, I32Array> {
            start,
            stop,
            step,
            _phantom: Default::default(),
        }
        .boxed()),
        _ => Err(ExprError::Internal(anyhow!(
            "the return type of Generate Series Function is incorrect".to_string(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, IntervalUnit, NaiveDateTimeWrapper, ScalarImpl};

    use super::*;
    use crate::expr::{Expression, LiteralExpression};
    use crate::vector_op::cast::str_to_timestamp;

    #[test]
    fn test_generate_i32_series() {
        generate_series_test_case(2, 4, 1);
        generate_series_test_case(0, 9, 2);
        generate_series_test_case(0, (DEFAULT_CHUNK_BUFFER_SIZE * 2 + 3) as i32, 1);
    }

    fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        fn to_lit_expr(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }

        let function = GenerateSeries::<I32Array, I32Array> {
            start: to_lit_expr(start),
            stop: to_lit_expr(stop),
            step: to_lit_expr(step),
            _phantom: Default::default(),
        }
        .boxed();
        let expect_cnt = ((stop - start) / step + 1) as usize;

        let dummy_chunk = DataChunk::new_dummy(1);
        let arrays = function.eval(&dummy_chunk).unwrap();

        let cnt: usize = arrays.iter().map(|a| a.len()).sum();
        assert_eq!(cnt, expect_cnt);
    }

    #[test]
    fn test_generate_time_series() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = IntervalUnit::from_minutes(1);
        let one_hour_step = IntervalUnit::from_minutes(60);
        let one_day_step = IntervalUnit::from_days(1);
        generate_time_series_test_case(start_time, stop_time, one_minute_step, 60 * 24 * 8 + 1);
        generate_time_series_test_case(start_time, stop_time, one_hour_step, 24 * 8 + 1);
        generate_time_series_test_case(start_time, stop_time, one_day_step, 8 + 1);
    }

    fn generate_time_series_test_case(
        start: NaiveDateTimeWrapper,
        stop: NaiveDateTimeWrapper,
        step: IntervalUnit,
        expect_cnt: usize,
    ) {
        fn to_lit_expr(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }

        let function = GenerateSeries::<NaiveDateTimeArray, IntervalArray> {
            start: to_lit_expr(DataType::Timestamp, start.into()),
            stop: to_lit_expr(DataType::Timestamp, stop.into()),
            step: to_lit_expr(DataType::Interval, step.into()),
            _phantom: Default::default(),
        };

        let dummy_chunk = DataChunk::new_dummy(1);
        let arrays = function.eval(&dummy_chunk).unwrap();

        let cnt: usize = arrays.iter().map(|a| a.len()).sum();
        assert_eq!(cnt, expect_cnt);
    }
}
