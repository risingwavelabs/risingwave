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

use anyhow::anyhow;
use risingwave_common::array::{I32Array, IntervalArray, NaiveDateTimeArray};

type Range<T, S> = GenerateSeries<T, S>;

use super::*;
use crate::ExprError;

pub fn new_range(prost: &TableFunctionProst, chunk_size: usize) -> Result<BoxedTableFunction> {
    let return_type = DataType::from(prost.get_return_type().unwrap());
    let args: Vec<_> = prost.args.iter().map(expr_build_from_prost).try_collect()?;
    let [start, stop, step]: [_; 3] = args.try_into().unwrap();

    match return_type {
        DataType::Timestamp => Ok(Range::<NaiveDateTimeArray, IntervalArray>::new(
            start, stop, step, chunk_size, false,
        )
        .boxed()),
        DataType::Int32 => {
            Ok(Range::<I32Array, I32Array>::new(start, stop, step, chunk_size, false).boxed())
        }
        _ => Err(ExprError::Internal(anyhow!(
            "the return type of Range Function is incorrect".to_string(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{DataType, IntervalUnit, NaiveDateTimeWrapper, ScalarImpl};

    use super::*;
    use crate::expr::{Expression, LiteralExpression};
    use crate::vector_op::cast::str_to_timestamp;

    const CHUNK_SIZE: usize = 1024;

    #[test]
    fn test_i32_range() {
        range_test_case(2, 4, 1);
        range_test_case(4, 2, -1);
        range_test_case(0, 9, 2);
        range_test_case(0, (CHUNK_SIZE * 2 + 3) as i32, 1);
    }

    fn range_test_case(start: i32, stop: i32, step: i32) {
        fn to_lit_expr(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }

        let function = Range::<I32Array, I32Array>::new(
            to_lit_expr(start),
            to_lit_expr(stop),
            to_lit_expr(step),
            CHUNK_SIZE,
            false,
        )
        .boxed();
        let expect_cnt = ((stop - start - step.signum()) / step + 1) as usize;

        let dummy_chunk = DataChunk::new_dummy(1);
        let arrays = function.eval(&dummy_chunk).unwrap();

        let cnt: usize = arrays.iter().map(|a| a.len()).sum();
        assert_eq!(cnt, expect_cnt);
    }

    #[test]
    fn test_time_range() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = IntervalUnit::from_minutes(1);
        let one_hour_step = IntervalUnit::from_minutes(60);
        let one_day_step = IntervalUnit::from_days(1);
        time_range_test_case(start_time, stop_time, one_minute_step, 60 * 24 * 8);
        time_range_test_case(start_time, stop_time, one_hour_step, 24 * 8);
        time_range_test_case(start_time, stop_time, one_day_step, 8);
        time_range_test_case(stop_time, start_time, -one_day_step, 8);
    }

    fn time_range_test_case(
        start: NaiveDateTimeWrapper,
        stop: NaiveDateTimeWrapper,
        step: IntervalUnit,
        expect_cnt: usize,
    ) {
        fn to_lit_expr(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }

        let function = Range::<NaiveDateTimeArray, IntervalArray>::new(
            to_lit_expr(DataType::Timestamp, start.into()),
            to_lit_expr(DataType::Timestamp, stop.into()),
            to_lit_expr(DataType::Interval, step.into()),
            CHUNK_SIZE,
            false,
        );

        let dummy_chunk = DataChunk::new_dummy(1);
        let arrays = function.eval(&dummy_chunk).unwrap();

        let cnt: usize = arrays.iter().map(|a| a.len()).sum();
        assert_eq!(cnt, expect_cnt);
    }
}
