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

use risingwave_common::array::{
    I32Array, I64Array, IntervalArray, NaiveDateArray, NaiveDateTimeArray, Utf8Array,
};
use risingwave_common::types::DataType;

use super::template::TernaryExpression;
use super::BoxedExpression;
use crate::vector_op::tumble::{
    tumble_start_date, tumble_start_date_time, tumble_start_timestamptz,
};
use crate::ExprError;

pub(crate) fn new_tumble_start(
    time: BoxedExpression,
    window_size: BoxedExpression,
    offset: Option<BoxedExpression>,
    return_type: DataType,
) -> BoxedExpression {
    let expr: BoxedExpression = match time.return_type() {
        DataType::Date => Box::new(TernaryExpression::<
            NaiveDateArray,
            IntervalArray,
            Option<IntervalArray>,
            NaiveDateTimeArray,
            _,
        >::new(
            time, window_size, offset, return_type, tumble_start_date
        )),
        // Box::new(
        //     TernaryBytesExpression::<Utf8Array, I32Array, I32Array, _>::new(
        //         items,
        //         off,
        //         len,
        //         return_type,
        //         substr_start_for,
        //     ),
        // )
        DataType::Timestamp => Box::new(TernaryExpression::<
            NaiveDateTimeArray,
            IntervalArray,
            IntervalArray,
            NaiveDateTimeArray,
            _,
        >::new(
            time,
            window_size,
            offset,
            return_type,
            tumble_start_date_time,
        )),
        DataType::Timestamptz => Box::new(TernaryExpression::<
            I64Array,
            IntervalArray,
            IntervalArray,
            I64Array,
            _,
        >::new(
            time,
            window_size,
            offset,
            return_type,
            tumble_start_timestamptz,
        )),
        _ => {
            return Err(ExprError::UnsupportedFunction(format!(
                "tumble_start is not supported for {:?}",
                time.return_type()
            )))
        }
    }?;

    Ok(expr)
}
