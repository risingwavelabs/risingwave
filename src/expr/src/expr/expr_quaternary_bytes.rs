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

//! For expression that accept 4 arguments + 1 bytes writer as input.

use risingwave_common::array::{I32Array, Utf8Array};
use risingwave_common::types::DataType;

use crate::expr::template::QuaternaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::vector_op::overlay::overlay_for;

pub fn new_overlay_for_exp(
    s: BoxedExpression,
    new_sub_str: BoxedExpression,
    start: BoxedExpression,
    count: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    Box::new(QuaternaryBytesExpression::<
        Utf8Array,
        Utf8Array,
        I32Array,
        I32Array,
        _,
    >::new(
        s, new_sub_str, start, count, return_type, overlay_for
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{Datum, ScalarImpl};

    use super::*;
    use crate::expr::LiteralExpression;

    async fn test_evals_dummy(expr: BoxedExpression, expected: Datum, is_negative_len: bool) {
        let res = expr.eval(&DataChunk::new_dummy(1)).await;
        if is_negative_len {
            assert!(res.is_err());
        } else {
            assert_eq!(res.unwrap().to_datum(), expected);
        }

        let res = expr.eval_row(&OwnedRow::new(vec![])).await;
        if is_negative_len {
            assert!(res.is_err());
        } else {
            assert_eq!(res.unwrap(), expected);
        }
    }

    #[tokio::test]
    async fn test_overlay() {
        let cases = vec![
            ("aaa", "XY", 1, 0, "XYaaa"),
            ("aaa_aaa", "XYZ", 4, 1, "aaaXYZaaa"),
            ("aaaaaa", "XYZ", 4, 0, "aaaXYZaaa"),
            ("aaa___aaa", "X", 4, 3, "aaaXaaa"),
            ("aaa", "X", 4, -123, "aaaX"),
            ("aaa_", "X", 4, 123, "aaaX"),
        ];

        for (s, new_sub_str, start, count, expected) in cases {
            let expr = new_overlay_for_exp(
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(s))),
                )),
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(new_sub_str))),
                )),
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::from(start)),
                )),
                Box::new(LiteralExpression::new(
                    DataType::Int32,
                    Some(ScalarImpl::from(count)),
                )),
                DataType::Varchar,
            );

            test_evals_dummy(expr, Some(ScalarImpl::from(String::from(expected))), false).await;
        }
    }
}
