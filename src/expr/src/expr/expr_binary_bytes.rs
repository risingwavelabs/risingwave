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

//! For expression that only accept two arguments + 1 bytes writer as input.

use risingwave_common::array::{I32Array, NaiveDateTimeArray, Utf8Array};
use risingwave_common::types::DataType;

use super::Expression;
use crate::expr::template::BinaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::vector_op::concat_op::concat_op;
use crate::vector_op::repeat::repeat;
use crate::vector_op::substr::*;
use crate::vector_op::to_char::to_char_timestamp;
use crate::vector_op::trim_characters::{ltrim_characters, rtrim_characters, trim_characters};

pub fn new_substr_start(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    BinaryBytesExpression::<Utf8Array, I32Array, _>::new(
        expr_ia1,
        expr_ia2,
        return_type,
        substr_start,
    )
    .boxed()
}

#[cfg_attr(not(test), expect(dead_code))]
pub fn new_substr_for(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    BinaryBytesExpression::<Utf8Array, I32Array, _>::new(
        expr_ia1,
        expr_ia2,
        return_type,
        substr_for,
    )
    .boxed()
}

// TODO: Support more `to_char` types.
pub fn new_to_char(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    BinaryBytesExpression::<NaiveDateTimeArray, Utf8Array, _>::new(
        expr_ia1,
        expr_ia2,
        return_type,
        to_char_timestamp,
    )
    .boxed()
}

pub fn new_repeat(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataType,
) -> BoxedExpression {
    BinaryBytesExpression::<Utf8Array, I32Array, _>::new(expr_ia1, expr_ia2, return_type, repeat)
        .boxed()
}

macro_rules! impl_utf8_utf8 {
    ($({ $func_name:ident, $method:ident }),*) => {
        $(pub fn $func_name(
            expr_ia1: BoxedExpression,
            expr_ia2: BoxedExpression,
            return_type: DataType,
        ) -> BoxedExpression {
            BinaryBytesExpression::<Utf8Array, Utf8Array, _>::new(
                expr_ia1,
                expr_ia2,
                return_type,
                $method,
            )
            .boxed()
        })*
    };
}

macro_rules! for_all_utf8_utf8_op {
    ($macro:ident) => {
        $macro! {
            { new_trim_characters, trim_characters },
            { new_ltrim_characters, ltrim_characters },
            { new_rtrim_characters, rtrim_characters },
            { new_concat_op, concat_op }
        }
    };
}

for_all_utf8_utf8_op! { impl_utf8_utf8 }

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{Datum, ScalarImpl};

    use super::*;
    use crate::expr::LiteralExpression;

    fn create_str_i32_binary_expr(
        f: fn(BoxedExpression, BoxedExpression, DataType) -> BoxedExpression,
        str_arg: Datum,
        i32_arg: Datum,
    ) -> BoxedExpression {
        f(
            Box::new(LiteralExpression::new(DataType::Varchar, str_arg)),
            Box::new(LiteralExpression::new(DataType::Int32, i32_arg)),
            DataType::Varchar,
        )
    }

    async fn test_evals_dummy(expr: &BoxedExpression, expected: Datum) {
        let res = expr.eval(&DataChunk::new_dummy(1)).await.unwrap();
        assert_eq!(res.to_datum(), expected);

        let res = expr.eval_row(&OwnedRow::new(vec![])).await.unwrap();
        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_substr() {
        let text = "quick brown";
        let start_pos = 3;
        let for_pos = 4;

        let substr_start_normal = create_str_i32_binary_expr(
            new_substr_start,
            Some(ScalarImpl::from(String::from(text))),
            Some(ScalarImpl::Int32(start_pos)),
        );
        test_evals_dummy(
            &substr_start_normal,
            Some(ScalarImpl::from(String::from(
                &text[start_pos as usize - 1..],
            ))),
        )
        .await;

        let substr_start_i32_none = create_str_i32_binary_expr(
            new_substr_start,
            Some(ScalarImpl::from(String::from(text))),
            None,
        );
        test_evals_dummy(&substr_start_i32_none, None).await;

        let substr_for_normal = create_str_i32_binary_expr(
            new_substr_for,
            Some(ScalarImpl::from(String::from(text))),
            Some(ScalarImpl::Int32(for_pos)),
        );
        test_evals_dummy(
            &substr_for_normal,
            Some(ScalarImpl::from(String::from(&text[..for_pos as usize]))),
        )
        .await;

        let substr_for_str_none =
            create_str_i32_binary_expr(new_substr_for, None, Some(ScalarImpl::Int32(for_pos)));
        test_evals_dummy(&substr_for_str_none, None).await;
    }
}
