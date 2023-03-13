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

//! For expression that accept 3 arguments + 1 bytes writer as input.

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{Datum, ScalarImpl};

    use super::*;
    use crate::expr::LiteralExpression;

    fn test_evals_dummy(expr: BoxedExpression, expected: Datum, is_negative_len: bool) {
        let res = expr.eval(&DataChunk::new_dummy(1));
        if is_negative_len {
            assert!(res.is_err());
        } else {
            assert_eq!(res.unwrap().to_datum(), expected);
        }

        let res = expr.eval_row(&OwnedRow::new(vec![]));
        if is_negative_len {
            assert!(res.is_err());
        } else {
            assert_eq!(res.unwrap(), expected);
        }
    }

    #[test]
    fn test_substr_start_end() {
        let text = "quick brown";
        let cases = [
            (
                Some(ScalarImpl::Int32(4)),
                Some(ScalarImpl::Int32(2)),
                Some(ScalarImpl::from(String::from("ck"))),
            ),
            (
                Some(ScalarImpl::Int32(-1)),
                Some(ScalarImpl::Int32(5)),
                Some(ScalarImpl::from(String::from("qui"))),
            ),
            (
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Int32(20)),
                Some(ScalarImpl::from(String::from("quick brown"))),
            ),
            (
                Some(ScalarImpl::Int32(12)),
                Some(ScalarImpl::Int32(20)),
                Some(ScalarImpl::from(String::from(""))),
            ),
            (
                Some(ScalarImpl::Int32(5)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::from(String::from(""))),
            ),
            (
                Some(ScalarImpl::Int32(5)),
                Some(ScalarImpl::Int32(-1)),
                Some(ScalarImpl::from(String::from(""))),
            ),
            (Some(ScalarImpl::Int32(12)), None, None),
            (None, Some(ScalarImpl::Int32(20)), None),
            (None, None, None),
        ];

        for (start, len, expected) in cases {
            let is_negative_len = matches!(len, Some(ScalarImpl::Int32(len_i32)) if len_i32 < 0);
            let expr = new_substr_start_end(
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(text))),
                )),
                Box::new(LiteralExpression::new(DataType::Int32, start)),
                Box::new(LiteralExpression::new(DataType::Int32, len)),
                DataType::Varchar,
            );

            test_evals_dummy(expr, expected, is_negative_len);
        }
    }

    #[test]
    fn test_replace() {
        let cases = [
            ("hello, word", "我的", "world", "hello, word"),
            ("hello, word", "", "world", "hello, word"),
            ("hello, word", "word", "world", "hello, world"),
            ("hello, world", "world", "", "hello, "),
            ("你是❤️，是暖，是希望", "是", "非", "你非❤️，非暖，非希望"),
            ("👴笑了", "👴", "爷爷", "爷爷笑了"),
            (
                "НОЧЬ НА ОЧКРАИНЕ МОСКВЫ",
                "ОЧ",
                "Ы",
                "НЫЬ НА ЫКРАИНЕ МОСКВЫ",
            ),
        ];

        for (text, pattern, replacement, expected) in cases {
            let expr = new_replace_expr(
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(text))),
                )),
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(pattern))),
                )),
                Box::new(LiteralExpression::new(
                    DataType::Varchar,
                    Some(ScalarImpl::from(String::from(replacement))),
                )),
                DataType::Varchar,
            );

            test_evals_dummy(expr, Some(ScalarImpl::from(String::from(expected))), false);
        }
    }

    #[test]
    fn test_overlay() {
        let cases = vec![
            ("aaa__aaa", "XY", 4, "aaaXYaaa"),
            ("aaa", "XY", 3, "aaXY"),
            ("aaa", "XY", 4, "aaaXY"),
            ("aaa", "XY", -123, "XYa"),
            ("aaa", "XY", 123, "aaaXY"),
        ];

        for (s, new_sub_str, start, expected) in cases {
            let expr = new_overlay_exp(
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
                DataType::Varchar,
            );

            test_evals_dummy(expr, Some(ScalarImpl::from(String::from(expected))), false);
        }
    }
}
