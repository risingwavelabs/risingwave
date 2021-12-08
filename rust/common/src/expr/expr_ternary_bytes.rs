/// For expression that accept 3 arguments + 1 bytes writer as input.
use crate::array::{I32Array, Utf8Array};
use crate::expr::template::TernaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::vector_op::replace::replace;
use crate::vector_op::substr::substr_start_for;
use std::marker::PhantomData;

pub fn new_substr_start_end(
    items: BoxedExpression,
    off: BoxedExpression,
    len: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(TernaryBytesExpression::<Utf8Array, I32Array, I32Array, _> {
        expr_ia1: items,
        expr_ia2: off,
        expr_ia3: len,
        return_type,
        func: substr_start_for,
        _phantom: PhantomData,
    })
}

pub fn new_replace_expr(
    s: BoxedExpression,
    from_str: BoxedExpression,
    to_str: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(
        TernaryBytesExpression::<Utf8Array, Utf8Array, Utf8Array, _> {
            expr_ia1: s,
            expr_ia2: from_str,
            expr_ia3: to_str,
            return_type,
            func: replace,
            _phantom: PhantomData,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::DataChunk;
    use crate::expr::LiteralExpression;
    use crate::types::{Int32Type, ScalarImpl, StringType};

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
            let mut expr = new_substr_start_end(
                Box::new(LiteralExpression::new(
                    StringType::create(false, 100, crate::types::DataTypeKind::Char),
                    Some(ScalarImpl::from(String::from(text))),
                )),
                Box::new(LiteralExpression::new(Int32Type::create(false), start)),
                Box::new(LiteralExpression::new(Int32Type::create(false), len)),
                StringType::create(false, 100, crate::types::DataTypeKind::Char),
            );
            let res = expr.eval(&DataChunk::new_dummy(1));
            if is_negative_len {
                assert!(res.is_err());
            } else {
                assert_eq!(res.unwrap().to_datum(), expected);
            }
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
            let mut expr = new_replace_expr(
                Box::new(LiteralExpression::new(
                    StringType::create(false, 100, crate::types::DataTypeKind::Char),
                    Some(ScalarImpl::from(String::from(text))),
                )),
                Box::new(LiteralExpression::new(
                    StringType::create(false, 100, crate::types::DataTypeKind::Char),
                    Some(ScalarImpl::from(String::from(pattern))),
                )),
                Box::new(LiteralExpression::new(
                    StringType::create(false, 100, crate::types::DataTypeKind::Char),
                    Some(ScalarImpl::from(String::from(replacement))),
                )),
                StringType::create(false, 100, crate::types::DataTypeKind::Char),
            );
            let res = expr.eval(&DataChunk::new_dummy(1)).unwrap();
            assert_eq!(
                res.to_datum(),
                Some(ScalarImpl::from(String::from(expected)))
            );
        }
    }
}
