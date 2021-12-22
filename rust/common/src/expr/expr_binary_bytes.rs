use std::marker::PhantomData;

/// For expression that only accept two arguments + 1 bytes writer as input.
use crate::array::{I32Array, Utf8Array};
use crate::expr::template::BinaryBytesExpression;
use crate::expr::BoxedExpression;
use crate::types::DataTypeRef;
use crate::vector_op::substr::*;

pub fn new_substr_start(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<Utf8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: substr_start,
        _phantom: PhantomData,
    })
}

pub fn new_substr_for(
    expr_ia1: BoxedExpression,
    expr_ia2: BoxedExpression,
    return_type: DataTypeRef,
) -> BoxedExpression {
    Box::new(BinaryBytesExpression::<Utf8Array, I32Array, _> {
        expr_ia1,
        expr_ia2,
        return_type,
        func: substr_for,
        _phantom: PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::DataChunk;
    use crate::expr::LiteralExpression;
    use crate::types::{Datum, Int32Type, ScalarImpl, StringType};

    fn create_str_i32_binary_expr(
        f: fn(BoxedExpression, BoxedExpression, DataTypeRef) -> BoxedExpression,
        str_arg: Datum,
        i32_arg: Datum,
    ) -> BoxedExpression {
        f(
            Box::new(LiteralExpression::new(
                StringType::create(false, 100, crate::types::DataTypeKind::Char),
                str_arg,
            )),
            Box::new(LiteralExpression::new(Int32Type::create(false), i32_arg)),
            StringType::create(false, 100, crate::types::DataTypeKind::Char),
        )
    }

    #[test]
    fn test_substr() {
        let text = "quick brown";
        let start_pos = 3;
        let for_pos = 4;

        let mut substr_start_normal = create_str_i32_binary_expr(
            new_substr_start,
            Some(ScalarImpl::from(String::from(text))),
            Some(ScalarImpl::Int32(start_pos)),
        );
        let res = substr_start_normal.eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(
            res.to_datum(),
            Some(ScalarImpl::from(String::from(
                &text[start_pos as usize - 1..]
            )))
        );

        let mut substr_start_i32_none = create_str_i32_binary_expr(
            new_substr_start,
            Some(ScalarImpl::from(String::from(text))),
            None,
        );
        let res = substr_start_i32_none
            .eval(&DataChunk::new_dummy(1))
            .unwrap();
        assert_eq!(res.to_datum(), None);

        let mut substr_for_normal = create_str_i32_binary_expr(
            new_substr_for,
            Some(ScalarImpl::from(String::from(text))),
            Some(ScalarImpl::Int32(for_pos)),
        );
        let res = substr_for_normal.eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(
            res.to_datum(),
            Some(ScalarImpl::from(String::from(&text[..for_pos as usize])))
        );

        let mut substr_for_str_none =
            create_str_i32_binary_expr(new_substr_for, None, Some(ScalarImpl::Int32(for_pos)));
        let res = substr_for_str_none.eval(&DataChunk::new_dummy(1)).unwrap();
        assert_eq!(res.to_datum(), None);
    }
}
