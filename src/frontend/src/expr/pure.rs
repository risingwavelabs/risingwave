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

use risingwave_pb::expr::expr_node;

use super::{ExprImpl, ExprVisitor};
struct ImpureAnalyzer {}

impl ExprVisitor<bool> for ImpureAnalyzer {
    fn merge(a: bool, b: bool) -> bool {
        // the expr will be impure if any of its input is impure
        a || b
    }

    fn visit_user_defined_function(&mut self, _func_call: &super::UserDefinedFunction) -> bool {
        true
    }

    fn visit_function_call(&mut self, func_call: &super::FunctionCall) -> bool {
        match func_call.get_expr_type() {
            expr_node::Type::Unspecified
            | expr_node::Type::InputRef
            | expr_node::Type::ConstantValue
            | expr_node::Type::Add
            | expr_node::Type::Subtract
            | expr_node::Type::Multiply
            | expr_node::Type::Divide
            | expr_node::Type::Modulus
            | expr_node::Type::Equal
            | expr_node::Type::NotEqual
            | expr_node::Type::LessThan
            | expr_node::Type::LessThanOrEqual
            | expr_node::Type::GreaterThan
            | expr_node::Type::GreaterThanOrEqual
            | expr_node::Type::And
            | expr_node::Type::Or
            | expr_node::Type::Not
            | expr_node::Type::In
            | expr_node::Type::Some
            | expr_node::Type::All
            | expr_node::Type::BitwiseAnd
            | expr_node::Type::BitwiseOr
            | expr_node::Type::BitwiseXor
            | expr_node::Type::BitwiseNot
            | expr_node::Type::BitwiseShiftLeft
            | expr_node::Type::BitwiseShiftRight
            | expr_node::Type::Extract
            | expr_node::Type::DatePart
            | expr_node::Type::TumbleStart
            | expr_node::Type::ToTimestamp
            | expr_node::Type::AtTimeZone
            | expr_node::Type::DateTrunc
            | expr_node::Type::ToTimestamp1
            | expr_node::Type::CastWithTimeZone
            | expr_node::Type::Cast
            | expr_node::Type::Substr
            | expr_node::Type::Length
            | expr_node::Type::Like
            | expr_node::Type::Upper
            | expr_node::Type::Lower
            | expr_node::Type::Trim
            | expr_node::Type::Replace
            | expr_node::Type::Position
            | expr_node::Type::Ltrim
            | expr_node::Type::Rtrim
            | expr_node::Type::Case
            | expr_node::Type::RoundDigit
            | expr_node::Type::Round
            | expr_node::Type::Ascii
            | expr_node::Type::Translate
            | expr_node::Type::Coalesce
            | expr_node::Type::ConcatWs
            | expr_node::Type::Abs
            | expr_node::Type::SplitPart
            | expr_node::Type::Ceil
            | expr_node::Type::Floor
            | expr_node::Type::ToChar
            | expr_node::Type::Md5
            | expr_node::Type::CharLength
            | expr_node::Type::Repeat
            | expr_node::Type::ConcatOp
            | expr_node::Type::BoolOut
            | expr_node::Type::OctetLength
            | expr_node::Type::BitLength
            | expr_node::Type::Overlay
            | expr_node::Type::RegexpMatch
            | expr_node::Type::Pow
            | expr_node::Type::Exp
            | expr_node::Type::Chr
            | expr_node::Type::StartsWith
            | expr_node::Type::Initcap
            | expr_node::Type::Lpad
            | expr_node::Type::Rpad
            | expr_node::Type::Reverse
            | expr_node::Type::Strpos
            | expr_node::Type::ToAscii
            | expr_node::Type::ToHex
            | expr_node::Type::QuoteIdent
            | expr_node::Type::Sin
            | expr_node::Type::Cos
            | expr_node::Type::Tan
            | expr_node::Type::Cot
            | expr_node::Type::Asin
            | expr_node::Type::Acos
            | expr_node::Type::Atan
            | expr_node::Type::Atan2
            | expr_node::Type::Sqrt
            | expr_node::Type::Degrees
            | expr_node::Type::Radians
            | expr_node::Type::IsTrue
            | expr_node::Type::IsNotTrue
            | expr_node::Type::IsFalse
            | expr_node::Type::IsNotFalse
            | expr_node::Type::IsNull
            | expr_node::Type::IsNotNull
            | expr_node::Type::IsDistinctFrom
            | expr_node::Type::IsNotDistinctFrom
            | expr_node::Type::Neg
            | expr_node::Type::Field
            | expr_node::Type::Array
            | expr_node::Type::ArrayAccess
            | expr_node::Type::Row
            | expr_node::Type::ArrayToString
            | expr_node::Type::ArrayCat
            | expr_node::Type::ArrayAppend
            | expr_node::Type::ArrayPrepend
            | expr_node::Type::FormatType
            | expr_node::Type::ArrayDistinct
            | expr_node::Type::ArrayLength
            | expr_node::Type::Cardinality
            | expr_node::Type::TrimArray
            | expr_node::Type::ArrayRemove
            | expr_node::Type::HexToInt256
            | expr_node::Type::JsonbAccessInner
            | expr_node::Type::JsonbAccessStr
            | expr_node::Type::JsonbTypeof
            | expr_node::Type::JsonbArrayLength
            | expr_node::Type::Pi
            | expr_node::Type::Sind
            | expr_node::Type::Cosd
            | expr_node::Type::Tand
            | expr_node::Type::ArrayPositions
            | expr_node::Type::StringToArray =>
            // expression output is deterministic(same result for the same input)
            {
                let x = func_call
                    .inputs()
                    .iter()
                    .map(|expr| self.visit_expr(expr))
                    .reduce(Self::merge)
                    .unwrap_or_default();
                x
            }
            // expression output is not deterministic
            expr_node::Type::Vnode
            | expr_node::Type::Now
            | expr_node::Type::Proctime
            | expr_node::Type::Udf => true,
        }
    }
}

pub fn is_pure(expr: &ExprImpl) -> bool {
    !is_impure(expr)
}
pub fn is_impure(expr: &ExprImpl) -> bool {
    let mut a = ImpureAnalyzer {};
    a.visit_expr(expr)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use crate::expr::{is_impure, is_pure, ExprImpl, FunctionCall, InputRef};

    fn expect_pure(expr: &ExprImpl) {
        assert!(is_pure(expr));
        assert!(!is_impure(expr));
    }

    fn expect_impure(expr: &ExprImpl) {
        assert!(!is_pure(expr));
        assert!(is_impure(expr));
    }

    #[test]
    fn test_pure_funcs() {
        let e: ExprImpl = FunctionCall::new(
            Type::Add,
            vec![
                InputRef::new(0, DataType::Int16).into(),
                InputRef::new(0, DataType::Int16).into(),
            ],
        )
        .unwrap()
        .into();
        expect_pure(&e);

        let e: ExprImpl = FunctionCall::new(
            Type::GreaterThan,
            vec![
                InputRef::new(0, DataType::Timestamptz).into(),
                FunctionCall::new(Type::Now, vec![]).unwrap().into(),
            ],
        )
        .unwrap()
        .into();
        expect_impure(&e);
    }
}
