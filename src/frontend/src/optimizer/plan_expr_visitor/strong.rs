// Copyright 2024 RisingWave Labs
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

use fixedbitset::FixedBitSet;

use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef};

/// This utilities are with the same definition in calcite.
/// Utilities for strong predicates.
/// A predicate is strong (or null-rejecting) with regards to selected subset of inputs
/// if it is UNKNOWN if all inputs in selected subset are UNKNOWN.
/// By the way, UNKNOWN is just the boolean form of NULL.
///
/// Examples:
///
/// UNKNOWN is strong in [] (definitely null)
/// `c = 1` is strong in [c] (definitely null if and only if c is null)
/// `c IS NULL` is not strong (always returns TRUE or FALSE, nevernull)
/// `p1 AND p2` is strong in [p1, p2] (definitely null if either p1 is null or p2 is null)
/// `p1 OR p2` is strong if p1 and p2 are strong
/// `c1 = 1 OR c2 IS NULL` is strong in [c1] (definitely null if c1 is null)

#[derive(Default)]
pub struct Strong {
    null_columns: FixedBitSet,
}

impl Strong {
    fn new(null_columns: FixedBitSet) -> Self {
        Self { null_columns }
    }

    // Returns whether the analyzed expression will definitely return null if
    // all of a given set of input columns are null.
    #[allow(dead_code)]
    pub fn is_null(expr: &ExprImpl, null_columns: FixedBitSet) -> bool {
        let strong = Strong::new(null_columns);
        strong.is_null_visit(expr)
    }

    fn is_input_ref_null(&self, input_ref: &InputRef) -> bool {
        self.null_columns.contains(input_ref.index())
    }

    fn is_null_visit(&self, expr: &ExprImpl) -> bool {
        match expr {
            ExprImpl::InputRef(input_ref) => self.is_input_ref_null(input_ref),
            ExprImpl::Literal(literal) => literal.get_data().is_none(),
            ExprImpl::FunctionCall(func_call) => self.is_null_function_call(func_call),
            ExprImpl::FunctionCallWithLambda(_) => false,
            ExprImpl::AggCall(_) => false,
            ExprImpl::Subquery(_) => false,
            ExprImpl::CorrelatedInputRef(_) => false,
            ExprImpl::TableFunction(_) => false,
            ExprImpl::WindowFunction(_) => false,
            ExprImpl::UserDefinedFunction(_) => false,
            ExprImpl::Parameter(_) => false,
            ExprImpl::Now(_) => false,
        }
    }

    fn is_null_function_call(&self, func_call: &FunctionCall) -> bool {
        match func_call.func_type() {
            // NOT NULL: This kind of expression is never null. No need to look at its arguments, if it has any.
            ExprType::IsNull
            | ExprType::IsNotNull
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom
            | ExprType::IsTrue
            | ExprType::IsNotTrue
            | ExprType::IsFalse
            | ExprType::IsNotFalse => false,
            // ANY: This kind of expression is null if and only if at least one of its arguments is null.
            ExprType::Not
            | ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::Like
            | ExprType::Add
            | ExprType::AddWithTimeZone
            | ExprType::Subtract
            | ExprType::Multiply
            | ExprType::Modulus
            | ExprType::Divide
            | ExprType::Cast
            | ExprType::Trim
            | ExprType::Ltrim
            | ExprType::Rtrim
            | ExprType::Ceil
            | ExprType::Floor
            | ExprType::Extract
            | ExprType::Greatest
            | ExprType::Least => self.any_null(func_call),
            ExprType::And | ExprType::Or | ExprType::Coalesce => self.all_null(func_call),
            // TODO: Function like case when is important but current its structure is complicated, so we need to implement it later if necessary.
            // Assume that any other expressions cannot be simplified.
            _ => false,
        }
    }

    fn any_null(&self, func_call: &FunctionCall) -> bool {
        func_call
            .inputs()
            .iter()
            .any(|expr| self.is_null_visit(expr))
    }

    fn all_null(&self, func_call: &FunctionCall) -> bool {
        func_call
            .inputs()
            .iter()
            .all(|expr| self.is_null_visit(expr))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::ExprImpl::Literal;

    #[test]
    fn test_literal() {
        let null_columns = FixedBitSet::with_capacity(1);
        let expr = Literal(crate::expr::Literal::new(None, DataType::Varchar).into());
        assert!(Strong::is_null(&expr, null_columns.clone()));

        let expr = Literal(
            crate::expr::Literal::new(Some("test".to_string().into()), DataType::Varchar).into(),
        );
        assert!(!Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_input_ref1() {
        let null_columns = FixedBitSet::with_capacity(2);
        let expr = InputRef::new(0, DataType::Varchar).into();
        assert!(!Strong::is_null(&expr, null_columns.clone()));

        let expr = InputRef::new(1, DataType::Varchar).into();
        assert!(!Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_input_ref2() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        null_columns.insert(1);
        let expr = InputRef::new(0, DataType::Varchar).into();
        assert!(Strong::is_null(&expr, null_columns.clone()));

        let expr = InputRef::new(1, DataType::Varchar).into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    #[test]
    fn test_divide() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        null_columns.insert(1);
        let expr = FunctionCall::new_unchecked(
            ExprType::Divide,
            vec![
                InputRef::new(0, DataType::Decimal).into(),
                InputRef::new(1, DataType::Decimal).into(),
            ],
            DataType::Varchar,
        )
        .into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    // generate a test case for (0.8 * sum / count) where sum is null and count is not null
    #[test]
    fn test_multiply_divide() {
        let mut null_columns = FixedBitSet::with_capacity(2);
        null_columns.insert(0);
        let expr = FunctionCall::new_unchecked(
            ExprType::Multiply,
            vec![
                Literal(crate::expr::Literal::new(Some(0.8f64.into()), DataType::Float64).into()),
                FunctionCall::new_unchecked(
                    ExprType::Divide,
                    vec![
                        InputRef::new(0, DataType::Decimal).into(),
                        InputRef::new(1, DataType::Decimal).into(),
                    ],
                    DataType::Decimal,
                )
                .into(),
            ],
            DataType::Decimal,
        )
        .into();
        assert!(Strong::is_null(&expr, null_columns));
    }

    // generate test cases for is not null
    macro_rules! gen_test {
        ($func:ident, $expr:expr, $expected:expr) => {
            #[test]
            fn $func() {
                let null_columns = FixedBitSet::with_capacity(2);
                let expr = $expr;
                assert_eq!(Strong::is_null(&expr, null_columns), $expected);
            }
        };
    }

    gen_test!(
        test_is_not_null,
        FunctionCall::new_unchecked(
            ExprType::IsNotNull,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_null,
        FunctionCall::new_unchecked(
            ExprType::IsNull,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_distinct_from,
        FunctionCall::new_unchecked(
            ExprType::IsDistinctFrom,
            vec![
                InputRef::new(0, DataType::Varchar).into(),
                InputRef::new(1, DataType::Varchar).into()
            ],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_not_distinct_from,
        FunctionCall::new_unchecked(
            ExprType::IsNotDistinctFrom,
            vec![
                InputRef::new(0, DataType::Varchar).into(),
                InputRef::new(1, DataType::Varchar).into()
            ],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_true,
        FunctionCall::new_unchecked(
            ExprType::IsTrue,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_not_true,
        FunctionCall::new_unchecked(
            ExprType::IsNotTrue,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
    gen_test!(
        test_is_false,
        FunctionCall::new_unchecked(
            ExprType::IsFalse,
            vec![InputRef::new(0, DataType::Varchar).into()],
            DataType::Varchar
        )
        .into(),
        false
    );
}
