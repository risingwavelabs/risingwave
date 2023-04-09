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

use risingwave_common::types::DataType;
pub use risingwave_pb::expr::expr_node::Type as ExprType;

pub use crate::expr::expr_rewriter::ExprRewriter;
pub use crate::expr::function_call::FunctionCall;
use crate::expr::{Expr, ExprImpl, Literal};

/// `SessionTimezone` will be used to resolve session
/// timezone-dependent casts, comparisons or arithmetic.
pub struct SessionTimezone {
    timezone: String,
    /// Whether or not the session timezone was used
    used: bool,
}

impl ExprRewriter for SessionTimezone {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if let Some(expr) = self.with_timezone(func_type, &inputs, ret.clone()) {
            self.used = true;
            expr
        } else {
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }
}

impl SessionTimezone {
    pub fn new(timezone: String) -> Self {
        Self {
            timezone,
            used: false,
        }
    }

    pub fn timezone(&self) -> String {
        self.timezone.clone()
    }

    pub fn used(&self) -> bool {
        self.used
    }

    pub fn warning(&self) -> Option<String> {
        if self.used {
            Some(format!(
                "Your session timezone is {}. It was used in the interpretation of timestamps and dates in your query. If this is unintended, \
                change your timezone to match that of your data's with `set timezone = [timezone]` or \
                rewrite your query with an explicit timezone conversion, e.g. with `AT TIME ZONE`.\n",
                self.timezone
            ))
        } else {
            None
        }
    }

    // Inlines conversions based on session timezone if required by the function
    fn with_timezone(
        &self,
        func_type: ExprType,
        inputs: &Vec<ExprImpl>,
        return_type: DataType,
    ) -> Option<ExprImpl> {
        match func_type {
            ExprType::Cast => {
                assert_eq!(inputs.len(), 1);
                let mut input = inputs[0].clone();
                let input_type = input.return_type();
                match (input_type, return_type.clone()) {
                    (DataType::Timestamptz, DataType::Varchar)
                    | (DataType::Varchar, DataType::Timestamptz) => {
                        Some(self.cast_with_timezone(input, return_type))
                    }
                    (DataType::Date, DataType::Timestamptz)
                    | (DataType::Timestamp, DataType::Timestamptz) => {
                        input = input.cast_explicit(DataType::Timestamp).unwrap();
                        Some(self.at_timezone(input))
                    }
                    (DataType::Timestamptz, DataType::Date)
                    | (DataType::Timestamptz, DataType::Time)
                    | (DataType::Timestamptz, DataType::Timestamp) => {
                        input = self.at_timezone(input);
                        input = input.cast_explicit(return_type).unwrap();
                        Some(input)
                    }
                    _ => None,
                }
            }
            // is cmp
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom => {
                assert_eq!(inputs.len(), 2);
                let mut inputs = inputs.clone();
                for idx in 0..2 {
                    if matches!(inputs[(idx + 1) % 2].return_type(), DataType::Timestamptz)
                        && matches!(
                            inputs[idx % 2].return_type(),
                            DataType::Date | DataType::Timestamp
                        )
                    {
                        let mut to_cast = inputs[idx % 2].clone();
                        // Cast to `Timestamp` first, then use `AT TIME ZONE` to convert to
                        // `Timestamptz`
                        to_cast = to_cast.cast_explicit(DataType::Timestamp).unwrap();
                        inputs[idx % 2] = self.at_timezone(to_cast);
                        return Some(
                            FunctionCall::new_unchecked(func_type, inputs, return_type).into(),
                        );
                    }
                }
                None
            }
            // TODO: handle tstz-related arithmetic with timezone
            // We first translate to timestamp to handle years, months and days,
            // then we translate back to timestamptz handle hours and milliseconds
            //
            // For performance concern, we assume that most the intervals are const-evaled.
            //
            // We impl the following expression tree:
            //
            //                    [+/-]
            //                   /      \
            //             timestamptz   [-]
            //              /            /   \
            //          [+/-]       interval  date_trunc
            //          /    \                   /     \
            //    timestamp  date_trunc       'day' interval
            //        /        /     \
            //  timestamptz  'day'  interval
            //
            //
            // Const-evaled expr tree:
            //
            //                    [+/-]
            //                   /      \
            //             timestamptz   interval_non_date_part
            //              /
            //          [+/-]
            //          /    \
            //    timestamp  interval_date_part
            //        /
            //  timestamptz
            ExprType::Subtract | ExprType::Add => {
                assert_eq!(inputs.len(), 2);
                let canonical_match = matches!(inputs[0].return_type(), DataType::Timestamptz)
                    && matches!(inputs[1].return_type(), DataType::Interval);
                let inverse_match = matches!(inputs[1].return_type(), DataType::Timestamptz)
                    && matches!(inputs[0].return_type(), DataType::Interval);
                assert!(!(inverse_match && func_type == ExprType::Subtract)); // This should never have been parsed.
                if canonical_match || inverse_match {
                    let (orig_timestamptz, interval) =
                        if func_type == ExprType::Add && inverse_match {
                            (inputs[1].clone(), inputs[0].clone())
                        } else {
                            (inputs[0].clone(), inputs[1].clone())
                        };
                    let interval_date_part: ExprImpl = FunctionCall::new_unchecked(
                        ExprType::DateTrunc,
                        vec![
                            Literal::new(Some("day".into()), DataType::Varchar).into(),
                            interval.clone(),
                        ],
                        DataType::Interval,
                    )
                    .into();
                    let interval_non_date_part = FunctionCall::new_unchecked(
                        ExprType::Subtract,
                        vec![interval, interval_date_part.clone()],
                        DataType::Interval,
                    )
                    .into();
                    let timestamp = self
                        .with_timezone(ExprType::Cast, &vec![orig_timestamptz], DataType::Timestamp)
                        .unwrap();
                    let timestamp_op_date_part = FunctionCall::new_unchecked(
                        func_type,
                        vec![timestamp, interval_date_part],
                        DataType::Timestamp,
                    )
                    .into();
                    let timestamptz = self
                        .with_timezone(
                            ExprType::Cast,
                            &vec![timestamp_op_date_part],
                            DataType::Timestamptz,
                        )
                        .unwrap();
                    let timestamptz_op_non_date_part = FunctionCall::new_unchecked(
                        func_type,
                        vec![timestamptz, interval_non_date_part],
                        DataType::Timestamptz,
                    )
                    .into();
                    return Some(timestamptz_op_non_date_part);
                }
                None
            }
            _ => None,
        }
    }

    fn at_timezone(&self, input: ExprImpl) -> ExprImpl {
        FunctionCall::new(
            ExprType::AtTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
        )
        .unwrap()
        .into()
    }

    fn cast_with_timezone(&self, input: ExprImpl, return_type: DataType) -> ExprImpl {
        FunctionCall::new_unchecked(
            ExprType::CastWithTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
            return_type,
        )
        .into()
    }
}
